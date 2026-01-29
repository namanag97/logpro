// Package checkpoint provides S3-backed checkpoint persistence for serverless.
package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config configures the S3 checkpoint backend.
type S3Config struct {
	// Bucket is the S3 bucket for storing checkpoints
	Bucket string

	// Prefix is prepended to all checkpoint keys (e.g., "checkpoints/")
	Prefix string

	// Region is the AWS region
	Region string

	// Endpoint overrides the default S3 endpoint (for S3-compatible services)
	Endpoint string

	// Credentials (optional - uses default chain if not provided)
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// UsePathStyle forces path-style addressing (for MinIO, LocalStack)
	UsePathStyle bool

	// Timeout for S3 operations
	Timeout time.Duration

	// StorageClass for checkpoint objects (default: STANDARD)
	StorageClass types.StorageClass

	// ServerSideEncryption enables SSE-S3 encryption
	ServerSideEncryption bool
}

// DefaultS3Config returns sensible defaults.
func DefaultS3Config(bucket string) S3Config {
	return S3Config{
		Bucket:       bucket,
		Prefix:       "checkpoints/",
		Timeout:      30 * time.Second,
		StorageClass: types.StorageClassStandard,
	}
}

// S3Backend stores checkpoints in S3.
type S3Backend struct {
	cfg    S3Config
	client *s3.Client
}

// NewS3Backend creates a new S3 checkpoint backend.
func NewS3Backend(ctx context.Context, cfg S3Config) (*S3Backend, error) {
	// Build AWS config options
	var opts []func(*config.LoadOptions) error

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	// Use explicit credentials if provided
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				cfg.SessionToken,
			),
		))
	}

	// Load AWS config
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client options
	s3Opts := []func(*s3.Options){}

	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	if cfg.UsePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &S3Backend{
		cfg:    cfg,
		client: client,
	}, nil
}

// key returns the S3 key for a checkpoint ID.
func (b *S3Backend) key(id string) string {
	return b.cfg.Prefix + id + ".json"
}

// Save persists a checkpoint to S3.
func (b *S3Backend) Save(ctx context.Context, cp *Checkpoint) error {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	// Serialize checkpoint
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket:       aws.String(b.cfg.Bucket),
		Key:          aws.String(b.key(cp.ID)),
		Body:         bytes.NewReader(data),
		ContentType:  aws.String("application/json"),
		StorageClass: b.cfg.StorageClass,
	}

	if b.cfg.ServerSideEncryption {
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	}

	_, err = b.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to save checkpoint to S3: %w", err)
	}

	return nil
}

// Load retrieves a checkpoint from S3.
func (b *S3Backend) Load(ctx context.Context, id string) (*Checkpoint, error) {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.cfg.Bucket),
		Key:    aws.String(b.key(id)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint from S3: %w", err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint data: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	return &cp, nil
}

// Delete removes a checkpoint from S3.
func (b *S3Backend) Delete(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.cfg.Bucket),
		Key:    aws.String(b.key(id)),
	})
	return err
}

// List returns all checkpoints with the given prefix.
func (b *S3Backend) List(ctx context.Context, prefix string) ([]*Checkpoint, error) {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	fullPrefix := b.cfg.Prefix
	if prefix != "" {
		fullPrefix += prefix
	}

	var checkpoints []*Checkpoint
	var continuationToken *string

	for {
		output, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.cfg.Bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list checkpoints: %w", err)
		}

		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)
			// Extract ID from key
			id := strings.TrimPrefix(key, b.cfg.Prefix)
			id = strings.TrimSuffix(id, ".json")

			cp, err := b.Load(ctx, id)
			if err != nil {
				continue // Skip invalid checkpoints
			}
			checkpoints = append(checkpoints, cp)
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return checkpoints, nil
}

// ListIncomplete returns all checkpoints that haven't completed.
func (b *S3Backend) ListIncomplete(ctx context.Context) ([]*Checkpoint, error) {
	all, err := b.List(ctx, "")
	if err != nil {
		return nil, err
	}

	var incomplete []*Checkpoint
	for _, cp := range all {
		if cp.Phase != "complete" {
			incomplete = append(incomplete, cp)
		}
	}

	return incomplete, nil
}

// FindByInput finds an incomplete checkpoint for the given input path.
func (b *S3Backend) FindByInput(ctx context.Context, inputPath string) (*Checkpoint, error) {
	incomplete, err := b.ListIncomplete(ctx)
	if err != nil {
		return nil, err
	}

	for _, cp := range incomplete {
		if cp.InputPath == inputPath {
			return cp, nil
		}
	}

	return nil, os.ErrNotExist
}

// Name returns "s3".
func (b *S3Backend) Name() string {
	return "s3"
}

// --- Lambda Integration Helpers ---

// LambdaCheckpointManager provides checkpoint management for AWS Lambda.
type LambdaCheckpointManager struct {
	backend *S3Backend
}

// NewLambdaCheckpointManager creates a checkpoint manager for Lambda.
func NewLambdaCheckpointManager(bucket string) (*LambdaCheckpointManager, error) {
	cfg := DefaultS3Config(bucket)
	cfg.Region = getEnvOrDefault("AWS_REGION", "us-east-1")

	backend, err := NewS3Backend(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	return &LambdaCheckpointManager{backend: backend}, nil
}

// FindOrCreate finds an existing checkpoint or creates a new one.
func (m *LambdaCheckpointManager) FindOrCreate(ctx context.Context, inputPath, outputPath string) (*Checkpoint, bool, error) {
	// Try to find existing checkpoint
	existing, err := m.backend.FindByInput(ctx, inputPath)
	if err == nil && existing.ShouldResume() {
		return existing, true, nil // Resuming
	}

	// Create new checkpoint
	cp := &Checkpoint{
		ID:         generateCheckpointID(),
		InputPath:  inputPath,
		OutputPath: outputPath,
		Phase:      "starting",
		StartedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Metadata:   make(map[string]interface{}),
	}

	if err := m.backend.Save(ctx, cp); err != nil {
		return nil, false, err
	}

	return cp, false, nil
}

// Save saves a checkpoint to S3.
func (m *LambdaCheckpointManager) Save(ctx context.Context, cp *Checkpoint) error {
	cp.UpdatedAt = time.Now()
	return m.backend.Save(ctx, cp)
}

// Complete marks a checkpoint as complete.
func (m *LambdaCheckpointManager) Complete(ctx context.Context, cp *Checkpoint) error {
	cp.Phase = "complete"
	now := time.Now()
	cp.CompletedAt = &now
	return m.backend.Save(ctx, cp)
}

// Cleanup removes old completed checkpoints.
func (m *LambdaCheckpointManager) Cleanup(ctx context.Context, maxAge time.Duration) (int, error) {
	all, err := m.backend.List(ctx, "")
	if err != nil {
		return 0, err
	}

	cutoff := time.Now().Add(-maxAge)
	removed := 0

	for _, cp := range all {
		if cp.Phase == "complete" && cp.UpdatedAt.Before(cutoff) {
			if err := m.backend.Delete(ctx, cp.ID); err == nil {
				removed++
			}
		}
	}

	return removed, nil
}

func generateCheckpointID() string {
	return fmt.Sprintf("cp_%d", time.Now().UnixNano())
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
