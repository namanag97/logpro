// Package s3 provides AWS S3 storage implementation with full SDK integration.
package s3

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Config holds S3 client configuration.
type Config struct {
	// Region is the AWS region (e.g., "us-east-1")
	Region string

	// Bucket is the default bucket name
	Bucket string

	// Endpoint overrides the default S3 endpoint (for S3-compatible services)
	Endpoint string

	// UsePathStyle forces path-style addressing (for MinIO, LocalStack)
	UsePathStyle bool

	// Credentials (optional - uses default chain if not provided)
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// Timeouts
	OperationTimeout time.Duration
	UploadTimeout    time.Duration
	DownloadTimeout  time.Duration

	// Multipart upload configuration
	PartSize          int64 // Size of each part in bytes (default: 5MB)
	Concurrency       int   // Number of concurrent uploads (default: 5)
	LeavePartsOnError bool  // Keep parts on failed upload for resume
}

// DefaultConfig returns sensible defaults for S3 configuration.
func DefaultConfig(bucket, region string) Config {
	return Config{
		Bucket:           bucket,
		Region:           region,
		OperationTimeout: 30 * time.Second,
		UploadTimeout:    5 * time.Minute,
		DownloadTimeout:  5 * time.Minute,
		PartSize:         5 * 1024 * 1024, // 5MB
		Concurrency:      5,
	}
}

// Client provides S3 operations.
type Client struct {
	mu     sync.RWMutex
	cfg    Config
	client *s3.Client
}

// NewClient creates a new S3 client.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
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

	// Create S3 client
	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &Client{
		cfg:    cfg,
		client: client,
	}, nil
}

// Bucket returns the default bucket name.
func (c *Client) Bucket() string {
	return c.cfg.Bucket
}

// --- Read Operations ---

// Reader returns a reader for the given key.
func (c *Client) Reader(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return c.ReaderFromBucket(ctx, c.cfg.Bucket, key)
}

// ReaderFromBucket returns a reader for a key in a specific bucket.
func (c *Client) ReaderFromBucket(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.DownloadTimeout)

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := c.client.GetObject(ctx, input)
	if err != nil {
		cancel()
		return nil, 0, fmt.Errorf("failed to get object %s/%s: %w", bucket, key, err)
	}

	// Wrap to cancel context on close
	return &cancelOnCloseReader{
		ReadCloser: output.Body,
		cancel:     cancel,
	}, aws.ToInt64(output.ContentLength), nil
}

// ReaderWithRange returns a reader for a byte range of the object.
func (c *Client) ReaderWithRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.DownloadTimeout)

	rangeStr := fmt.Sprintf("bytes=%d-%d", start, end)
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeStr),
	}

	output, err := c.client.GetObject(ctx, input)
	if err != nil {
		cancel()
		return nil, 0, fmt.Errorf("failed to get object range %s: %w", key, err)
	}

	return &cancelOnCloseReader{
		ReadCloser: output.Body,
		cancel:     cancel,
	}, aws.ToInt64(output.ContentLength), nil
}

type cancelOnCloseReader struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *cancelOnCloseReader) Close() error {
	r.cancel()
	return r.ReadCloser.Close()
}

// --- Write Operations ---

// Writer returns a writer for the given key.
func (c *Client) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	return c.WriterWithOptions(ctx, c.cfg.Bucket, key, WriteOptions{})
}

// WriteOptions configures write operations.
type WriteOptions struct {
	ContentType     string
	ContentEncoding string
	Metadata        map[string]string
	StorageClass    types.StorageClass
	ACL             types.ObjectCannedACL
}

// WriterWithOptions returns a writer with custom options.
func (c *Client) WriterWithOptions(ctx context.Context, bucket, key string, opts WriteOptions) (io.WriteCloser, error) {
	return newS3Writer(c.client, bucket, key, c.cfg, opts), nil
}

// s3Writer implements io.WriteCloser for S3 uploads.
type s3Writer struct {
	client *s3.Client
	bucket string
	key    string
	cfg    Config
	opts   WriteOptions

	mu       sync.Mutex
	buf      []byte
	parts    []types.CompletedPart
	uploadID string
	partNum  int32
	closed   bool
	err      error
}

func newS3Writer(client *s3.Client, bucket, key string, cfg Config, opts WriteOptions) *s3Writer {
	return &s3Writer{
		client: client,
		bucket: bucket,
		key:    key,
		cfg:    cfg,
		opts:   opts,
		buf:    make([]byte, 0, cfg.PartSize),
	}
}

func (w *s3Writer) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, fmt.Errorf("writer is closed")
	}
	if w.err != nil {
		return 0, w.err
	}

	w.buf = append(w.buf, p...)

	// Upload part if buffer exceeds part size
	for int64(len(w.buf)) >= w.cfg.PartSize {
		if err := w.uploadPartLocked(); err != nil {
			w.err = err
			return len(p), err
		}
	}

	return len(p), nil
}

func (w *s3Writer) uploadPartLocked() error {
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.UploadTimeout)
	defer cancel()

	// Initialize multipart upload if needed
	if w.uploadID == "" {
		input := &s3.CreateMultipartUploadInput{
			Bucket:      aws.String(w.bucket),
			Key:         aws.String(w.key),
			ContentType: aws.String(w.opts.ContentType),
		}
		if w.opts.StorageClass != "" {
			input.StorageClass = w.opts.StorageClass
		}
		if len(w.opts.Metadata) > 0 {
			input.Metadata = w.opts.Metadata
		}

		output, err := w.client.CreateMultipartUpload(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to create multipart upload: %w", err)
		}
		w.uploadID = aws.ToString(output.UploadId)
	}

	// Upload part
	w.partNum++
	partData := w.buf[:w.cfg.PartSize]

	input := &s3.UploadPartInput{
		Bucket:     aws.String(w.bucket),
		Key:        aws.String(w.key),
		UploadId:   aws.String(w.uploadID),
		PartNumber: aws.Int32(w.partNum),
		Body:       &bytesReader{data: partData},
	}

	output, err := w.client.UploadPart(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload part %d: %w", w.partNum, err)
	}

	w.parts = append(w.parts, types.CompletedPart{
		ETag:       output.ETag,
		PartNumber: aws.Int32(w.partNum),
	})

	// Remove uploaded data from buffer
	w.buf = w.buf[w.cfg.PartSize:]

	return nil
}

func (w *s3Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if w.err != nil {
		return w.err
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.UploadTimeout)
	defer cancel()

	// If we never started multipart (small file), do simple PUT
	if w.uploadID == "" {
		input := &s3.PutObjectInput{
			Bucket:      aws.String(w.bucket),
			Key:         aws.String(w.key),
			Body:        &bytesReader{data: w.buf},
			ContentType: aws.String(w.opts.ContentType),
		}
		if w.opts.StorageClass != "" {
			input.StorageClass = w.opts.StorageClass
		}
		if len(w.opts.Metadata) > 0 {
			input.Metadata = w.opts.Metadata
		}

		_, err := w.client.PutObject(ctx, input)
		return err
	}

	// Upload remaining data as final part
	if len(w.buf) > 0 {
		w.partNum++
		input := &s3.UploadPartInput{
			Bucket:     aws.String(w.bucket),
			Key:        aws.String(w.key),
			UploadId:   aws.String(w.uploadID),
			PartNumber: aws.Int32(w.partNum),
			Body:       &bytesReader{data: w.buf},
		}

		output, err := w.client.UploadPart(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to upload final part: %w", err)
		}

		w.parts = append(w.parts, types.CompletedPart{
			ETag:       output.ETag,
			PartNumber: aws.Int32(w.partNum),
		})
	}

	// Complete multipart upload
	_, err := w.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(w.bucket),
		Key:      aws.String(w.key),
		UploadId: aws.String(w.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: w.parts,
		},
	})

	return err
}

// bytesReader implements io.Reader for a byte slice.
type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// --- Metadata Operations ---

// Stat returns file info for the given key.
func (c *Client) Stat(ctx context.Context, key string) (*ObjectInfo, error) {
	return c.StatFromBucket(ctx, c.cfg.Bucket, key)
}

// ObjectInfo holds S3 object metadata.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
	Metadata     map[string]string
	StorageClass types.StorageClass
}

// StatFromBucket returns object info from a specific bucket.
func (c *Client) StatFromBucket(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.OperationTimeout)
	defer cancel()

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := c.client.HeadObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to head object %s/%s: %w", bucket, key, err)
	}

	return &ObjectInfo{
		Key:          key,
		Size:         aws.ToInt64(output.ContentLength),
		LastModified: aws.ToTime(output.LastModified),
		ETag:         aws.ToString(output.ETag),
		ContentType:  aws.ToString(output.ContentType),
		Metadata:     output.Metadata,
		StorageClass: output.StorageClass,
	}, nil
}

// Exists checks if an object exists.
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.Stat(ctx, key)
	if err != nil {
		// Check if it's a "not found" error
		return false, nil
	}
	return true, nil
}

// --- Delete Operations ---

// Delete removes an object.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.DeleteFromBucket(ctx, c.cfg.Bucket, key)
}

// DeleteFromBucket removes an object from a specific bucket.
func (c *Client) DeleteFromBucket(ctx context.Context, bucket, key string) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.OperationTimeout)
	defer cancel()

	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

// DeleteMany removes multiple objects.
func (c *Client) DeleteMany(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, c.cfg.OperationTimeout)
	defer cancel()

	objects := make([]types.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objects[i] = types.ObjectIdentifier{Key: aws.String(key)}
	}

	_, err := c.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(c.cfg.Bucket),
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	})
	return err
}

// --- List Operations ---

// ListOptions configures list operations.
type ListOptions struct {
	Prefix     string
	Delimiter  string
	MaxKeys    int32
	StartAfter string
}

// List lists objects with the given prefix.
func (c *Client) List(ctx context.Context, opts ListOptions) ([]ObjectInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.OperationTimeout)
	defer cancel()

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.cfg.Bucket),
	}
	if opts.Prefix != "" {
		input.Prefix = aws.String(opts.Prefix)
	}
	if opts.Delimiter != "" {
		input.Delimiter = aws.String(opts.Delimiter)
	}
	if opts.MaxKeys > 0 {
		input.MaxKeys = aws.Int32(opts.MaxKeys)
	}
	if opts.StartAfter != "" {
		input.StartAfter = aws.String(opts.StartAfter)
	}

	output, err := c.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	objects := make([]ObjectInfo, len(output.Contents))
	for i, obj := range output.Contents {
		objects[i] = ObjectInfo{
			Key:          aws.ToString(obj.Key),
			Size:         aws.ToInt64(obj.Size),
			LastModified: aws.ToTime(obj.LastModified),
			ETag:         aws.ToString(obj.ETag),
			StorageClass: types.StorageClass(obj.StorageClass),
		}
	}

	return objects, nil
}

// ListAll lists all objects with pagination.
func (c *Client) ListAll(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var allObjects []ObjectInfo
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(c.cfg.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		}

		output, err := c.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range output.Contents {
			allObjects = append(allObjects, ObjectInfo{
				Key:          aws.ToString(obj.Key),
				Size:         aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
				ETag:         aws.ToString(obj.ETag),
				StorageClass: types.StorageClass(obj.StorageClass),
			})
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return allObjects, nil
}

// --- Copy Operations ---

// Copy copies an object within S3.
func (c *Client) Copy(ctx context.Context, srcKey, dstKey string) error {
	return c.CopyBetweenBuckets(ctx, c.cfg.Bucket, srcKey, c.cfg.Bucket, dstKey)
}

// CopyBetweenBuckets copies an object between buckets.
func (c *Client) CopyBetweenBuckets(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.OperationTimeout)
	defer cancel()

	copySource := fmt.Sprintf("%s/%s", srcBucket, srcKey)
	_, err := c.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(copySource),
	})
	return err
}

// --- Presigned URLs ---

// PresignedGetURL generates a presigned URL for GET operations.
func (c *Client) PresignedGetURL(ctx context.Context, key string, expires time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(c.client)

	input := &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	}

	resp, err := presignClient.PresignGetObject(ctx, input, func(opts *s3.PresignOptions) {
		opts.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("failed to presign GET URL: %w", err)
	}

	return resp.URL, nil
}

// PresignedPutURL generates a presigned URL for PUT operations.
func (c *Client) PresignedPutURL(ctx context.Context, key string, expires time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(c.client)

	input := &s3.PutObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	}

	resp, err := presignClient.PresignPutObject(ctx, input, func(opts *s3.PresignOptions) {
		opts.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("failed to presign PUT URL: %w", err)
	}

	return resp.URL, nil
}

// --- Storage Interface Compliance ---

// Scheme returns "s3".
func (c *Client) Scheme() string {
	return "s3"
}
