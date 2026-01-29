// Package checkpoint provides Redis-backed checkpoint persistence for low-latency access.
package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig configures the Redis checkpoint backend.
type RedisConfig struct {
	// Address is the Redis server address (e.g., "localhost:6379")
	Address string

	// Password for Redis authentication (optional)
	Password string

	// Database number to use (default: 0)
	Database int

	// Prefix is prepended to all checkpoint keys (e.g., "logflow:checkpoints:")
	Prefix string

	// TTL is the time-to-live for checkpoint keys (0 = no expiration)
	TTL time.Duration

	// Timeout for Redis operations
	Timeout time.Duration

	// PoolSize is the maximum number of connections
	PoolSize int

	// MinIdleConns is the minimum number of idle connections
	MinIdleConns int

	// TLS enables TLS connection
	TLS bool
}

// DefaultRedisConfig returns sensible defaults.
func DefaultRedisConfig(address string) RedisConfig {
	return RedisConfig{
		Address:      address,
		Prefix:       "logflow:checkpoints:",
		TTL:          24 * time.Hour, // Keep checkpoints for 24 hours
		Timeout:      5 * time.Second,
		PoolSize:     10,
		MinIdleConns: 2,
	}
}

// RedisBackend stores checkpoints in Redis for low-latency access.
type RedisBackend struct {
	cfg    RedisConfig
	client *redis.Client
}

// NewRedisBackend creates a new Redis checkpoint backend.
func NewRedisBackend(cfg RedisConfig) (*RedisBackend, error) {
	opts := &redis.Options{
		Addr:         cfg.Address,
		Password:     cfg.Password,
		DB:           cfg.Database,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		ReadTimeout:  cfg.Timeout,
		WriteTimeout: cfg.Timeout,
	}

	client := redis.NewClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisBackend{
		cfg:    cfg,
		client: client,
	}, nil
}

// key returns the Redis key for a checkpoint ID.
func (b *RedisBackend) key(id string) string {
	return b.cfg.Prefix + id
}

// inputIndexKey returns the key for the input path index.
func (b *RedisBackend) inputIndexKey(inputPath string) string {
	return b.cfg.Prefix + "index:input:" + sanitizeKey(inputPath)
}

// incompleteSetKey returns the key for the incomplete checkpoints set.
func (b *RedisBackend) incompleteSetKey() string {
	return b.cfg.Prefix + "incomplete"
}

// sanitizeKey removes characters that may cause issues in Redis keys.
func sanitizeKey(s string) string {
	// Replace path separators and other special chars
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, ":", "_")
	s = strings.ReplaceAll(s, " ", "_")
	return s
}

// Save persists a checkpoint to Redis.
func (b *RedisBackend) Save(ctx context.Context, cp *Checkpoint) error {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	// Serialize checkpoint
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Use pipeline for atomic operations
	pipe := b.client.Pipeline()

	// Set the checkpoint data
	if b.cfg.TTL > 0 {
		pipe.Set(ctx, b.key(cp.ID), data, b.cfg.TTL)
	} else {
		pipe.Set(ctx, b.key(cp.ID), data, 0)
	}

	// Update input path index
	pipe.Set(ctx, b.inputIndexKey(cp.InputPath), cp.ID, b.cfg.TTL)

	// Update incomplete set
	if cp.Phase != "complete" {
		pipe.SAdd(ctx, b.incompleteSetKey(), cp.ID)
	} else {
		pipe.SRem(ctx, b.incompleteSetKey(), cp.ID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to save checkpoint to Redis: %w", err)
	}

	return nil
}

// Load retrieves a checkpoint from Redis.
func (b *RedisBackend) Load(ctx context.Context, id string) (*Checkpoint, error) {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	data, err := b.client.Get(ctx, b.key(id)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("failed to load checkpoint from Redis: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	return &cp, nil
}

// Delete removes a checkpoint from Redis.
func (b *RedisBackend) Delete(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	// First load to get input path for index cleanup
	cp, err := b.Load(ctx, id)
	if err != nil && err != os.ErrNotExist {
		return err
	}

	pipe := b.client.Pipeline()

	// Delete checkpoint data
	pipe.Del(ctx, b.key(id))

	// Remove from incomplete set
	pipe.SRem(ctx, b.incompleteSetKey(), id)

	// Remove input index if we have the checkpoint
	if cp != nil {
		pipe.Del(ctx, b.inputIndexKey(cp.InputPath))
	}

	_, err = pipe.Exec(ctx)
	return err
}

// List returns all checkpoints with the given prefix.
func (b *RedisBackend) List(ctx context.Context, prefix string) ([]*Checkpoint, error) {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	pattern := b.cfg.Prefix
	if prefix != "" {
		pattern += prefix + "*"
	} else {
		pattern += "*"
	}

	// Scan for matching keys
	var keys []string
	iter := b.client.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		// Skip index keys
		if strings.Contains(key, ":index:") || strings.HasSuffix(key, ":incomplete") {
			continue
		}
		keys = append(keys, key)
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan keys: %w", err)
	}

	if len(keys) == 0 {
		return nil, nil
	}

	// Load all checkpoints
	var checkpoints []*Checkpoint
	for _, key := range keys {
		id := strings.TrimPrefix(key, b.cfg.Prefix)
		cp, err := b.Load(ctx, id)
		if err != nil {
			continue // Skip invalid checkpoints
		}
		checkpoints = append(checkpoints, cp)
	}

	return checkpoints, nil
}

// ListIncomplete returns all checkpoints that haven't completed.
func (b *RedisBackend) ListIncomplete(ctx context.Context) ([]*Checkpoint, error) {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	// Get all IDs from the incomplete set
	ids, err := b.client.SMembers(ctx, b.incompleteSetKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get incomplete checkpoints: %w", err)
	}

	var checkpoints []*Checkpoint
	for _, id := range ids {
		cp, err := b.Load(ctx, id)
		if err != nil {
			// Remove stale entries
			b.client.SRem(ctx, b.incompleteSetKey(), id)
			continue
		}
		if cp.Phase != "complete" {
			checkpoints = append(checkpoints, cp)
		} else {
			// Remove completed entries from set
			b.client.SRem(ctx, b.incompleteSetKey(), id)
		}
	}

	return checkpoints, nil
}

// FindByInput finds an incomplete checkpoint for the given input path.
func (b *RedisBackend) FindByInput(ctx context.Context, inputPath string) (*Checkpoint, error) {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	// Look up in index
	id, err := b.client.Get(ctx, b.inputIndexKey(inputPath)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("failed to find checkpoint by input: %w", err)
	}

	cp, err := b.Load(ctx, id)
	if err != nil {
		return nil, err
	}

	// Verify it's incomplete
	if cp.Phase == "complete" {
		return nil, os.ErrNotExist
	}

	return cp, nil
}

// Name returns "redis".
func (b *RedisBackend) Name() string {
	return "redis"
}

// Close closes the Redis connection.
func (b *RedisBackend) Close() error {
	return b.client.Close()
}

// --- Distributed Locking for Multi-Worker Support ---

// Lock represents a distributed lock.
type Lock struct {
	backend *RedisBackend
	key     string
	value   string
	ttl     time.Duration
}

// AcquireLock attempts to acquire a distributed lock for a checkpoint.
func (b *RedisBackend) AcquireLock(ctx context.Context, checkpointID string, ttl time.Duration) (*Lock, error) {
	lockKey := b.cfg.Prefix + "lock:" + checkpointID
	lockValue := fmt.Sprintf("%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()

	// Try to acquire lock with SET NX EX
	ok, err := b.client.SetNX(ctx, lockKey, lockValue, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("lock already held")
	}

	return &Lock{
		backend: b,
		key:     lockKey,
		value:   lockValue,
		ttl:     ttl,
	}, nil
}

// Release releases the distributed lock.
func (l *Lock) Release(ctx context.Context) error {
	// Use Lua script to ensure we only release our own lock
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)

	_, err := script.Run(ctx, l.backend.client, []string{l.key}, l.value).Result()
	return err
}

// Extend extends the lock TTL.
func (l *Lock) Extend(ctx context.Context) error {
	// Use Lua script to extend only our own lock
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	ttlMs := l.ttl.Milliseconds()
	result, err := script.Run(ctx, l.backend.client, []string{l.key}, l.value, ttlMs).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return fmt.Errorf("lock no longer held")
	}
	return nil
}

// --- Health Check ---

// Ping checks the Redis connection.
func (b *RedisBackend) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.Timeout)
	defer cancel()
	return b.client.Ping(ctx).Err()
}

// Stats returns Redis connection pool statistics.
func (b *RedisBackend) Stats() *redis.PoolStats {
	return b.client.PoolStats()
}
