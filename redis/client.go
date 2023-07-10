package redisclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
)

var ctx = context.Background()

// Client is a Redis client
type Client struct {
	rdb *redis.Client
}

// NewClient creates a new Redis client
func NewClient() (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.host"),
		Password: viper.GetString("redis.password"),
		Username: viper.GetString("redis.username"),
	})

	// Check if the connection is successful
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

// Close closes the Redis client connection
func (c *Client) Close() error {
	if c.rdb != nil {
		return c.rdb.Close()
	}
	return nil
}

// Example function: Set a key-value pair in Redis
func (c *Client) Set(key string, value string, expiration time.Duration) error {
	err := c.rdb.Set(ctx, key, value, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set key-value pair in Redis: %w", err)
	}
	return nil
}

// Example function: Get a value from Redis by key
func (c *Client) Get(key string) (string, error) {
	value, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return "", fmt.Errorf("failed to get value from Redis: %w", err)
	}
	return value, nil
}

// ZAdd adds a member with a score to a sorted set in Redis
func (c *Client) ZAdd(key string, score float64, member interface{}) error {
	// Marshal the member into a JSON string
	jsonData, err := json.Marshal(member)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON member: %w", err)
	}

	// Add the member to the sorted set with a score
	err = c.rdb.ZAdd(context.Background(), key, &redis.Z{
		Score:  score,
		Member: string(jsonData),
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to add member to sorted set: %w", err)
	}

	return nil
}

// ZRangeByLex retrieves members from a sorted set by lexicographical ordering
func (c *Client) ZRangeByLex(key string, min string, max string) ([]string, error) {
	results, err := c.rdb.ZRangeByLex(context.Background(), key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve range by lexicographical ordering: %w", err)
	}

	return results, nil
}
