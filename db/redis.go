package db

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Client struct {
	redisClient *redis.Client
	tracer      trace.Tracer
}

// NewClient creates a new redis client and verifies connectivity using PING
func NewClient(ctx context.Context, addr string) (*Client, error) {
	c := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	if _, err := c.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &Client{
		redisClient: c,
		tracer:      otel.Tracer("redis"),
	}, nil
}

// Get returns the order with the given ID
func (c *Client) Get(ctx context.Context, id string) (string, error) {
	ctx, span := c.tracer.Start(ctx, "get", trace.WithAttributes(attribute.String("id", id)))
	defer span.End()
	return c.redisClient.Get(ctx, id).Result()
}

func (c *Client) Close() {
	c.redisClient.Close()
}
