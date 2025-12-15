package cache

import (
	"context"
	"fmt"
	"time"

	"caching-api/internal/database"
)

type CacheStrategy interface {
	Get(ctx context.Context, key string) (map[string]any, error)
	Set(ctx context.Context, key string, value map[string]any) error
	Update(ctx context.Context, key string, value map[string]any) error
	Delete(ctx context.Context, key string) error
}

type CacheAsideStrategy struct {
	cache   CacheStore
	db      database.Database
	expiry  time.Duration
}

func NewCacheAsideStrategy(cache CacheStore, db database.Database) *CacheAsideStrategy {
	return &CacheAsideStrategy{
		cache:  cache,
		db:     db,
		expiry: 5 * time.Minute,
	}
}

func (c *CacheAsideStrategy) Get(ctx context.Context, key string) (map[string]any, error) {
	cached, err := c.cache.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	
	if cached != nil {
		return cached, nil
	}
	
	data, err := c.db.Get(key)
	if err != nil {
		return nil, err
	}
	
	if data != nil {
		if err := c.cache.Set(ctx, key, data, c.expiry); err != nil {
			fmt.Printf("Cache set error: %v\n", err)
		}
	}
	
	return data, nil
}

func (c *CacheAsideStrategy) Set(ctx context.Context, key string, value map[string]any) error {
	if err := c.db.Set(key, value); err != nil {
		return err
	}
	
	return c.cache.Set(ctx, key, value, c.expiry)
}

func (c *CacheAsideStrategy) Update(ctx context.Context, key string, value map[string]interface{}) error {
	if err := c.db.Update(key, value); err != nil {
		return err
	}
	
	updated, err := c.db.Get(key)
	if err != nil {
		return err
	}
	
	return c.cache.Set(ctx, key, updated, c.expiry)
}

func (c *CacheAsideStrategy) Delete(ctx context.Context, key string) error {
	if err := c.cache.Delete(ctx, key); err != nil {
		fmt.Printf("Cache delete error: %v\n", err)
	}
	
	return c.db.Delete(key)
}

type WriteThroughStrategy struct {
	cache   CacheStore
	db      database.Database
	expiry  time.Duration
}

func NewWriteThroughStrategy(cache CacheStore, db database.Database) *WriteThroughStrategy {
	return &WriteThroughStrategy{
		cache:  cache,
		db:     db,
		expiry: 10 * time.Minute,
	}
}

func (w *WriteThroughStrategy) Get(ctx context.Context, key string) (map[string]interface{}, error) {
	return w.cache.Get(ctx, key)
}

func (w *WriteThroughStrategy) Set(ctx context.Context, key string, value map[string]interface{}) error {
	if err := w.cache.Set(ctx, key, value, w.expiry); err != nil {
		return err
	}
	
	return w.db.Set(key, value)
}

func (w *WriteThroughStrategy) Update(ctx context.Context, key string, value map[string]interface{}) error {
	if err := w.cache.Set(ctx, key, value, w.expiry); err != nil {
		return err
	}
	
	return w.db.Update(key, value)
}

func (w *WriteThroughStrategy) Delete(ctx context.Context, key string) error {
	if err := w.cache.Delete(ctx, key); err != nil {
		fmt.Printf("Cache delete error: %v\n", err)
	}
	
	return w.db.Delete(key)
}