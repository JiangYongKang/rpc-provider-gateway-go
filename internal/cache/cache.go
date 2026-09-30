// Package cache caches successful results of read-only JSON-RPC methods,
// keyed by method + params. Entries expire after a configured TTL.
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"
)

// Entry is one cached upstream result.
type Entry struct {
	Value     json.RawMessage
	ExpiresAt time.Time
}

// Cache is a concurrency-safe TTL result cache.
type Cache struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

// New creates an empty cache.
func New() *Cache {
	return &Cache{entries: make(map[string]Entry)}
}

// Key builds a stable cache key from method and params. Params are normalized
// through JSON first so semantically equal params share an entry.
func Key(method string, params json.RawMessage) string {
	p := params
	if len(p) == 0 {
		p = json.RawMessage("null")
	} else {
		var v any
		if err := json.Unmarshal(p, &v); err == nil {
			if norm, err := json.Marshal(v); err == nil {
				p = norm
			}
		}
	}
	sum := sha256.Sum256(append([]byte(method), p...))
	return method + ":" + hex.EncodeToString(sum[:])
}

// Get returns a non-expired entry. Expired entries are removed on access.
func (c *Cache) Get(key string) (json.RawMessage, bool) {
	now := time.Now()
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if now.Before(e.ExpiresAt) {
		return e.Value, true
	}
	c.mu.Lock()
	// Re-check under the write lock; another goroutine may have refreshed it.
	if cur, ok := c.entries[key]; ok && now.Before(cur.ExpiresAt) {
		c.mu.Unlock()
		return cur.Value, true
	}
	delete(c.entries, key)
	c.mu.Unlock()
	return nil, false
}

// Set stores an entry with the given TTL.
func (c *Cache) Set(key string, value json.RawMessage, ttl time.Duration) {
	if ttl <= 0 || len(value) == 0 {
		return
	}
	cp := append(json.RawMessage(nil), value...)
	c.mu.Lock()
	c.entries[key] = Entry{Value: cp, ExpiresAt: time.Now().Add(ttl)}
	c.mu.Unlock()
}

// DeleteExpired removes all expired entries and returns how many were removed.
func (c *Cache) DeleteExpired(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, e := range c.entries {
		if !now.Before(e.ExpiresAt) {
			delete(c.entries, k)
			n++
		}
	}
	return n
}

// Len returns the number of entries currently held (including stale ones not
// yet swept).
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
