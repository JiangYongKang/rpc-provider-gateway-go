package gateway

import (
	"encoding/json"
	"sync"
	"time"
)

// cacheEntry 是一条缓存结果。
type cacheEntry struct {
	value  json.RawMessage
	expire int64 // unix nano；0 表示永不过期（当前未使用，保留）
}

// ResultCache 按 "方法 + 规范化参数" 缓存只读方法的成功结果。
// 只缓存成功结果；错误、超时等一律不缓存。
type ResultCache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
	now     func() time.Time
}

// NewResultCache 创建结果缓存。
func NewResultCache() *ResultCache {
	return &ResultCache{entries: make(map[string]cacheEntry), now: time.Now}
}

// cacheKey 依据方法与参数生成稳定的缓存键。
// 参数会被解码后重新编码，使对象参数的键顺序不影响缓存命中。
func cacheKey(method string, params json.RawMessage) (string, error) {
	canonical, err := canonicalParams(params)
	if err != nil {
		return "", err
	}
	return method + "#" + string(canonical), nil
}

// canonicalParams 返回规范化的参数 JSON；空参数统一为 []。
func canonicalParams(params json.RawMessage) (json.RawMessage, error) {
	if len(params) == 0 {
		return json.RawMessage("[]"), nil
	}
	var v any
	if err := json.Unmarshal(params, &v); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// Get 命中且未过期时返回结果；未命中或已过期返回 nil, false。
func (c *ResultCache) Get(method string, params json.RawMessage) (json.RawMessage, bool) {
	key, err := cacheKey(method, params)
	if err != nil {
		return nil, false
	}
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if e.expire != 0 && e.expire <= c.now().UnixNano() {
		c.mu.Lock()
		// 双检，避免与并发 Set 竞争时误删新值。
		if cur, ok := c.entries[key]; ok && cur.expire == e.expire {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return nil, false
	}
	return e.value, true
}

// Set 写入缓存，ttl 后失效。ttl <= 0 时不写入。
func (c *ResultCache) Set(method string, params, value json.RawMessage, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	key, err := cacheKey(method, params)
	if err != nil {
		return
	}
	c.mu.Lock()
	c.entries[key] = cacheEntry{value: value, expire: c.now().Add(ttl).UnixNano()}
	c.mu.Unlock()
}

// Invalidate 清除指定方法与参数的缓存（测试 / 主动失效用）。
func (c *ResultCache) Invalidate(method string, params json.RawMessage) {
	key, err := cacheKey(method, params)
	if err != nil {
		return
	}
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// Len 返回当前缓存条目数（测试用）。
func (c *ResultCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
