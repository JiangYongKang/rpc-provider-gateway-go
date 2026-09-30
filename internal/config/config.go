// Package config defines gateway configuration and its JSON file format.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Config is the root gateway configuration.
type Config struct {
	// ListenAddr is the HTTP listen address, e.g. ":8080".
	ListenAddr string `json:"listen_addr"`

	// Providers is the ordered failover list: the first healthy provider is
	// tried first, then the next, and so on.
	Providers []ProviderConfig `json:"providers"`

	// DefaultTimeout is the per-upstream-call timeout when a provider does
	// not override it.
	DefaultTimeout Duration `json:"default_timeout"`

	// MaxBatchSize limits the number of requests in one JSON-RPC batch.
	MaxBatchSize int `json:"max_batch_size"`

	// MaxConcurrent limits the number of in-flight calls gateway-wide.
	MaxConcurrent int `json:"max_concurrent"`

	// RequestsPerSecond limits accepted calls per second (token bucket).
	RequestsPerSecond int `json:"requests_per_second"`

	// Cache configures read-only result caching.
	Cache CacheConfig `json:"cache"`

	// IdempotentMethods are read methods eligible for failover retries.
	IdempotentMethods []string `json:"idempotent_methods"`

	// NonRetryableMethods must never be retried on another provider
	// (send-type non-idempotent calls). They are sent at most once.
	NonRetryableMethods []string `json:"non_retryable_methods"`
}

// ProviderConfig configures one backend provider.
type ProviderConfig struct {
	// Name is the unique provider identifier used in logs and error data.
	Name string `json:"name"`
	// URL is the JSON-RPC HTTP endpoint.
	URL string `json:"url"`
	// Weight is reserved for future weighted selection; order is used today.
	Weight int `json:"weight"`
	// Timeout overrides DefaultTimeout when positive.
	Timeout Duration `json:"timeout"`
	// Healthy is the initial availability state.
	Healthy bool `json:"healthy"`
	// Methods restricts which methods this provider serves; empty = all.
	Methods []string `json:"methods"`
}

// CacheConfig configures read-only result caching.
type CacheConfig struct {
	// Enabled turns caching on/off.
	Enabled bool `json:"enabled"`
	// TTL is how long cached results stay fresh.
	TTL Duration `json:"ttl"`
	// Methods lists read-only methods whose successful results are cached.
	Methods []string `json:"methods"`
}

// Default returns a config with sane defaults and the standard blockchain
// RPC method classification.
func Default() *Config {
	return &Config{
		ListenAddr:        ":8080",
		DefaultTimeout:    Duration{5 * time.Second},
		MaxBatchSize:      100,
		MaxConcurrent:     64,
		RequestsPerSecond: 0, // unlimited
		IdempotentMethods: []string{
			"eth_getBalance", "eth_getTransactionByHash", "eth_getTransactionReceipt",
			"eth_blockNumber", "eth_getBlockByNumber", "eth_getBlockByHash",
			"eth_getCode", "eth_call", "eth_estimateGas", "eth_chainId",
			"eth_getTransactionCount", "net_version", "web3_clientVersion",
		},
		NonRetryableMethods: []string{
			"eth_sendRawTransaction", "eth_sendTransaction",
			"personal_sendTransaction", "eth_sign", "personal_sign",
		},
		Cache: CacheConfig{
			Enabled: true,
			TTL:     Duration{2 * time.Second},
			Methods: []string{
				"eth_getBalance", "eth_blockNumber", "eth_getCode",
				"eth_chainId", "net_version", "web3_clientVersion",
			},
		},
	}
}

// Load reads and validates a JSON config file. Unset fields are filled from
// Default().
func Load(path string) (*Config, error) {
	cfg := Default()
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks required fields and sane quotas.
func (c *Config) Validate() error {
	if len(c.Providers) == 0 {
		return fmt.Errorf("config: at least one provider is required")
	}
	names := map[string]bool{}
	for i, p := range c.Providers {
		if p.Name == "" {
			return fmt.Errorf("config: provider[%d] has no name", i)
		}
		if names[p.Name] {
			return fmt.Errorf("config: duplicate provider name %q", p.Name)
		}
		names[p.Name] = true
		if p.URL == "" {
			return fmt.Errorf("config: provider %q has no url", p.Name)
		}
		if p.Timeout.Duration == 0 {
			p.Timeout.Duration = c.DefaultTimeout.Duration
			c.Providers[i] = p
		}
	}
	if c.DefaultTimeout.Duration <= 0 {
		return fmt.Errorf("config: default_timeout must be positive")
	}
	if c.MaxBatchSize < 1 {
		c.MaxBatchSize = 1
	}
	if c.MaxConcurrent < 0 || c.RequestsPerSecond < 0 {
		return fmt.Errorf("config: quota values must be non-negative")
	}
	if c.Cache.Enabled && c.Cache.TTL.Duration <= 0 {
		return fmt.Errorf("config: cache.ttl must be positive when cache is enabled")
	}
	return nil
}

// ServesMethod reports whether the provider is configured to serve a method.
func (p *ProviderConfig) ServesMethod(method string) bool {
	if len(p.Methods) == 0 {
		return true
	}
	for _, m := range p.Methods {
		if m == method {
			return true
		}
	}
	return false
}
