package gateway

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Config 是网关配置。
type Config struct {
	ListenAddr     string                `json:"listen_addr"`
	Path           string                `json:"path"`
	DefaultTimeout Duration              `json:"default_timeout"`
	MaxConcurrent  int                   `json:"max_concurrent"`
	QPS            int                   `json:"qps"`
	Providers      []ProviderConf        `json:"providers"`
	Methods        map[string]MethodConf `json:"methods"`
}

// ProviderConf 是 provider 的 JSON 配置。顺序即故障转移优先级。
type ProviderConf struct {
	Name           string   `json:"name"`
	URL            string   `json:"url"`
	Weight         int      `json:"weight"`
	Timeout        Duration `json:"timeout"`
	HealthCooldown Duration `json:"health_cooldown"`
}

// MethodConf 是方法策略的 JSON 配置。
type MethodConf struct {
	// ReadOnly=true 表示只读：可故障转移；Cacheable=true 表示按参数缓存。
	ReadOnly  bool     `json:"read_only"`
	Cacheable bool     `json:"cacheable"`
	TTL       Duration `json:"ttl"`
}

// Duration 是可从 JSON 字符串（如 "2s"、"500ms"）解析的 time.Duration。
type Duration time.Duration

// UnmarshalJSON 支持 "2s" 这样的字符串，也支持纳秒数字。
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	if parsed, err := time.ParseDuration(s); err == nil {
		*d = Duration(parsed)
		return nil
	}
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err == nil {
		*d = Duration(n)
		return nil
	}
	return fmt.Errorf("invalid duration %q: use e.g. \"2s\" or \"500ms\"", s)
}

// Std 转换为 time.Duration。
func (d Duration) Std() time.Duration { return time.Duration(d) }

// LoadConfig 从 JSON 文件读取配置。
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, NewError(CodeGatewayError, "cannot read config file", err, nil)
	}
	return ParseConfig(raw)
}

// ParseConfig 解析 JSON 配置。
func ParseConfig(raw []byte) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, NewError(CodeGatewayError, "invalid config json", err, nil)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate 校验配置并填充默认值。
func (c *Config) Validate() error {
	if c.ListenAddr == "" {
		c.ListenAddr = ":8080"
	}
	if c.Path == "" {
		c.Path = "/"
	}
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = Duration(10 * time.Second)
	}
	if len(c.Providers) == 0 {
		return NewError(CodeGatewayError, "config requires at least one provider", nil, nil)
	}
	names := make(map[string]struct{}, len(c.Providers))
	for i := range c.Providers {
		p := &c.Providers[i]
		if p.Name == "" || p.URL == "" {
			return NewError(CodeGatewayError,
				fmt.Sprintf("providers[%d] requires name and url", i), nil, nil)
		}
		if _, dup := names[p.Name]; dup {
			return NewError(CodeGatewayError,
				fmt.Sprintf("duplicate provider name %q", p.Name), nil, nil)
		}
		names[p.Name] = struct{}{}
	}
	for name, m := range c.Methods {
		if m.Cacheable && !m.ReadOnly {
			return NewError(CodeGatewayError,
				fmt.Sprintf("method %q is cacheable but not read_only", name), nil, nil)
		}
		if m.Cacheable && m.TTL <= 0 {
			return NewError(CodeGatewayError,
				fmt.Sprintf("method %q is cacheable but ttl not set", name), nil, nil)
		}
	}
	return nil
}

// Build 依据配置组装运行时 Gateway。
func (c *Config) Build(log Logger) (*Gateway, error) {
	providerConfigs := make([]ProviderConfig, 0, len(c.Providers))
	for _, p := range c.Providers {
		providerConfigs = append(providerConfigs, ProviderConfig{
			Name:           p.Name,
			URL:            p.URL,
			Weight:         p.Weight,
			Timeout:        p.Timeout.Std(),
			HealthCooldown: p.HealthCooldown.Std(),
		})
	}
	pool, err := NewProviderPool(providerConfigs)
	if err != nil {
		return nil, err
	}
	policies := make(map[string]MethodPolicy, len(c.Methods))
	for name, m := range c.Methods {
		policies[name] = MethodPolicy{
			ReadOnly:  m.ReadOnly,
			Cacheable: m.Cacheable,
			TTL:       m.TTL.Std(),
		}
	}
	router := NewMethodRouter(policies)
	cache := NewResultCache()
	quota := NewQuota(c.MaxConcurrent, c.QPS)
	return New(Options{
		Router:  router,
		Pool:    pool,
		Cache:   cache,
		Quota:   quota,
		Timeout: c.DefaultTimeout.Std(),
		Log:     log,
	}), nil
}
