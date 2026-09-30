package gateway

import (
	"testing"
	"time"
)

func TestDuration_Unmarshal(t *testing.T) {
	var d Duration
	if err := d.UnmarshalJSON([]byte(`"2500ms"`)); err != nil {
		t.Fatal(err)
	}
	if d.Std() != 2500*time.Millisecond {
		t.Fatalf("want 2.5s, got %v", d.Std())
	}
	if err := d.UnmarshalJSON([]byte(`"bad"`)); err == nil {
		t.Fatal("invalid duration must error")
	}
}

const validConfig = `{
  "listen_addr": "127.0.0.1:18080",
  "path": "/rpc",
  "default_timeout": "3s",
  "max_concurrent": 100,
  "qps": 200,
  "providers": [
    {"name": "primary", "url": "http://127.0.0.1:9001", "health_cooldown": "1s"},
    {"name": "backup", "url": "http://127.0.0.1:9002", "timeout": "2s"}
  ],
  "methods": {
    "eth_blockNumber": {"read_only": true, "cacheable": true, "ttl": "500ms"},
    "eth_call":        {"read_only": true},
    "eth_sendRawTransaction": {}
  }
}`

func TestConfig_ParseAndDefaults(t *testing.T) {
	cfg, err := ParseConfig([]byte(validConfig))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.ListenAddr != "127.0.0.1:18080" || cfg.Path != "/rpc" {
		t.Fatalf("addr/path wrong: %q %q", cfg.ListenAddr, cfg.Path)
	}
	if cfg.DefaultTimeout.Std() != 3*time.Second {
		t.Fatalf("timeout %v", cfg.DefaultTimeout)
	}
	if cfg.Providers[0].HealthCooldown.Std() != time.Second {
		t.Fatalf("cooldown %v", cfg.Providers[0].HealthCooldown)
	}
	m := cfg.Methods["eth_blockNumber"]
	if !m.ReadOnly || !m.Cacheable || m.TTL.Std() != 500*time.Millisecond {
		t.Fatalf("method policy wrong: %+v", m)
	}
}

func TestConfig_DefaultsApplied(t *testing.T) {
	cfg, err := ParseConfig([]byte(`{"providers":[{"name":"a","url":"http://x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":8080" || cfg.Path != "/" {
		t.Fatalf("defaults not applied: %q %q", cfg.ListenAddr, cfg.Path)
	}
	if cfg.DefaultTimeout.Std() != 10*time.Second {
		t.Fatalf("default timeout: %v", cfg.DefaultTimeout)
	}
}

func TestConfig_ValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{"no providers", `{}`},
		{"provider missing url", `{"providers":[{"name":"a"}]}`},
		{"duplicate provider name", `{"providers":[{"name":"a","url":"http://1"},{"name":"a","url":"http://2"}]}`},
		{"cacheable non-readonly", `{"providers":[{"name":"a","url":"http://x"}],"methods":{"m":{"cacheable":true,"ttl":"1s"}}}`},
		{"cacheable without ttl", `{"providers":[{"name":"a","url":"http://x"}],"methods":{"m":{"read_only":true,"cacheable":true}}}`},
		{"bad json", `{not json`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseConfig([]byte(tc.json)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestConfig_Build(t *testing.T) {
	cfg, err := ParseConfig([]byte(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	gw, err := cfg.Build(nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if gw.pool.Len() != 2 {
		t.Fatalf("want 2 providers, got %d", gw.pool.Len())
	}
	if !gw.router.IsSupported("eth_sendRawTransaction") {
		t.Fatal("sendRawTransaction should be registered")
	}
	p := gw.router.Policy("eth_sendRawTransaction")
	if p.ReadOnly || p.Cacheable {
		t.Fatalf("send tx must be non-readonly/non-cacheable: %+v", p)
	}
	cp := gw.router.Policy("eth_blockNumber")
	if !cp.ReadOnly || !cp.Cacheable || cp.TTL != 500*time.Millisecond {
		t.Fatalf("block number policy wrong: %+v", cp)
	}
	// 未登记的方法不被支持，与 provider 故障区分。
	if gw.router.IsSupported("eth_frobnicate") {
		t.Fatal("unknown method must be unsupported")
	}
}
