package config

import "testing"

func TestLoadExampleConfig(t *testing.T) {
	cfg, err := Load("../../configs/config.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DefaultTimeout.Seconds() != 5 {
		t.Fatalf("default timeout = %v", cfg.DefaultTimeout)
	}
	if cfg.Cache.TTL.Seconds() != 2 {
		t.Fatalf("ttl = %v", cfg.Cache.TTL)
	}
	if len(cfg.Providers) != 3 {
		t.Fatalf("providers = %d", len(cfg.Providers))
	}
	if !cfg.Providers[0].ServesMethod("anything") {
		t.Fatal("provider with empty methods should serve all")
	}
	if cfg.Providers[2].ServesMethod("eth_getBalance") != true ||
		cfg.Providers[2].ServesMethod("eth_sendRawTransaction") != false {
		t.Fatal("method filter mismatch")
	}
}

func TestValidateRejects(t *testing.T) {
	if _, err := Load("../../configs/does-not-exist.json"); err == nil {
		t.Fatal("expected error for missing file")
	}
	c := Default()
	c.Providers = nil
	if err := c.Validate(); err == nil {
		t.Fatal("expected error with no providers")
	}
}
