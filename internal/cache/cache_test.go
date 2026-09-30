package cache

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCacheSetGetAndExpiry(t *testing.T) {
	c := New()
	key := Key("eth_blockNumber", json.RawMessage(`[]`))
	if _, ok := c.Get(key); ok {
		t.Fatal("expected miss on empty cache")
	}
	c.Set(key, json.RawMessage(`"0x10"`), 50*time.Millisecond)
	v, ok := c.Get(key)
	if !ok || string(v) != `"0x10"` {
		t.Fatalf("expected hit, got %v %v", v, ok)
	}
	time.Sleep(70 * time.Millisecond)
	if _, ok := c.Get(key); ok {
		t.Fatal("expected miss after TTL expiry")
	}
}

func TestCacheKeyStableAcrossParamWhitespace(t *testing.T) {
	k1 := Key("eth_getBalance", json.RawMessage(`["0xabc", "latest"]`))
	k2 := Key("eth_getBalance", json.RawMessage(`[ "0xabc","latest" ]`))
	if k1 != k2 {
		t.Fatal("semantically equal params should share a key")
	}
	k3 := Key("eth_getCode", json.RawMessage(`["0xabc","latest"]`))
	if k1 == k3 {
		t.Fatal("different methods must have different keys")
	}
}

func TestDeleteExpired(t *testing.T) {
	c := New()
	c.Set("a", json.RawMessage(`1`), 10*time.Millisecond)
	c.Set("b", json.RawMessage(`2`), time.Hour)
	time.Sleep(20 * time.Millisecond)
	if n := c.DeleteExpired(time.Now()); n != 1 {
		t.Fatalf("expected 1 expired, got %d", n)
	}
	if c.Len() != 1 {
		t.Fatalf("expected 1 remaining, got %d", c.Len())
	}
}
