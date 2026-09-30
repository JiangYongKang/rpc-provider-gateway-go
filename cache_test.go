package gateway

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

var cachePolicies = map[string]MethodPolicy{
	"eth_blockNumber": {ReadOnly: true, Cacheable: true, TTL: 100 * time.Millisecond},
}

// 1) 网关层面：同一方法同参数第二次命中缓存，provider 只被调用一次。
func TestCache_HitAvoidsSecondProviderCall(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{resp: okResp(1, `"0xabc"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, cachePolicies, p1)

	r1 := gw.Serve(context.Background(), mustReq(`1`, "eth_blockNumber", ""))
	r2 := gw.Serve(context.Background(), mustReq(`2`, "eth_blockNumber", ""))
	if r1.Error != nil || r2.Error != nil {
		t.Fatalf("unexpected errors: %v %v", r1.Error, r2.Error)
	}
	if string(r1.Result) != `"0xabc"` || string(r2.Result) != `"0xabc"` {
		t.Fatalf("both results should be cached value, got %s %s", r1.Result, r2.Result)
	}
	if p1.callCount() != 1 {
		t.Fatalf("provider should be called exactly once, got %d", p1.callCount())
	}
	// 即使命中缓存，响应 id 仍必须是各请求自己的 id，不能串。
	if string(r1.ID) != `1` || string(r2.ID) != `2` {
		t.Fatalf("cached hit must preserve per-request id: %s %s", r1.ID, r2.ID)
	}
}

// 2) 不同参数不共享缓存。
func TestCache_DifferentParamsNotShared(t *testing.T) {
	p1 := &paramEchoProvider{name: "echo"}
	policies := map[string]MethodPolicy{
		"eth_getBalance": {ReadOnly: true, Cacheable: true, TTL: time.Minute},
	}
	gw := buildTestGatewayWithPolicies(t, NewQuota(0, 0), 0, policies, p1)

	a := gw.Serve(context.Background(), mustReq(`1`, "eth_getBalance", `["0xAAA","latest"]`))
	b := gw.Serve(context.Background(), mustReq(`2`, "eth_getBalance", `["0xBBB","latest"]`))
	a2 := gw.Serve(context.Background(), mustReq(`3`, "eth_getBalance", `["0xAAA","latest"]`))

	if !contains(string(a.Result), "0xAAA") || !contains(string(b.Result), "0xBBB") {
		t.Fatalf("params crossed: a=%s b=%s", a.Result, b.Result)
	}
	if !contains(string(a2.Result), "0xAAA") {
		t.Fatalf("a2 should be cached AAA result, got %s", a2.Result)
	}
	if p1.calls != 2 {
		t.Fatalf("want 2 upstream calls (AAA, BBB) + 1 cache hit, upstream calls=%d", p1.calls)
	}
}

// 3) TTL 过期后缓存失效，再次回源。
func TestCache_TTLExpiry(t *testing.T) {
	p1 := newFakeProvider("p1",
		fakeBehavior{resp: okResp(1, `"v1"`)},
		fakeBehavior{resp: okResp(2, `"v2"`)},
	)
	gw := buildTestGateway(t, NewQuota(0, 0), 0, cachePolicies, p1)

	r1 := gw.Serve(context.Background(), mustReq(`1`, "eth_blockNumber", ""))
	if string(r1.Result) != `"v1"` {
		t.Fatalf("want v1, got %s", r1.Result)
	}
	// 未过期：仍是 v1，不回源。
	r2 := gw.Serve(context.Background(), mustReq(`2`, "eth_blockNumber", ""))
	if string(r2.Result) != `"v1"` {
		t.Fatalf("within TTL must be cached v1, got %s", r2.Result)
	}
	time.Sleep(150 * time.Millisecond)
	// 过期：回源拿到 v2。
	r3 := gw.Serve(context.Background(), mustReq(`3`, "eth_blockNumber", ""))
	if string(r3.Result) != `"v2"` {
		t.Fatalf("after TTL expiry want fresh v2, got %s", r3.Result)
	}
	if p1.callCount() != 2 {
		t.Fatalf("want exactly 2 upstream calls after expiry, got %d", p1.callCount())
	}
}

// 4) 上游错误绝不写缓存：失败后重试会再次回源。
func TestCache_ErrorsNotCached(t *testing.T) {
	p1 := newFakeProvider("p1",
		fakeBehavior{fail: &UpstreamFailure{Kind: FailureTransport, Cause: errSimple("down")}},
		fakeBehavior{resp: okResp(2, `"recovered"`)},
	)
	gw := buildTestGateway(t, NewQuota(0, 0), 0, cachePolicies, p1)

	r1 := gw.Serve(context.Background(), mustReq(`1`, "eth_blockNumber", ""))
	if r1.Error == nil || Code(r1.Error.Code) != CodeNoProvider {
		t.Fatalf("first call should fail -32001, got %+v", r1.Error)
	}
	r2 := gw.Serve(context.Background(), mustReq(`2`, "eth_blockNumber", ""))
	if r2.Error != nil || string(r2.Result) != `"recovered"` {
		t.Fatalf("error must not be cached; second call should reach provider, got %+v", r2)
	}
	if p1.callCount() != 2 {
		t.Fatalf("want 2 upstream calls (failure not cached), got %d", p1.callCount())
	}
}

// 5) 缓存键规范化：对象参数的键顺序不同视为同一参数，命中同一缓存。
func TestCache_KeyCanonicalization(t *testing.T) {
	c := NewResultCache()
	params1 := json.RawMessage(`{"a":1,"b":2}`)
	params2 := json.RawMessage(`{"b":2,"a":1}`)
	c.Set("m", params1, json.RawMessage(`"same"`), time.Minute)
	v1, ok1 := c.Get("m", params1)
	v2, ok2 := c.Get("m", params2)
	if !ok1 || !ok2 || string(v1) != `"same"` || string(v2) != `"same"` {
		t.Fatalf("reordered object keys must share cache: %s(%t) %s(%t)", v1, ok1, v2, ok2)
	}
	// 不同方法不共享。
	if _, ok := c.Get("other", params1); ok {
		t.Fatal("different methods must not share cache")
	}
}

// 6) ResultCache 单元：可控时钟直接验证过期语义。
func TestResultCache_ControlledClock(t *testing.T) {
	c := NewResultCache()
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }

	c.Set("m", nil, json.RawMessage(`1`), 10*time.Second)
	if _, ok := c.Get("m", nil); !ok {
		t.Fatal("should hit before expiry")
	}
	now = now.Add(10*time.Second + time.Nanosecond)
	if _, ok := c.Get("m", nil); ok {
		t.Fatal("should miss after expiry")
	}
	if c.Len() != 0 {
		t.Fatalf("expired entry should be evicted on Get, len=%d", c.Len())
	}
}

// paramEchoProvider 把首个参数原样放进结果，用于验证参数隔离。
type paramEchoProvider struct {
	name  string
	mu    chan struct{}
	calls int
}

func (p *paramEchoProvider) Name() string { return p.name }

func (p *paramEchoProvider) Call(_ context.Context, req *Request) (*Response, error) {
	if p.mu == nil {
		p.mu = make(chan struct{}, 1)
	}
	p.mu <- struct{}{}
	p.calls++
	<-p.mu
	var params []string
	_ = json.Unmarshal(req.Params, &params)
	who := ""
	if len(params) > 0 {
		who = params[0]
	}
	return &Response{
		JSONRPC: JSONRPCVersion,
		ID:      req.ID,
		Result:  json.RawMessage(`{"addr":"` + who + `"}`),
	}, nil
}

type errString string

func (e errString) Error() string { return string(e) }

func errSimple(s string) error { return errString(s) }
