package gateway

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

var txPolicies = map[string]MethodPolicy{
	"eth_sendRawTransaction": {ReadOnly: false}, // 非幂等：不可缓存、不可重试
}

// 1) 非幂等方法成功：只发送一次，返回结果。
func TestNonIdempotent_SuccessSentOnce(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{resp: okResp(1, `"0xtxhash"`)})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"should-not-be-used"`)})
	gw := buildTestGatewayWithPolicies(t, NewQuota(0, 0), 0, txPolicies,
		countCalls(p1, new(atomic.Int32)), countCalls(p2, new(atomic.Int32)))

	resp := gw.Serve(context.Background(), mustReq(`100`, "eth_sendRawTransaction", `["0xraw"]`))
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if string(resp.Result) != `"0xtxhash"` {
		t.Fatalf("unexpected result %s", resp.Result)
	}
	if p1.callCount() != 1 || p2.callCount() != 0 {
		t.Fatalf("non-idempotent success must hit only p1 once: p1=%d p2=%d", p1.callCount(), p2.callCount())
	}
}

// 2) 非幂等方法遇到传输错误：绝不切换 provider，只发送一次，回传 -32003。
func TestNonIdempotent_TransportErrorNotRetried(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{fail: &UpstreamFailure{Kind: FailureTransport, Cause: errors.New("reset")}})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"danger-duplicate"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, txPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`101`, "eth_sendRawTransaction", `["0xraw"]`))
	if resp.Error == nil {
		t.Fatal("must fail, got success — duplicate send risk")
	}
	if Code(resp.Error.Code) != CodeNonIdempotentFail {
		t.Fatalf("want -32003, got %d (%s)", resp.Error.Code, resp.Error.Message)
	}
	if p1.callCount() != 1 {
		t.Fatalf("p1 must be called exactly once, got %d", p1.callCount())
	}
	if p2.callCount() != 0 {
		t.Fatalf("p2 must NEVER be called for a failed non-idempotent method, got %d", p2.callCount())
	}
	if !contains(string(resp.Error.Data), `"provider":"p1"`) ||
		!contains(string(resp.Error.Data), `"kind":"transport"`) {
		t.Fatalf("error data must show provider and real kind: %s", resp.Error.Data)
	}
	if string(resp.ID) != `101` {
		t.Fatalf("id mismatch: %s", resp.ID)
	}
}

// 3) 非幂等方法遇到超时：同样只发一次，-32003 中保留 timeout 类别。
func TestNonIdempotent_TimeoutNotRetried(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{timeout: true})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"danger"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 50*time.Millisecond, txPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`102`, "eth_sendRawTransaction", `["0xraw"]`))
	if resp.Error == nil || Code(resp.Error.Code) != CodeNonIdempotentFail {
		t.Fatalf("want -32003, got %+v", resp.Error)
	}
	if !contains(string(resp.Error.Data), `"kind":"timeout"`) {
		t.Fatalf("want timeout kind in data, got %s", resp.Error.Data)
	}
	if p1.callCount() != 1 || p2.callCount() != 0 {
		t.Fatalf("timeout must not trigger failover: p1=%d p2=%d", p1.callCount(), p2.callCount())
	}
}

//  4. 非幂等方法遇到上游 JSON-RPC error：只发一次，-32003 中保留上游错误码，
//     绝不切换到第二个节点重发。
func TestNonIdempotent_RPCErrorNotRetried(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{fail: &UpstreamFailure{
		Kind: FailureRPCError, RPC: &RPCError{Code: -32000, Message: "nonce too low"},
	}})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"danger"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, txPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`103`, "eth_sendRawTransaction", `["0xraw"]`))
	if resp.Error == nil {
		t.Fatal("must return error")
	}
	if Code(resp.Error.Code) != CodeNonIdempotentFail {
		t.Fatalf("want -32003 for single-attempt failure, got %d", resp.Error.Code)
	}
	if !contains(string(resp.Error.Data), `"upstream_code":-32000`) {
		t.Fatalf("upstream code must be preserved: %s", resp.Error.Data)
	}
	if p1.callCount() != 1 || p2.callCount() != 0 {
		t.Fatalf("no failover allowed: p1=%d p2=%d", p1.callCount(), p2.callCount())
	}
}

// 5) 非幂等方法遇到 HTTP 503（即使可重试）：也绝不切换，只发一次。
func TestNonIdempotent_HTTP503NotRetried(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{fail: &UpstreamFailure{Kind: FailureHTTPStatus, Status: http.StatusServiceUnavailable}})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"danger"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, txPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`104`, "eth_sendRawTransaction", `["0xraw"]`))
	if resp.Error == nil || Code(resp.Error.Code) != CodeNonIdempotentFail {
		t.Fatalf("want -32003, got %+v", resp.Error)
	}
	if p1.callCount() != 1 || p2.callCount() != 0 {
		t.Fatalf("http 503 must not cause failover for non-idempotent: p1=%d p2=%d", p1.callCount(), p2.callCount())
	}
}

//  6. 首选节点 p1 处于冷却、p2 健康时：非幂等请求只投递给健康的 p2 一次，
//     绝不“试探”冷却中的 p1（避免不确定的重复投递）。
func TestNonIdempotent_SkipsCoolingUsesHealthyOnce(t *testing.T) {
	p1 := newFakeProvider("p1")
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"0xok"`)})
	gw := buildTestGatewayWithPolicies(t, NewQuota(0, 0), 0, mergePolicies(readOnlyPolicies, txPolicies), p1, p2)
	// 用只读失败把 p1 打进冷却（p2 提供成功响应）。
	p1.script = []fakeBehavior{{fail: &UpstreamFailure{Kind: FailureTransport, Cause: errors.New("down")}}}
	warm := gw.Serve(context.Background(), mustReq(`1`, "eth_blockNumber", ""))
	if warm.Error != nil {
		t.Fatalf("warmup: %v", warm.Error)
	}
	if p1.callCount() != 1 {
		t.Fatalf("warmup should have hit p1 once, got %d", p1.callCount())
	}
	// 发交易：必须只发给健康的 p2，p1 不被试探。
	resp := gw.Serve(context.Background(), mustReq(`105`, "eth_sendRawTransaction", `["0xraw"]`))
	if resp.Error != nil {
		t.Fatalf("send to healthy p2 should succeed once: %v", resp.Error)
	}
	if string(resp.Result) != `"0xok"` {
		t.Fatalf("unexpected result %s", resp.Result)
	}
	if p1.callCount() != 1 {
		t.Fatalf("cooling p1 must not be probed, p1 calls=%d", p1.callCount())
	}
	if p2.callCount() != 2 { // warmup 1 + send 1
		t.Fatalf("p2 should handle warmup + one send, got %d", p2.callCount())
	}
}

// 7) 所有节点都冷却：非幂等方法必须明确拒绝，不做任何投递。
func TestNonIdempotent_AllCoolingRefusedWithoutSend(t *testing.T) {
	p1 := newFakeProvider("p1")
	p2 := newFakeProvider("p2")
	gw := buildTestGatewayWithPolicies(t, NewQuota(0, 0), 0, mergePolicies(readOnlyPolicies, txPolicies), p1, p2)
	// 手动让两个节点进入冷却。
	for _, tp := range gw.pool.providers {
		gw.pool.MarkFailure(tp)
	}
	resp := gw.Serve(context.Background(), mustReq(`106`, "eth_sendRawTransaction", `["0xraw"]`))
	if resp.Error == nil || Code(resp.Error.Code) != CodeNonIdempotentFail {
		t.Fatalf("want -32003 refusal when all cooling, got %+v", resp.Error)
	}
	if p1.callCount() != 0 || p2.callCount() != 0 {
		t.Fatalf("refusal must not send anything: p1=%d p2=%d", p1.callCount(), p2.callCount())
	}
	if !contains(resp.Error.Message, "not attempted") {
		t.Fatalf("message should state not attempted: %q", resp.Error.Message)
	}
}

// ---- 辅助 ----

// countingProvider 包装 fake 并额外用原子计数记录调用次数。
type countingProvider struct {
	inner *fakeProvider
	n     *atomic.Int32
}

func (c countingProvider) Name() string { return c.inner.Name() }

func (c countingProvider) Call(ctx context.Context, req *Request) (*Response, error) {
	c.n.Add(1)
	return c.inner.Call(ctx, req)
}

func countCalls(fp *fakeProvider, n *atomic.Int32) Provider { return countingProvider{inner: fp, n: n} }

func mergePolicies(a, b map[string]MethodPolicy) map[string]MethodPolicy {
	m := make(map[string]MethodPolicy, len(a)+len(b))
	for k, v := range a {
		m[k] = v
	}
	for k, v := range b {
		m[k] = v
	}
	return m
}

// buildTestGatewayWithPolicies 与 buildTestGateway 相同但允许自定义策略集。
func buildTestGatewayWithPolicies(t *testing.T, quota *Quota, timeout time.Duration, policies map[string]MethodPolicy, fakes ...Provider) *Gateway {
	t.Helper()
	pool, err := NewProviderPool(nil)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	for _, fp := range fakes {
		pool.addTestProvider(fp, ProviderConfig{Name: fp.Name(), URL: "fake://" + fp.Name(), HealthCooldown: 50 * time.Millisecond})
	}
	if timeout == 0 {
		timeout = 2 * time.Second
	}
	return New(Options{
		Router:  NewMethodRouter(policies),
		Pool:    pool,
		Cache:   NewResultCache(),
		Quota:   quota,
		Timeout: timeout,
		Log:     testLog{t},
	})
}
