package gateway

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

var readOnlyPolicies = map[string]MethodPolicy{
	"eth_blockNumber": {ReadOnly: true},
	"eth_getBalance":  {ReadOnly: true},
}

// 1) 第一个 provider 正常时不发生故障转移。
func TestFailover_FirstHealthyWins(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{resp: okResp(1, `"0x10"`)})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(2, `"0x20"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, readOnlyPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`7`, "eth_blockNumber", ""))
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if string(resp.Result) != `"0x10"` {
		t.Fatalf("want result from p1, got %s", resp.Result)
	}
	if p1.callCount() != 1 || p2.callCount() != 0 {
		t.Fatalf("want only p1 called once, got p1=%d p2=%d", p1.callCount(), p2.callCount())
	}
	if string(resp.ID) != `7` {
		t.Fatalf("response id mismatch: %s", resp.ID)
	}
}

// 2) 传输层错误：自动切换到下一个 provider，只对外返回一次最终结果。
func TestFailover_TransportErrorFailsOver(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{fail: &UpstreamFailure{Kind: FailureTransport, Cause: errors.New("connection refused")}})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"0x20"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, readOnlyPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`"abc"`, "eth_blockNumber", ""))
	if resp.Error != nil {
		t.Fatalf("unexpected error after failover: %v", resp.Error)
	}
	if string(resp.Result) != `"0x20"` {
		t.Fatalf("want result from p2, got %s", resp.Result)
	}
	if p1.callCount() != 1 || p2.callCount() != 1 {
		t.Fatalf("want p1=1 p2=1, got p1=%d p2=%d", p1.callCount(), p2.callCount())
	}
	if string(resp.ID) != `"abc"` {
		t.Fatalf("id must be preserved, got %s", resp.ID)
	}
}

// 3) 超时：标记 p1 失败并切换到 p2。
func TestFailover_TimeoutFailsOver(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{timeout: true})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"0x30"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 60*time.Millisecond, readOnlyPolicies, p1, p2)

	start := time.Now()
	resp := gw.Serve(context.Background(), mustReq(`9`, "eth_blockNumber", ""))
	if time.Since(start) > 5*time.Second {
		t.Fatalf("test hung too long")
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error after timeout failover: %v", resp.Error)
	}
	if string(resp.Result) != `"0x30"` {
		t.Fatalf("want result from p2, got %s", resp.Result)
	}
	if p1.callCount() != 1 || p2.callCount() != 1 {
		t.Fatalf("want p1=1 p2=1, got p1=%d p2=%d", p1.callCount(), p2.callCount())
	}
}

// 4) HTTP 503：可重试类状态码触发切换；随后成功。
func TestFailover_HTTP503FailsOver(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{fail: &UpstreamFailure{Kind: FailureHTTPStatus, Status: http.StatusServiceUnavailable, Cause: errors.New("503")}})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"0x40"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, readOnlyPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`10`, "eth_blockNumber", ""))
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if string(resp.Result) != `"0x40"` {
		t.Fatalf("want p2 result, got %s", resp.Result)
	}
}

// 5) HTTP 400（非可重试状态码）：不切换，直接对外返回 -32001 类传输错误详情。
func TestFailover_HTTP400DoesNotFailOver(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{fail: &UpstreamFailure{Kind: FailureHTTPStatus, Status: http.StatusBadRequest, Cause: errors.New("400")}})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"0x99"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, readOnlyPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`11`, "eth_blockNumber", ""))
	if resp.Error == nil {
		t.Fatal("want error for 400, got success")
	}
	if Code(resp.Error.Code) != CodeNoProvider {
		t.Fatalf("want code -32001 for non-retryable http 400, got %d", resp.Error.Code)
	}
	if p1.callCount() != 1 {
		t.Fatalf("p1 should be called exactly once, got %d", p1.callCount())
	}
	if p2.callCount() != 0 {
		t.Fatalf("p2 must NOT be called for http 400, got %d", p2.callCount())
	}
}

//  6. JSON-RPC error 是确定性应答：不切换 provider，错误码透传为 -32000，
//     data 中保留上游原始错误码。
func TestFailover_RPCErrorNotFailOver(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{fail: &UpstreamFailure{
		Kind: FailureRPCError,
		RPC:  &RPCError{Code: -32010, Message: "execution reverted", Data: []byte(`{"r":"x"}`)},
	}})
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"0x99"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, readOnlyPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`12`, "eth_getBalance", `["0xabc","latest"]`))
	if resp.Error == nil {
		t.Fatal("want rpc error, got success")
	}
	if Code(resp.Error.Code) != CodeUpstreamError {
		t.Fatalf("want -32000, got %d", resp.Error.Code)
	}
	if p2.callCount() != 0 {
		t.Fatalf("p2 must not be called for deterministic rpc error, got %d", p2.callCount())
	}
	// 上游原始错误码必须保留在 data 中。
	if !contains(string(resp.Error.Data), `"upstream_code":-32010`) {
		t.Fatalf("upstream_code missing in data: %s", resp.Error.Data)
	}
}

// 7) 全部 provider 均因可重试错误失败：返回 -32001，且 attempts 可见每个节点。
func TestFailover_AllProvidersFail(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{fail: &UpstreamFailure{Kind: FailureTransport, Cause: errors.New("econnrefused")}})
	p2 := newFakeProvider("p2", fakeBehavior{fail: &UpstreamFailure{Kind: FailureHTTPStatus, Status: http.StatusBadGateway, Cause: errors.New("502")}})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, readOnlyPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`13`, "eth_blockNumber", ""))
	if resp.Error == nil || Code(resp.Error.Code) != CodeNoProvider {
		t.Fatalf("want -32001, got %+v", resp.Error)
	}
	if !contains(string(resp.Error.Data), `"provider":"p1"`) ||
		!contains(string(resp.Error.Data), `"provider":"p2"`) {
		t.Fatalf("attempts missing provider details: %s", resp.Error.Data)
	}
	if p1.callCount() != 1 || p2.callCount() != 1 {
		t.Fatalf("want each provider called once, p1=%d p2=%d", p1.callCount(), p2.callCount())
	}
}

// 8) 全部 provider 超时：返回专门的 -32002，区别于一般全部不可用。
func TestFailover_AllTimeout(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{timeout: true})
	p2 := newFakeProvider("p2", fakeBehavior{timeout: true})
	gw := buildTestGateway(t, NewQuota(0, 0), 40*time.Millisecond, readOnlyPolicies, p1, p2)

	resp := gw.Serve(context.Background(), mustReq(`14`, "eth_blockNumber", ""))
	if resp.Error == nil || Code(resp.Error.Code) != CodeUpstreamTimeout {
		t.Fatalf("want -32002, got %+v", resp.Error)
	}
}

//  9. 健康冷却：p1 连续失败后进入冷却，后续只读请求直接优先选中 p2；
//     冷却到期后 p1 才会重新进入候选。
func TestFailover_HealthCooldown(t *testing.T) {
	p1 := newFakeProvider("p1",
		fakeBehavior{fail: &UpstreamFailure{Kind: FailureTransport, Cause: errors.New("down")}},
		fakeBehavior{resp: okResp(2, `"from-p1-recovered"`)},
	)
	p2 := newFakeProvider("p2", fakeBehavior{resp: okResp(1, `"from-p2"`)})
	gw := buildTestGateway(t, NewQuota(0, 0), 0, readOnlyPolicies, p1, p2)

	// 第一次：p1 失败 -> 切换 p2 成功；p1 进入冷却。
	r1 := gw.Serve(context.Background(), mustReq(`1`, "eth_blockNumber", ""))
	if r1.Error != nil || string(r1.Result) != `"from-p2"` {
		t.Fatalf("first call should fail over to p2, got %+v", r1)
	}
	// 冷却期内：p1 被排到后面，p2 优先，p1 不应被调用。
	r2 := gw.Serve(context.Background(), mustReq(`2`, "eth_blockNumber", ""))
	if r2.Error != nil || string(r2.Result) != `"from-p2"` {
		t.Fatalf("second call should use p2 while p1 cooling, got %+v", r2)
	}
	if p1.callCount() != 1 {
		t.Fatalf("p1 should have cooled and skipped, calls=%d", p1.callCount())
	}
	// 等待冷却到期（helper 中固定 50ms）。
	time.Sleep(90 * time.Millisecond)
	r3 := gw.Serve(context.Background(), mustReq(`3`, "eth_blockNumber", ""))
	if r3.Error != nil || string(r3.Result) != `"from-p1-recovered"` {
		t.Fatalf("after cooldown p1 should be first again, got %+v", r3)
	}
}

func okResp(_ int, result string) *Response {
	// id 由 fakeProvider 按请求 id 回显，这里只预置 result。
	return &Response{JSONRPC: JSONRPCVersion, Result: []byte(result)}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
