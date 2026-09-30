package router

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/gwerrors"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/provider"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/provider/fake"
)

func newRouter(t *testing.T, nonRetry []string, ps ...*fake.Provider) *Router {
	t.Helper()
	all := make([]provider.Provider, 0, len(ps))
	for _, p := range ps {
		all = append(all, p)
	}
	return New(all, time.Second, nonRetry, nil, nil)
}

func TestFailoverTriesNextProviderAndReturnsOnce(t *testing.T) {
	a := fake.New("A", fake.Rule{Behavior: fake.BehaviorRPCError, RPCCode: -32000, RPCMsg: "boom"})
	b := fake.New("B", fake.Rule{Behavior: fake.BehaviorHTTP, Status: 503})
	c := fake.New("C", fake.Rule{Behavior: fake.BehaviorOK, Result: json.RawMessage(`"from-C"`)})
	r := newRouter(t, nil, a, b, c)

	res, trace, err := r.Dispatch(context.Background(), "eth_blockNumber", nil)
	if err != nil {
		t.Fatalf("expected success after failover, got %v", err)
	}
	if string(res) != `"from-C"` {
		t.Fatalf("unexpected result %s", res)
	}
	if a.Calls("eth_blockNumber") != 1 || b.Calls("eth_blockNumber") != 1 || c.Calls("eth_blockNumber") != 1 {
		t.Fatalf("expected A,B,C each called once: %d %d %d",
			a.Calls("eth_blockNumber"), b.Calls("eth_blockNumber"), c.Calls("eth_blockNumber"))
	}
	// trace must explain selection and reasons
	joined := ""
	for _, s := range trace {
		joined += s.Provider + ":" + s.Reason + "\n"
	}
	if !strings.Contains(joined, "failover") || !strings.Contains(joined, "success") {
		t.Fatalf("trace missing failover/success rationale:\n%s", joined)
	}
	t.Logf("TRACE:\n%s", joined)
}

func TestFailoverOnTimeoutAndTransport(t *testing.T) {
	a := fake.New("A", fake.Rule{Behavior: fake.BehaviorTimeout})
	b := fake.New("B", fake.Rule{Behavior: fake.BehaviorTransport})
	c := fake.New("C", fake.Rule{Behavior: fake.BehaviorOK, Result: json.RawMessage(`"C"`)})
	r := newRouter(t, nil, a, b, c)

	start := time.Now()
	_, _, err := r.Dispatch(context.Background(), "m", nil)
	if err != nil {
		t.Fatal(err)
	}
	// timeout provider is bounded by the router timeout, so total must be small
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("failover took too long: %v", d)
	}
}

func TestNonIdempotentMethodSentExactlyOnce(t *testing.T) {
	a := fake.New("A", fake.Rule{Behavior: fake.BehaviorRPCError, RPCCode: -32000, RPCMsg: "nonce too low"})
	b := fake.New("B", fake.Rule{Behavior: fake.BehaviorOK, Result: json.RawMessage(`"would-have-double-sent"`)})
	c := fake.New("C", fake.Rule{Behavior: fake.BehaviorOK})
	r := newRouter(t, []string{"eth_sendRawTransaction"}, a, b, c)

	_, trace, err := r.Dispatch(context.Background(), "eth_sendRawTransaction", json.RawMessage(`["0xRAW"]`))
	if err == nil {
		t.Fatal("non-idempotent failure must be returned as an error, not swallowed")
	}
	gw := gwerrors.Normalize(err)
	if gw.Code != gwerrors.CodeRetryNotAllowed {
		t.Fatalf("expected retry-not-allowed code, got %d (%v)", gw.Code, err)
	}
	if a.TotalCalls() != 1 {
		t.Fatalf("send method must be attempted exactly once on A, got %d", a.TotalCalls())
	}
	if b.TotalCalls() != 0 || c.TotalCalls() != 0 {
		t.Fatalf("send method must never fail over: B=%d C=%d", b.TotalCalls(), c.TotalCalls())
	}
	joined := ""
	for _, s := range trace {
		joined += s.Provider + ":" + s.Reason + "\n"
	}
	if !strings.Contains(joined, "at most once") {
		t.Fatalf("trace should explain non-retry, got:\n%s", joined)
	}
	t.Logf("NON-IDEMPOTENT TRACE:\n%s", joined)
}

func TestNonIdempotentSuccessNotRetried(t *testing.T) {
	a := fake.New("A", fake.Rule{Behavior: fake.BehaviorOK, Result: json.RawMessage(`"txhash"`)})
	b := fake.New("B", fake.Rule{Behavior: fake.BehaviorOK})
	r := newRouter(t, []string{"eth_sendRawTransaction"}, a, b)
	res, _, err := r.Dispatch(context.Background(), "eth_sendRawTransaction", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != `"txhash"` || a.TotalCalls() != 1 || b.TotalCalls() != 0 {
		t.Fatalf("single successful send: res=%s A=%d B=%d", res, a.TotalCalls(), b.TotalCalls())
	}
}

func TestUnavailableProvidersSkipped(t *testing.T) {
	a := fake.New("A", fake.Rule{Behavior: fake.BehaviorOK})
	b := fake.New("B", fake.Rule{Behavior: fake.BehaviorOK})
	a.SetAvailable(false)
	b.SetAvailable(false)
	r := newRouter(t, nil, a, b)
	_, _, err := r.Dispatch(context.Background(), "m", nil)
	gw := gwerrors.Normalize(err)
	if gw.Code != gwerrors.CodeAllProvidersDown {
		t.Fatalf("expected all-providers-down, got %d", gw.Code)
	}
	if a.TotalCalls() != 0 || b.TotalCalls() != 0 {
		t.Fatal("unavailable providers must not be called")
	}
}

func TestAllProvidersFail(t *testing.T) {
	a := fake.New("A", fake.Rule{Behavior: fake.BehaviorTransport})
	b := fake.New("B", fake.Rule{Behavior: fake.BehaviorRPCError})
	r := newRouter(t, nil, a, b)
	_, _, err := r.Dispatch(context.Background(), "m", nil)
	gw := gwerrors.Normalize(err)
	if gw.Code != gwerrors.CodeAllProvidersDown {
		t.Fatalf("expected all-providers-down, got %d", gw.Code)
	}
}

func TestServesMethod(t *testing.T) {
	a := fake.New("A", fake.Rule{Behavior: fake.BehaviorOK})
	all := []provider.Provider{a}
	r := New(all, time.Second, nil, nil, map[string][]string{"onlyX": {"A"}})
	if !r.ServesMethod("onlyX") {
		t.Fatal("provider A should serve onlyX")
	}
	if r.ServesMethod("other") {
		t.Fatal("other is not served by any configured provider")
	}
}
