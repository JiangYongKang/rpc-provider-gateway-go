package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/cache"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/config"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/gwerrors"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/provider"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/provider/fake"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/ratelimit"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/router"
)

type testLogger struct {
	sb strings.Builder
	mu sync.Mutex
}

func (l *testLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sb.WriteString(strings.TrimSpace(fmt.Sprintf(format, args...)) + "\n")
}
func (l *testLogger) Logs() string { l.mu.Lock(); defer l.mu.Unlock(); return l.sb.String() }

type harness struct {
	gw   *Gateway
	log  *testLogger
	a, b *fake.Provider
	srv  *httptest.Server
}

func baseCfg() *config.Config {
	c := config.Default()
	c.MaxConcurrent = 1000
	c.RequestsPerSecond = 0
	c.Cache.Enabled = true
	c.Cache.TTL = config.Duration{Duration: 60 * time.Millisecond}
	c.Cache.Methods = []string{"eth_blockNumber"}
	c.IdempotentMethods = []string{"eth_gasPrice", "eth_blockNumber"}
	c.NonRetryableMethods = []string{"eth_sendRawTransaction"}
	return c
}

func setup(t *testing.T, cfg *config.Config) *harness {
	t.Helper()
	a := fake.New("A", fake.Rule{Behavior: fake.BehaviorRPCError, RPCCode: -32000, RPCMsg: "A down"})
	b := fake.New("B", fake.Rule{Behavior: fake.BehaviorOK, Result: json.RawMessage(`"B-ok"`)})
	ps := []provider.Provider{a, b}
	rtr := router.New(ps, 500*time.Millisecond, cfg.NonRetryableMethods, SupportedMethods(cfg), nil)
	c := cache.New()
	l := ratelimit.New(cfg.MaxConcurrent, cfg.RequestsPerSecond)
	lg := &testLogger{}
	g := New(cfg, rtr, c, l, lg)
	srv := httptest.NewServer(g.Handler())
	t.Cleanup(srv.Close)
	return &harness{gw: g, log: lg, a: a, b: b, srv: srv}
}

func post(t *testing.T, url, body string) []byte {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	return buf.Bytes()
}

type rpcResp struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code int             `json:"code"`
		Msg  string          `json:"message"`
		Data json.RawMessage `json:"data"`
	} `json:"error"`
}

func TestFailoverEndToEnd(t *testing.T) {
	h := setup(t, baseCfg())
	body := post(t, h.srv.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice","params":[]}`)
	var resp rpcResp
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %s", resp.Error.Msg)
	}
	if string(resp.Result) != `"B-ok"` {
		t.Fatalf("failover result = %s", resp.Result)
	}
	if h.a.Calls("eth_gasPrice") != 1 || h.b.Calls("eth_gasPrice") != 1 {
		t.Fatalf("expected A then B once each, got A=%d B=%d",
			h.a.Calls("eth_gasPrice"), h.b.Calls("eth_gasPrice"))
	}
	logs := h.log.Logs()
	t.Logf("DECISION LOG:\n%s", logs)
	if !strings.Contains(logs, "id=1") {
		t.Fatalf("log should print request id:\n%s", logs)
	}
	if !strings.Contains(logs, "provider=A") || !strings.Contains(logs, "provider=B") {
		t.Fatalf("log should name selected providers:\n%s", logs)
	}
	if !strings.Contains(logs, "failover") {
		t.Fatalf("log should state the failover basis:\n%s", logs)
	}
}

func TestAllProvidersDownDistinct(t *testing.T) {
	h := setup(t, baseCfg())
	h.b.SetFallback(fake.Rule{Behavior: fake.BehaviorTransport})
	body := post(t, h.srv.URL, `{"jsonrpc":"2.0","id":2,"method":"eth_gasPrice"}`)
	var resp rpcResp
	_ = json.Unmarshal(body, &resp)
	if resp.Error == nil || resp.Error.Code != int(gwerrors.CodeAllProvidersDown) {
		t.Fatalf("expected all-providers-down (-32002), got %v", resp.Error)
	}
}

func TestMethodNotSupportedDistinct(t *testing.T) {
	h := setup(t, baseCfg())
	body := post(t, h.srv.URL, `{"jsonrpc":"2.0","id":3,"method":"definitely_not_a_method"}`)
	var resp rpcResp
	_ = json.Unmarshal(body, &resp)
	if resp.Error == nil || resp.Error.Code != int(gwerrors.CodeMethodNotFound) {
		t.Fatalf("expected method-not-found (-32601), got %v", resp.Error)
	}
}

func TestRateLimitedDistinct(t *testing.T) {
	cfg := baseCfg()
	cfg.MaxConcurrent = 2 // only 2 in flight; a burst of many must reject some
	h := setup(t, cfg)
	// Both providers block on timeout so acquired concurrency slots stay
	// occupied long enough for the burst to overflow the 2-slot cap.
	blocking := fake.Rule{Behavior: fake.BehaviorTimeout}
	h.a.SetFallback(blocking)
	h.b.SetFallback(blocking)

	const n = 30
	var wg sync.WaitGroup
	codes := make([]int, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			body := post(t, h.srv.URL, `{"jsonrpc":"2.0","id":`+strconv.Itoa(i+1)+`,"method":"eth_gasPrice"}`)
			var resp rpcResp
			_ = json.Unmarshal(body, &resp)
			if resp.Error != nil {
				codes[i] = resp.Error.Code
			}
		}(i)
	}
	wg.Wait()

	rejected := 0
	rateLimited := 0
	for _, code := range codes {
		if code != 0 {
			rejected++
		}
		if code == int(gwerrors.CodeRateLimited) {
			rateLimited++
		}
	}
	if rateLimited == 0 {
		t.Fatalf("expected at least one rate-limited (-32001), codes=%v", codes)
	}
	t.Logf("rate-limited %d/%d requests", rateLimited, n)
}

func TestCacheHitAndExpiry(t *testing.T) {
	h := setup(t, baseCfg())
	call := `{"jsonrpc":"2.0","id":10,"method":"eth_blockNumber","params":[]}`
	b1 := post(t, h.srv.URL, call)
	b2 := post(t, h.srv.URL, call)
	var r1, r2 rpcResp
	_ = json.Unmarshal(b1, &r1)
	_ = json.Unmarshal(b2, &r2)
	if r1.Error != nil || r2.Error != nil {
		t.Fatalf("unexpected errors: %v %v", r1.Error, r2.Error)
	}
	if h.b.Calls("eth_blockNumber") != 1 {
		t.Fatalf("second call should be served from cache, B calls=%d", h.b.Calls("eth_blockNumber"))
	}
	if !strings.Contains(h.log.Logs(), "cache HIT") {
		t.Fatalf("expected cache HIT log, got:\n%s", h.log.Logs())
	}
	time.Sleep(80 * time.Millisecond)
	b3 := post(t, h.srv.URL, call)
	var r3 rpcResp
	_ = json.Unmarshal(b3, &r3)
	if h.b.Calls("eth_blockNumber") != 2 {
		t.Fatalf("after expiry expect a fresh upstream call, B calls=%d", h.b.Calls("eth_blockNumber"))
	}
}

func TestNonIdempotentSentOnceEndToEnd(t *testing.T) {
	h := setup(t, baseCfg())
	h.b.SetRule("eth_sendRawTransaction", fake.Rule{Behavior: fake.BehaviorOK, Result: json.RawMessage(`"B-hash"`)})
	body := post(t, h.srv.URL, `{"jsonrpc":"2.0","id":20,"method":"eth_sendRawTransaction","params":["0xRAW"]}`)
	var resp rpcResp
	_ = json.Unmarshal(body, &resp)
	if resp.Error == nil || resp.Error.Code != int(gwerrors.CodeRetryNotAllowed) {
		t.Fatalf("expected retry-not-allowed (-32008), got %v", resp.Error)
	}
	if h.a.Calls("eth_sendRawTransaction") != 1 {
		t.Fatalf("send attempted on A = %d, want 1", h.a.Calls("eth_sendRawTransaction"))
	}
	if h.b.Calls("eth_sendRawTransaction") != 0 {
		t.Fatalf("send must never fail over to B, got %d", h.b.Calls("eth_sendRawTransaction"))
	}
}

func TestNonIdempotentSuccess(t *testing.T) {
	h := setup(t, baseCfg())
	h.a.SetRule("eth_sendRawTransaction", fake.Rule{Behavior: fake.BehaviorOK, Result: json.RawMessage(`"A-hash"`)})
	body := post(t, h.srv.URL, `{"jsonrpc":"2.0","id":21,"method":"eth_sendRawTransaction","params":["0xRAW"]}`)
	var resp rpcResp
	_ = json.Unmarshal(body, &resp)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if string(resp.Result) != `"A-hash"` {
		t.Fatalf("result = %s", resp.Result)
	}
	if h.a.Calls("eth_sendRawTransaction") != 1 || h.b.Calls("eth_sendRawTransaction") != 0 {
		t.Fatalf("send must hit only A once: A=%d B=%d", h.a.Calls("eth_sendRawTransaction"), h.b.Calls("eth_sendRawTransaction"))
	}
}

func TestConcurrentIDsDoNotCross(t *testing.T) {
	h := setup(t, baseCfg())
	const n = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "id-" + strconv.Itoa(i)
			body := `{"jsonrpc":"2.0","id":"` + id + `","method":"eth_gasPrice"}`
			b := post(t, h.srv.URL, body)
			var resp rpcResp
			if err := json.Unmarshal(b, &resp); err != nil {
				mu.Lock()
				failures = append(failures, id+": bad json")
				mu.Unlock()
				return
			}
			if string(resp.ID) != `"`+id+`"` {
				mu.Lock()
				failures = append(failures, id+" got id "+string(resp.ID))
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	for _, f := range failures {
		t.Error(f)
	}
}

func TestBatchMatchesIDs(t *testing.T) {
	h := setup(t, baseCfg())
	body := `[{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice"},
	          {"jsonrpc":"2.0","id":2,"method":"definitely_not_a_method"},
	          {"jsonrpc":"2.0","method":"notify-only"}]`
	b := post(t, h.srv.URL, body)
	var resps []rpcResp
	if err := json.Unmarshal(b, &resps); err != nil {
		t.Fatal(err)
	}
	if len(resps) != 2 {
		t.Fatalf("notification must be omitted, got %d responses", len(resps))
	}
	if string(resps[0].ID) != "1" || resps[0].Error != nil {
		t.Fatalf("first response mismatch: %+v", resps[0])
	}
	if string(resps[1].ID) != "2" || resps[1].Error == nil ||
		resps[1].Error.Code != int(gwerrors.CodeMethodNotFound) {
		t.Fatalf("second response mismatch: %+v", resps[1])
	}
}
