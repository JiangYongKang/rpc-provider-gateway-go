package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// startE2E 用两个真实 httptest 上游启动一个监听随机端口的网关。
// down1 为 true 时第一个上游返回 503。
func startE2E(t *testing.T, down1 *atomic.Bool, p1Calls, p2Calls *atomic.Int32) string {
	t.Helper()

	newUpstream := func(name string, down *atomic.Bool, calls *atomic.Int32, result string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			body, _ := io.ReadAll(r.Body)
			var req Request
			_ = json.Unmarshal(body, &req)
			t.Logf("upstream=%s received req_id=%s method=%s", name, requestIDString(req.ID), req.Method)
			if name == "p1" && down != nil && down.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`upstream busy`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":` + result + `}`))
		}))
	}

	up1 := newUpstream("p1", down1, p1Calls, `"from-p1"`)
	up2 := newUpstream("p2", nil, p2Calls, `"from-p2"`)
	t.Cleanup(up1.Close)
	t.Cleanup(up2.Close)

	cfg := &Config{
		ListenAddr:     "127.0.0.1:0",
		Path:           "/rpc",
		DefaultTimeout: Duration(2 * time.Second),
		MaxConcurrent:  0,
		QPS:            0,
		Providers: []ProviderConf{
			{Name: "p1", URL: up1.URL, HealthCooldown: Duration(200 * time.Millisecond)},
			{Name: "p2", URL: up2.URL, HealthCooldown: Duration(200 * time.Millisecond)},
		},
		Methods: map[string]MethodConf{
			"eth_blockNumber":        {ReadOnly: true},
			"eth_sendRawTransaction": {},
		},
	}
	gw, err := cfg.Build(testLog{t})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	srv := NewHTTPServer(gw, "127.0.0.1:0", "/rpc")
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return "http://" + srv.Addr() + "/rpc"
}

// 1) 正常 HTTP 单请求。
func TestE2E_HappyPath(t *testing.T) {
	var down atomic.Bool
	var c1, c2 atomic.Int32
	url := startE2E(t, &down, &c1, &c2)

	resp := postJSON(t, url, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("http status %d", resp.StatusCode)
	}
	var r Response
	decode(t, resp, &r)
	if r.Error != nil || string(r.Result) != `"from-p1"` {
		t.Fatalf("want p1 result, got %+v", r)
	}
	if c1.Load() != 1 || c2.Load() != 0 {
		t.Fatalf("want only p1 hit, c1=%d c2=%d", c1.Load(), c2.Load())
	}
}

// 2) p1 返回 503：真实 HTTP 链路上故障转移到 p2，对外只返回一次最终结果。
func TestE2E_HTTPFailover(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	var c1, c2 atomic.Int32
	url := startE2E(t, &down, &c1, &c2)

	resp := postJSON(t, url, `{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}`)
	var r Response
	decode(t, resp, &r)
	if r.Error != nil || string(r.Result) != `"from-p2"` {
		t.Fatalf("want failover to p2, got %+v", r)
	}
	if c1.Load() != 1 || c2.Load() != 1 {
		t.Fatalf("want p1=1 p2=1, got %d %d", c1.Load(), c2.Load())
	}
}

// 3) 非幂等方法：p1 503 时不切换，只发一次并回 -32003。
func TestE2E_NonIdempotentNoFailover(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	var c1, c2 atomic.Int32
	url := startE2E(t, &down, &c1, &c2)

	resp := postJSON(t, url, `{"jsonrpc":"2.0","id":3,"method":"eth_sendRawTransaction","params":["0xraw"]}`)
	var r Response
	decode(t, resp, &r)
	if r.Error == nil || Code(r.Error.Code) != CodeNonIdempotentFail {
		t.Fatalf("want -32003, got %+v", r.Error)
	}
	if c1.Load() != 1 || c2.Load() != 0 {
		t.Fatalf("non-idempotent must hit p1 exactly once and never p2: %d %d", c1.Load(), c2.Load())
	}
}

// 4) GET 方法被拒为 405。
func TestE2E_MethodNotAllowed(t *testing.T) {
	var down atomic.Bool
	var c1, c2 atomic.Int32
	url := startE2E(t, &down, &c1, &c2)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", resp.StatusCode)
	}
}

// 5) 批量请求在 HTTP 层同样保持 id 一一对应。
func TestE2E_Batch(t *testing.T) {
	var down atomic.Bool
	var c1, c2 atomic.Int32
	url := startE2E(t, &down, &c1, &c2)
	body := `[{"jsonrpc":"2.0","id":"a","method":"eth_blockNumber"},
	          {"jsonrpc":"2.0","id":"b","method":"eth_unknown"}]`
	resp := postJSON(t, url, body)
	var batch []Response
	decode(t, resp, &batch)
	if len(batch) != 2 {
		t.Fatalf("want 2 responses, got %d", len(batch))
	}
	if string(batch[0].ID) != `"a"` || string(batch[1].ID) != `"b"` {
		t.Fatalf("batch ids crossed: %s %s", batch[0].ID, batch[1].ID)
	}
	if batch[1].Error == nil || Code(batch[1].Error.Code) != CodeMethodNotFound {
		t.Fatalf("id b must be method-not-found, got %+v", batch[1].Error)
	}
}

func postJSON(t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http do: %v", err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("decode %s: %v", string(raw), err)
	}
}
