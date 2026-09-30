package gateway

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeProvider 是本地可控的假 provider：
// 按调用序号依次返回预设结果，可模拟延迟、传输错误、HTTP 状态、
// JSON-RPC error 与超时，并记录调用次数与并发在途数。
type fakeProvider struct {
	name string
	mu   sync.Mutex

	// calls 记录每次收到的请求（按调用顺序）。
	calls []*Request
	// times 记录每次调用开始时间。
	times []time.Time
	// inFlight 为当前在途调用数；maxInFlight 为其峰值。
	inFlight    atomic.Int64
	maxInFlight atomic.Int64

	// script 按调用次序返回预设行为；超出长度后重复最后一项。
	script []fakeBehavior
	idx    atomic.Int64

	delay time.Duration
}

// fakeBehavior 描述假 provider 对一次调用的应答方式。
type fakeBehavior struct {
	// resp / fail 二选一：fail 非空时直接返回错误。
	resp *Response
	fail *UpstreamFailure
	// timeout 为 true 时阻塞到 ctx 超时（模拟 provider 挂死）。
	timeout bool
	delay   time.Duration
}

func newFakeProvider(name string, script ...fakeBehavior) *fakeProvider {
	return &fakeProvider{name: name, script: script}
}

func (f *fakeProvider) withDelay(d time.Duration) *fakeProvider { f.delay = d; return f }

func (f *fakeProvider) Name() string { return f.name }

func (f *fakeProvider) Call(ctx context.Context, req *Request) (*Response, error) {
	cur := f.inFlight.Add(1)
	for {
		max := f.maxInFlight.Load()
		if cur <= max || f.maxInFlight.CompareAndSwap(max, cur) {
			break
		}
	}
	defer f.inFlight.Add(-1)

	f.mu.Lock()
	f.calls = append(f.calls, req)
	f.times = append(f.times, time.Now())
	f.mu.Unlock()

	i := f.idx.Add(1) - 1
	b := fakeBehavior{}
	if len(f.script) > 0 {
		if int(i) >= len(f.script) {
			b = f.script[len(f.script)-1]
		} else {
			b = f.script[i]
		}
	}
	delay := f.delay
	if b.delay > 0 {
		delay = b.delay
	}
	if b.timeout {
		<-ctx.Done()
		return nil, &UpstreamFailure{Kind: FailureTimeout, Cause: ctx.Err(), Provider: f.name}
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, &UpstreamFailure{Kind: FailureTimeout, Cause: ctx.Err(), Provider: f.name}
		}
	}
	if b.fail != nil {
		if b.fail.Provider == "" {
			b.fail.Provider = f.name
		}
		return nil, b.fail
	}
	resp := b.resp
	if resp == nil {
		resp = &Response{JSONRPC: JSONRPCVersion, Result: json.RawMessage(`{"ok":true}`)}
	}
	// 始终以请求 id 回显，保证“响应与请求一一对应”；预置 resp 只提供 result/error。
	resp.ID = req.ID
	if resp.JSONRPC == "" {
		resp.JSONRPC = JSONRPCVersion
	}
	return resp, nil
}

// callCount 返回该 provider 被调用的总次数。
func (f *fakeProvider) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// peakInFlight 返回并发在途峰值。
func (f *fakeProvider) peakInFlight() int64 { return f.maxInFlight.Load() }

// testLog 把判定日志同时输出到 testing 日志（go test -v 可见请求 ID、provider、依据）。
type testLog struct{ t *testing.T }

func (l testLog) Printf(format string, args ...any) { l.t.Logf(format, args...) }

// buildTestGateway 用假 provider 组装网关并返回内部句柄，便于断言。
func buildTestGateway(t *testing.T, quota *Quota, timeout time.Duration, policies map[string]MethodPolicy, fakes ...*fakeProvider) *Gateway {
	t.Helper()
	pool, err := NewProviderPool(nil)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	for _, fp := range fakes {
		cfg := ProviderConfig{Name: fp.name, URL: "fake://" + fp.name, HealthCooldown: 50 * time.Millisecond}
		pool.addTestProvider(fp, cfg)
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

// mustReq 构造一个 JSON-RPC 请求对象。
func mustReq(id, method, params string) *Request {
	req := &Request{JSONRPC: JSONRPCVersion, Method: method}
	if id != "" {
		req.ID = json.RawMessage(id)
	}
	if params != "" {
		req.Params = json.RawMessage(params)
	}
	return req
}

var _ = atomic.Int64{}

// idEchoProvider 依据请求参数返回对应结果，并记录调用次数与真实并发峰值。
type idEchoProvider struct {
	name      string
	delay     time.Duration
	mu        sync.Mutex
	calls     int
	inFlight  int64
	maxFlight int64
}

func (p *idEchoProvider) Name() string { return p.name }

func (p *idEchoProvider) Call(ctx context.Context, req *Request) (*Response, error) {
	p.mu.Lock()
	p.calls++
	p.inFlight++
	if p.inFlight > p.maxFlight {
		p.maxFlight = p.inFlight
	}
	params := string(req.Params)
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.inFlight--
		p.mu.Unlock()
	}()

	select {
	case <-time.After(p.delay):
	case <-ctx.Done():
		return nil, &UpstreamFailure{Kind: FailureTimeout, Cause: ctx.Err(), Provider: p.name}
	}

	result := json.RawMessage(`{"echo":` + trimBrackets(params) + `,"provider":"` + p.name + `"}`)
	return &Response{JSONRPC: JSONRPCVersion, ID: req.ID, Result: result}, nil
}

// trimBrackets 把位置参数 ["0x", 7] 形式粗暴还原为内部值用于断言。
func trimBrackets(s string) string {
	if len(s) >= 2 && s[0] == '[' && s[len(s)-1] == ']' {
		inner := s[1 : len(s)-1]
		if inner == "" {
			return "null"
		}
		return inner
	}
	return s
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
