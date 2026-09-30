package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Provider 表示一个后端 JSON-RPC provider。
type Provider interface {
	// Name 返回 provider 的唯一名称（日志与错误详情中使用）。
	Name() string
	// Call 向 provider 发送单个 JSON-RPC 请求并返回响应。
	// 失败时返回的 error 必须是 *UpstreamFailure（已归一化分类），
	// 以便网关决定是否故障转移。
	Call(ctx context.Context, req *Request) (*Response, error)
}

// ProviderConfig 描述一个 provider 的静态配置。
type ProviderConfig struct {
	Name           string
	URL            string
	Weight         int // 预留：当前按配置顺序作为优先级依次尝试
	Timeout        time.Duration
	HealthCooldown time.Duration
}

// HealthState 表示 provider 在网关侧的健康状态。
type HealthState int

const (
	HealthUnknown HealthState = iota
	HealthHealthy
	HealthCooling
)

func (s HealthState) String() string {
	switch s {
	case HealthHealthy:
		return "healthy"
	case HealthCooling:
		return "cooling"
	default:
		return "unknown"
	}
}

// trackedProvider 把一个 Provider 与健康冷却状态打包。
type trackedProvider struct {
	p         Provider
	cfg       ProviderConfig
	coolUntil atomic.Int64 // unix nano；非零且大于当前时间表示冷却中
}

// Name 返回 provider 名称。
func (tp *trackedProvider) Name() string { return tp.cfg.Name }

// State 返回当前健康状态。
func (tp *trackedProvider) State(now time.Time) HealthState {
	if tp.coolUntil.Load() > now.UnixNano() {
		return HealthCooling
	}
	return HealthHealthy
}

// healthyNow 判断 provider 当前是否可被优先选中。
func (tp *trackedProvider) healthyNow(now time.Time) bool {
	return tp.coolUntil.Load() <= now.UnixNano()
}

// ProviderPool 按配置顺序保存 provider，负责选择顺序与健康冷却。
type ProviderPool struct {
	providers []*trackedProvider
	now       func() time.Time
}

// NewProviderPool 依据配置创建 provider 池，每个节点使用 HTTP JSON-RPC。
func NewProviderPool(configs []ProviderConfig) (*ProviderPool, error) {
	pool := &ProviderPool{now: time.Now}
	for _, cfg := range configs {
		if cfg.Name == "" || cfg.URL == "" {
			return nil, NewError(CodeGatewayError, "provider config requires name and url", nil, nil)
		}
		prov := newHTTPProvider(cfg)
		pool.providers = append(pool.providers, &trackedProvider{p: prov, cfg: cfg})
	}
	return pool, nil
}

// addTestProvider 供测试注入自定义 Provider 实现（白盒测试）。
func (p *ProviderPool) addTestProvider(prov Provider, cfg ProviderConfig) {
	if cfg.Name == "" {
		cfg.Name = prov.Name()
	}
	p.providers = append(p.providers, &trackedProvider{p: prov, cfg: cfg})
}

// Len 返回 provider 数量。
func (p *ProviderPool) Len() int { return len(p.providers) }

// PickOrder 返回一次请求应当尝试的 provider 顺序：
// 健康节点按配置顺序在前，冷却中的节点按配置顺序作为兜底在后。
// 这样故障节点会被短暂摘除，但全部节点都冷却时请求仍会被尝试，
// 而不是被静默丢弃。
func (p *ProviderPool) PickOrder() []*trackedProvider {
	now := p.now()
	healthy := make([]*trackedProvider, 0, len(p.providers))
	cooling := make([]*trackedProvider, 0)
	for _, tp := range p.providers {
		if tp.healthyNow(now) {
			healthy = append(healthy, tp)
		} else {
			cooling = append(cooling, tp)
		}
	}
	return append(healthy, cooling...)
}

// MarkSuccess 在调用成功后清除冷却标记。
func (p *ProviderPool) MarkSuccess(tp *trackedProvider) {
	tp.coolUntil.Store(0)
}

// MarkFailure 在调用失败后让节点进入健康冷却。
func (p *ProviderPool) MarkFailure(tp *trackedProvider) {
	if tp.cfg.HealthCooldown <= 0 {
		return
	}
	deadline := p.now().Add(tp.cfg.HealthCooldown).UnixNano()
	tp.coolUntil.Store(deadline)
}

// httpProvider 通过 HTTP POST 调用后端 JSON-RPC。
type httpProvider struct {
	cfg    ProviderConfig
	client *http.Client
}

func newHTTPProvider(cfg ProviderConfig) *httpProvider {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &httpProvider{
		cfg: cfg,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				// 连接必须尽快失败，便于故障转移生效。
				DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
			},
		},
	}
}

func (h *httpProvider) Name() string { return h.cfg.Name }

func (h *httpProvider) Call(ctx context.Context, req *Request) (*Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, &UpstreamFailure{Kind: FailureTransport, Cause: err, Provider: h.cfg.Name}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, h.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return nil, &UpstreamFailure{Kind: FailureTransport, Cause: err, Provider: h.cfg.Name}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := h.client.Do(httpReq)
	if err != nil {
		return nil, h.classifyErr(err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, &UpstreamFailure{Kind: FailureTransport, Cause: err, Provider: h.cfg.Name, Status: resp.StatusCode}
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, &UpstreamFailure{
			Kind:     FailureHTTPStatus,
			Status:   resp.StatusCode,
			Cause:    errors.New("non-2xx http status: " + http.StatusText(resp.StatusCode)),
			Provider: h.cfg.Name,
		}
	}

	var rpcResp Response
	if err := json.Unmarshal(raw, &rpcResp); err != nil {
		return nil, &UpstreamFailure{
			Kind:     FailureTransport,
			Cause:    errors.New("invalid json response body: " + err.Error()),
			Provider: h.cfg.Name,
			Status:   resp.StatusCode,
		}
	}
	if rpcResp.Error != nil {
		return nil, &UpstreamFailure{Kind: FailureRPCError, RPC: rpcResp.Error, Provider: h.cfg.Name}
	}
	return &rpcResp, nil
}

// classifyErr 把 net/http 的底层错误归一为超时或传输层故障。
func (h *httpProvider) classifyErr(err error) *UpstreamFailure {
	f := &UpstreamFailure{Cause: err, Provider: h.cfg.Name}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		f.Kind = FailureTimeout
		return f
	}
	if errors.Is(err, context.DeadlineExceeded) {
		f.Kind = FailureTimeout
		return f
	}
	f.Kind = FailureTransport
	return f
}
