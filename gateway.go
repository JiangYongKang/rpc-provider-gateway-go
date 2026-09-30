package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Logger 用于单测与运行时输出请求 ID、选中的 provider 与判定依据。
type Logger interface {
	Printf(format string, args ...any)
}

// discardLogger 为默认空日志。
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Options 是 New 的组装参数。
type Options struct {
	Router  *MethodRouter
	Pool    *ProviderPool
	Cache   *ResultCache
	Quota   *Quota
	Timeout time.Duration // 单次 provider 调用的超时上限
	Log     Logger
}

// Gateway 是 JSON-RPC 网关核心：路由、配额、故障转移、缓存。
type Gateway struct {
	router  *MethodRouter
	pool    *ProviderPool
	cache   *ResultCache
	quota   *Quota
	timeout time.Duration
	log     Logger
}

// New 组装网关。
func New(opts Options) *Gateway {
	log := opts.Log
	if log == nil {
		log = discardLogger{}
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Gateway{
		router:  opts.Router,
		pool:    opts.Pool,
		cache:   opts.Cache,
		quota:   opts.Quota,
		timeout: timeout,
		log:     log,
	}
}

// Handle 处理原始 JSON 报文（单个或批量），返回应写回的报文与 HTTP 状态码。
//
// HTTP 状态码语义（传输层）：
//
//	200 至少有一个请求被正常处理（批量中部分失败也为 200，错误体现在 JSON 中）；
//	204 全部为通知（无响应可返回）；
//	400 JSON 解析失败或不是合法的 JSON-RPC 请求对象；
//	405 非 POST；
//	415/其它由 server 层处理。
func (g *Gateway) Handle(ctx context.Context, raw []byte) ([]byte, int) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return errorEnvelope(nil, NewError(CodeParseError, "", nil, nil)), httpStatusBadJSON
	}
	// 批量：以 '[' 开头。
	if trimmed[0] == '[' {
		var batch []json.RawMessage
		if err := json.Unmarshal(trimmed, &batch); err != nil {
			return errorEnvelope(nil, NewError(CodeParseError, "", err, nil)), httpStatusBadJSON
		}
		if len(batch) == 0 {
			resp := RPCErrorResponse(nil, NewError(CodeInvalidRequest,
				"Invalid Request: empty batch", nil, nil))
			out, _ := json.Marshal(resp)
			return out, httpStatusBadJSON
		}
		responses := make([]*Response, 0, len(batch))
		for _, item := range batch {
			req, perr := parseRequest(item)
			if perr != nil {
				// 批量中的非法单项不影响其它项，按规范回一条错误。
				responses = append(responses, RPCErrorResponse(nil, perr))
				continue
			}
			if isNotification(req) {
				continue // 通知不产生响应
			}
			responses = append(responses, g.Serve(ctx, req))
		}
		if len(responses) == 0 {
			return nil, httpStatusNoContent
		}
		out, _ := json.Marshal(responses)
		return out, httpStatusOK
	}

	// 顶层单请求：JSON 语法错误属于 -32700 Parse error。
	if !json.Valid(trimmed) {
		return errorEnvelope(nil, NewError(CodeParseError, "Parse error: malformed JSON", nil, nil)), httpStatusBadJSON
	}
	req, perr := parseRequest(trimmed)
	if perr != nil {
		return errorEnvelope(nil, perr), httpStatusBadJSON
	}
	if isNotification(req) {
		// 单个通知：网关接受但不返回内容。
		g.handleNotification(ctx, req)
		return nil, httpStatusNoContent
	}
	resp := g.Serve(ctx, req)
	out, _ := json.Marshal(resp)
	return out, httpStatusOK
}

const (
	httpStatusOK        = 200
	httpStatusNoContent = 204
	httpStatusBadJSON   = 400
)

func errorEnvelope(id json.RawMessage, err error) []byte {
	out, _ := json.Marshal(RPCErrorResponse(id, err))
	return out
}

// parseRequest 解析并做 JSON-RPC 2.0 基本合法性校验。
// 进入这里的 raw 已经是语法合法的 JSON；若它不是合法的请求对象（
// 例如数字、数组，或字段缺失），按规范返回 -32600 Invalid Request。
func parseRequest(raw json.RawMessage) (*Request, *Error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, NewError(CodeInvalidRequest,
			"Invalid Request: request must be a JSON object", nil, nil)
	}
	var req Request
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, NewError(CodeInvalidRequest, "Invalid Request: "+err.Error(), err, nil)
	}
	if req.JSONRPC != JSONRPCVersion || req.Method == "" {
		return nil, NewError(CodeInvalidRequest,
			"Invalid Request: jsonrpc must be \"2.0\" and method is required", nil, nil)
	}
	// ID 若存在，必须是 string、number 或 null。
	if len(req.ID) > 0 {
		var idv any
		if err := json.Unmarshal(req.ID, &idv); err != nil {
			return nil, NewError(CodeInvalidRequest, "Invalid Request: bad id", err, nil)
		}
		switch idv.(type) {
		case string, float64, nil:
		default:
			return nil, NewError(CodeInvalidRequest, "Invalid Request: id must be string, number or null", nil, nil)
		}
	}
	return &req, nil
}

// isNotification 判断是否为通知（无 id 字段）。
func isNotification(req *Request) bool { return len(req.ID) == 0 }

// handleNotification 对通知做尽力处理，结果不外显。
func (g *Gateway) handleNotification(ctx context.Context, req *Request) {
	release, err := g.quota.Acquire(ctx)
	if err != nil {
		g.log.Printf("notification method=%s dropped: %v", req.Method, err)
		return
	}
	defer release()
	order := g.pool.PickOrder()
	if len(order) == 0 {
		return
	}
	// 通知只发往第一个健康节点，且不做故障转移（没有 id 可对应响应）。
	_, _ = g.callOnce(ctx, order[0], req)
}

// Serve 处理一个已解析的 JSON-RPC 请求（单个），返回 id 一一对应的响应。
func (g *Gateway) Serve(ctx context.Context, req *Request) *Response {
	idStr := requestIDString(req.ID)

	// 1) 方法路由：不支持的方法与 provider 故障、限流严格区分。
	if !g.router.IsSupported(req.Method) {
		g.log.Printf("req_id=%s method=%s decision=reject reason=method_not_supported", idStr, req.Method)
		return RPCErrorResponse(req.ID, NewError(CodeMethodNotFound, "", nil, map[string]any{
			"method": req.Method,
		}))
	}
	policy := g.router.Policy(req.Method)

	// 2) 配额：超限立即拒绝，不触碰任何 provider。
	release, qerr := g.quota.Acquire(ctx)
	if qerr != nil {
		g.log.Printf("req_id=%s method=%s decision=reject reason=quota_exceeded err=%v", idStr, req.Method, qerr)
		return RPCErrorResponse(req.ID, qerr)
	}
	defer release()

	// 3) 只读可缓存方法：先查缓存。
	if policy.Cacheable {
		if cached, ok := g.cache.Get(req.Method, req.Params); ok {
			g.log.Printf("req_id=%s method=%s decision=cache_hit", idStr, req.Method)
			return &Response{JSONRPC: JSONRPCVersion, Result: cached, ID: req.ID}
		}
		g.log.Printf("req_id=%s method=%s decision=cache_miss", idStr, req.Method)
	}

	// 4) 选择 provider 并执行。
	order := g.pool.PickOrder()
	if len(order) == 0 {
		g.log.Printf("req_id=%s method=%s decision=reject reason=no_provider", idStr, req.Method)
		return RPCErrorResponse(req.ID, NewError(CodeNoProvider, "", nil, nil))
	}

	// 非幂等方法：跳过冷却节点，只对第一个健康节点发送一次，
	// 任何结果都如实回传；唯一一次调用失败返回 -32003，绝不重试。
	if !policy.Retryable() {
		tp := order[0]
		if tp.State(g.pool.now()) == HealthCooling {
			// 没有健康节点时，为避免重复投递风险，直接拒绝而非试探。
			g.log.Printf("req_id=%s method=%s decision=reject reason=non_idempotent_no_healthy_provider",
				idStr, req.Method)
			return RPCErrorResponse(req.ID, NewError(CodeNonIdempotentFail,
				"Non-idempotent request refused: no healthy provider; not attempted to avoid duplicate send",
				nil, map[string]any{"method": req.Method}))
		}
		g.log.Printf("req_id=%s method=%s decision=single_attempt provider=%s reason=non_idempotent",
			idStr, req.Method, tp.Name())
		result, fail := g.callOnce(ctx, tp, req)
		if fail != nil {
			g.pool.MarkFailure(tp)
			g.log.Printf("req_id=%s method=%s provider=%s decision=fail_once reason=%s retry=refused",
				idStr, req.Method, tp.Name(), fail.Kind)
			return RPCErrorResponse(req.ID, g.nonIdempotentError(fail))
		}
		g.pool.MarkSuccess(tp)
		g.log.Printf("req_id=%s method=%s provider=%s decision=success", idStr, req.Method, tp.Name())
		return &Response{JSONRPC: JSONRPCVersion, Result: result, ID: req.ID}
	}

	// 只读（幂等）方法：按顺序故障转移，直到成功或耗尽全部 provider。
	attempts := make([]map[string]any, 0, len(order))
	kinds := make([]FailureKind, 0, len(order))
	for i, tp := range order {
		g.log.Printf("req_id=%s method=%s decision=failover_attempt provider=%s attempt=%d",
			idStr, req.Method, tp.Name(), i+1)
		result, fail := g.callOnce(ctx, tp, req)
		if fail == nil {
			g.pool.MarkSuccess(tp)
			g.log.Printf("req_id=%s method=%s provider=%s decision=success attempt=%d",
				idStr, req.Method, tp.Name(), i+1)
			if policy.Cacheable {
				g.cache.Set(req.Method, req.Params, result, policy.TTL)
			}
			return &Response{JSONRPC: JSONRPCVersion, Result: result, ID: req.ID}
		}
		// 只有真正失败的节点才进入健康冷却；成功节点不能被误标。
		g.pool.MarkFailure(tp)
		attempts = append(attempts, map[string]any{
			"provider":  tp.Name(),
			"kind":      fail.Kind.String(),
			"retryable": fail.Retryable(),
		})
		kinds = append(kinds, fail.Kind)
		g.log.Printf("req_id=%s method=%s provider=%s decision=failed kind=%s retryable=%t",
			idStr, req.Method, tp.Name(), fail.Kind, fail.Retryable())

		// JSON-RPC error 等确定性应答不切换 provider，直接透传给调用方。
		if !fail.Retryable() {
			g.log.Printf("req_id=%s method=%s decision=return_upstream_error provider=%s",
				idStr, req.Method, tp.Name())
			return RPCErrorResponse(req.ID, fail.toGatewayError())
		}
	}

	g.log.Printf("req_id=%s method=%s decision=all_providers_failed attempts=%d", idStr, req.Method, len(attempts))
	// 全部失败均为超时：返回专门的 -32002，与一般“全部不可用”区分。
	allTimeout := len(kinds) > 0
	for _, k := range kinds {
		if k != FailureTimeout {
			allTimeout = false
			break
		}
	}
	if allTimeout {
		return RPCErrorResponse(req.ID, NewError(CodeUpstreamTimeout,
			"All providers timed out", nil, map[string]any{
				"method":   req.Method,
				"attempts": attempts,
			}))
	}
	return RPCErrorResponse(req.ID, NewError(CodeNoProvider,
		"All providers failed for this request", nil, map[string]any{
			"method":   req.Method,
			"attempts": attempts,
		}))
}

// nonIdempotentError 把非幂等方法唯一一次调用的失败映射为 -32003，
// 并在 data 中保留真实失败类别与上游错误码，方便调用方判断是否可安全重发。
func (g *Gateway) nonIdempotentError(fail *UpstreamFailure) *Error {
	details := map[string]any{
		"provider": fail.Provider,
		"kind":     fail.Kind.String(),
		"note":     "request was sent at most once; gateway did not retry",
	}
	if fail.Status != 0 {
		details["http_status"] = fail.Status
	}
	if fail.RPC != nil {
		details["upstream_code"] = fail.RPC.Code
		details["upstream_message"] = fail.RPC.Message
	}
	if fail.Cause != nil {
		details["cause"] = fail.Cause.Error()
	}
	return NewError(CodeNonIdempotentFail, "", fail, details)
}

// callOnce 对单个 provider 发送一次请求。
// 成功返回 result；失败返回归一化的 *UpstreamFailure。
// 每次调用使用独立的超时 context；同时校验上游响应 id 必须与请求一致，
// 防止串包。
func (g *Gateway) callOnce(ctx context.Context, tp *trackedProvider, req *Request) (json.RawMessage, *UpstreamFailure) {
	callCtx, cancel := context.WithTimeout(ctx, tp.callTimeout(g.timeout))
	defer cancel()

	resp, err := tp.p.Call(callCtx, req)
	if err != nil {
		var fail *UpstreamFailure
		if errors.As(err, &fail) {
			if fail.Provider == "" {
				fail.Provider = tp.Name()
			}
			// context 超时可能被底层包成普通错误，这里再兜底识别一次。
			if callCtx.Err() == context.DeadlineExceeded && fail.Kind != FailureTimeout {
				// 仅当底层确属 deadline 时归类为超时。
				if errors.Is(err, context.DeadlineExceeded) {
					fail.Kind = FailureTimeout
				}
			}
			return nil, fail
		}
		return nil, &UpstreamFailure{Kind: FailureTransport, Cause: err, Provider: tp.Name()}
	}

	// id 校验：响应必须与本次请求一一对应。
	if !jsonEqualRaw(resp.ID, req.ID) {
		return nil, &UpstreamFailure{
			Kind:     FailureTransport,
			Provider: tp.Name(),
			Cause:    errors.New("upstream response id does not match request id"),
		}
	}
	return resp.Result, nil
}

// callTimeout 取 provider 专属超时，缺省回落到网关默认超时。
func (tp *trackedProvider) callTimeout(def time.Duration) time.Duration {
	if tp.cfg.Timeout > 0 {
		return tp.cfg.Timeout
	}
	return def
}

// jsonEqualRaw 比较两个原始 JSON 值在语义上是否相等。
func jsonEqualRaw(a, b json.RawMessage) bool {
	if bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b)) {
		return true
	}
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	ca, _ := json.Marshal(va)
	cb, _ := json.Marshal(vb)
	return bytes.Equal(ca, cb)
}

// requestIDString 把请求 id 转为便于日志输出的字符串。
func requestIDString(id json.RawMessage) string {
	if len(bytes.TrimSpace(id)) == 0 {
		return "<notification>"
	}
	return string(bytes.TrimSpace(id))
}
