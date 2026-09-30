package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// Code 是网关对外暴露的稳定错误码。
//
// 标准 JSON-RPC 错误码：
//
//	-32700 Parse error；-32600 Invalid Request；-32601 Method not found；
//	-32603 Internal error。
//
// 网关自有错误码（-32000 ~ -32099 服务端保留区间）：
//
//	-32000 上游 provider 返回 JSON-RPC error（data 中携带上游原始错误码）；
//	-32001 provider 全部不可用 / 全部尝试失败；
//	-32002 上游超时；
//	-32003 非幂等方法已投递一次但失败，网关拒绝重试；
//	-32004 请求超出并发/速率配额被拒绝；
//	-32005 网关配置或内部错误。
type Code int

const (
	CodeParseError        Code = -32700
	CodeInvalidRequest    Code = -32600
	CodeMethodNotFound    Code = -32601
	CodeInternalError     Code = -32603
	CodeUpstreamError     Code = -32000
	CodeNoProvider        Code = -32001
	CodeUpstreamTimeout   Code = -32002
	CodeNonIdempotentFail Code = -32003
	CodeRateLimited       Code = -32004
	CodeGatewayError      Code = -32005
)

// codeMessage 是每个错误码对外的稳定简短描述。
var codeMessage = map[Code]string{
	CodeParseError:        "Parse error",
	CodeInvalidRequest:    "Invalid Request",
	CodeMethodNotFound:    "Method not found",
	CodeInternalError:     "Internal error",
	CodeUpstreamError:     "Upstream JSON-RPC error",
	CodeNoProvider:        "No provider available",
	CodeUpstreamTimeout:   "Upstream request timeout",
	CodeNonIdempotentFail: "Non-idempotent request failed on the only attempt; retry refused",
	CodeRateLimited:       "Rate limited: request quota exceeded",
	CodeGatewayError:      "Gateway error",
}

// Error 是网关统一错误类型，携带稳定错误码、可读信息与可选详情。
type Error struct {
	Code    Code
	Message string
	// Details 会被序列化到 JSON-RPC error.data 中，用于区分失败原因。
	Details map[string]any
	// cause 用于 errors.Is/As，不会原样暴露给调用方。
	cause error
}

// NewError 构造统一错误。
func NewError(code Code, message string, cause error, details map[string]any) *Error {
	if message == "" {
		message = codeMessage[code]
	}
	return &Error{Code: code, Message: message, Details: details, cause: cause}
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	s := fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
	if e.cause != nil {
		s += ": " + e.cause.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.cause }

// AsError 把任意 error 归一为 *Error；已是 *Error 则原样返回。
func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	var ge *Error
	if errors.As(err, &ge) {
		return ge
	}
	return &Error{Code: CodeGatewayError, Message: err.Error(), cause: err}
}

// RPCErrorResponse 依据统一错误构造 JSON-RPC 错误响应。
// id 为 nil（通知或无法解析）时按规范使用 null。
func RPCErrorResponse(id json.RawMessage, err error) *Response {
	ge := AsError(err)
	data := ge.Details
	rpcErr := &RPCError{
		Code:    int(ge.Code),
		Message: ge.Message,
	}
	if len(data) > 0 {
		if raw, mErr := json.Marshal(data); mErr == nil {
			rpcErr.Data = raw
		}
	}
	if id == nil {
		id = json.RawMessage("null")
	}
	return &Response{JSONRPC: JSONRPCVersion, Error: rpcErr, ID: id}
}

// UpstreamFailure 描述一次对 provider 调用失败的归一化分类。
// provider 的各类错误（传输层、HTTP 状态码、JSON-RPC error、超时）
// 在进入网关决策逻辑之前都会被归一成该类型。
type UpstreamFailure struct {
	// Kind 为故障大类。
	Kind FailureKind
	// Cause 为底层错误。
	Cause error
	// Status 为 HTTP 状态码（HTTPStatus 类故障时有效）。
	Status int
	// RPC 为上游返回的 JSON-RPC error 对象（RPCError 类故障时有效）。
	RPC *RPCError
	// Provider 为发生故障的 provider 名称。
	Provider string
}

// FailureKind 是上游故障大类。
type FailureKind int

const (
	// FailureTransport 传输层错误：连接被拒、连接重置、EOF、DNS 失败等。
	FailureTransport FailureKind = iota
	// FailureTimeout 请求超时（context deadline exceeded）。
	FailureTimeout
	// FailureHTTPStatus 上游返回了非 2xx 的 HTTP 状态码。
	FailureHTTPStatus
	// FailureRPCError 成功收到 HTTP 响应，但其中是 JSON-RPC error 对象。
	FailureRPCError
)

func (kind FailureKind) String() string {
	switch kind {
	case FailureTransport:
		return "transport"
	case FailureTimeout:
		return "timeout"
	case FailureHTTPStatus:
		return "http_status"
	case FailureRPCError:
		return "rpc_error"
	default:
		return "unknown"
	}
}

func (f *UpstreamFailure) Error() string {
	if f == nil {
		return ""
	}
	switch f.Kind {
	case FailureRPCError:
		if f.RPC != nil {
			return fmt.Sprintf("provider %s rpc error %d: %s", f.Provider, f.RPC.Code, f.RPC.Message)
		}
	case FailureHTTPStatus:
		return fmt.Sprintf("provider %s http status %d", f.Provider, f.Status)
	case FailureTimeout:
		return fmt.Sprintf("provider %s timeout: %v", f.Provider, f.Cause)
	}
	return fmt.Sprintf("provider %s transport error: %v", f.Provider, f.Cause)
}

// Retryable 表示该故障在“方法本身允许故障转移”的前提下，
// 是否允许把请求切换到下一个 provider 重试。
//
// 传输层错误与超时：请求很可能没有被后端处理，可切换。
// HTTP 408/429/5xx：服务端过载或暂时不可用，可切换。
// JSON-RPC error 对象：这是后端给出的确定性业务/协议应答，不切换，直接透传。
func (f *UpstreamFailure) Retryable() bool {
	switch f.Kind {
	case FailureTransport, FailureTimeout:
		return true
	case FailureHTTPStatus:
		return f.Status == http.StatusRequestTimeout ||
			f.Status == http.StatusTooManyRequests ||
			f.Status >= 500
	default:
		return false
	}
}

// toGatewayError 把单次上游故障映射为对外统一错误（用于“不再继续尝试”时）。
func (f *UpstreamFailure) toGatewayError() *Error {
	switch f.Kind {
	case FailureTimeout:
		return NewError(CodeUpstreamTimeout, "", f.Cause, map[string]any{
			"provider": f.Provider,
			"kind":     f.Kind.String(),
		})
	case FailureRPCError:
		details := map[string]any{"provider": f.Provider, "kind": f.Kind.String()}
		if f.RPC != nil {
			details["upstream_code"] = f.RPC.Code
			details["upstream_message"] = f.RPC.Message
			if len(f.RPC.Data) > 0 {
				var data any
				if json.Unmarshal(f.RPC.Data, &data) == nil {
					details["upstream_data"] = data
				}
			}
		}
		return NewError(CodeUpstreamError, "", f, details)
	default:
		details := map[string]any{"provider": f.Provider, "kind": f.Kind.String()}
		if f.Status != 0 {
			details["http_status"] = f.Status
		}
		if f.Cause != nil {
			details["cause"] = f.Cause.Error()
		}
		return NewError(CodeNoProvider, "", f, details)
	}
}
