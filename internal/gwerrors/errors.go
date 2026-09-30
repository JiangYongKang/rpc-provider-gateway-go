// Package gwerrors defines the gateway's unified error type, stable external
// error codes and the typed upstream errors that providers produce.
package gwerrors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/jsonrpc"
)

// Code is the stable external error code. Callers can branch on these values;
// they never change between releases.
type Code int

// Stable external error codes. -32700..-32600 follow the JSON-RPC 2.0
// specification; -32001..-32099 are gateway-defined.
const (
	CodeJSONParseError     Code = -32700
	CodeInvalidRequest     Code = -32600
	CodeMethodNotFound     Code = -32601
	CodeInvalidParams      Code = -32602
	CodeInternalError      Code = -32603
	CodeRateLimited        Code = -32001
	CodeAllProvidersDown   Code = -32002
	CodeUpstreamTimeout    Code = -32003
	CodeUpstreamTransport  Code = -32004
	CodeUpstreamHTTPStatus Code = -32005
	CodeUpstreamRPCError   Code = -32006
	CodeInvalidUpstream    Code = -32007
	CodeRetryNotAllowed    Code = -32008
)

// Sentinels for router/gateway level failures that are not tied to one
// provider response.
var (
	ErrRateLimited      = &GatewayError{Code: CodeRateLimited, Message: DefaultMessage(CodeRateLimited)}
	ErrAllProvidersDown = &GatewayError{Code: CodeAllProvidersDown, Message: DefaultMessage(CodeAllProvidersDown)}
	ErrMethodNotFound   = &GatewayError{Code: CodeMethodNotFound, Message: DefaultMessage(CodeMethodNotFound)}
)

// GatewayError is the single normalized error type exposed by the gateway.
type GatewayError struct {
	Code     Code
	Message  string
	Provider string          // provider that produced the failure, if a single one
	Upstream json.RawMessage // raw upstream JSON-RPC error, if any
	Cause    error
}

// Error implements error.
func (e *GatewayError) Error() string {
	var b strings.Builder
	if e.Provider != "" {
		fmt.Fprintf(&b, "provider %q: ", e.Provider)
	}
	if e.Message != "" {
		b.WriteString(e.Message)
	} else {
		b.WriteString(DefaultMessage(e.Code))
	}
	if e.Cause != nil {
		fmt.Fprintf(&b, ": %v", e.Cause)
	}
	return b.String()
}

// Unwrap supports errors.Is/errors.As.
func (e *GatewayError) Unwrap() error { return e.Cause }

// DefaultMessage returns the canonical message for a code.
func DefaultMessage(c Code) string {
	switch c {
	case CodeJSONParseError:
		return "parse error"
	case CodeInvalidRequest:
		return "invalid request"
	case CodeMethodNotFound:
		return "method not supported by any configured provider"
	case CodeInvalidParams:
		return "invalid params"
	case CodeInternalError:
		return "internal error"
	case CodeRateLimited:
		return "request rejected: call quota exceeded"
	case CodeAllProvidersDown:
		return "all providers unavailable"
	case CodeUpstreamTimeout:
		return "upstream request timed out"
	case CodeUpstreamTransport:
		return "upstream transport error"
	case CodeUpstreamHTTPStatus:
		return "upstream returned a non-2xx HTTP status"
	case CodeUpstreamRPCError:
		return "upstream returned a JSON-RPC error"
	case CodeInvalidUpstream:
		return "upstream response was not valid JSON-RPC"
	case CodeRetryNotAllowed:
		return "method is non-idempotent and cannot be retried on another provider"
	default:
		return "error"
	}
}

// New builds a GatewayError with the canonical message for code.
func New(code Code, cause error) *GatewayError {
	return &GatewayError{Code: code, Message: DefaultMessage(code), Cause: cause}
}

// WithProvider annotates an error with the provider name.
func (e *GatewayError) WithProvider(name string) *GatewayError {
	e.Provider = name
	return e
}

// WithUpstream attaches the raw upstream JSON-RPC error.
func (e *GatewayError) WithUpstream(raw json.RawMessage) *GatewayError {
	e.Upstream = raw
	return e
}

// Typed upstream failures produced by provider implementations.

// TimeoutError means the upstream did not respond in time.
type TimeoutError struct {
	Provider string
	Op       string
	Cause    error
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("provider %q: timeout during %s: %v", e.Provider, e.Op, e.Cause)
}
func (e *TimeoutError) Unwrap() error { return e.Cause }

// TransportError means the HTTP request itself failed (connection refused,
// DNS error, connection reset, ...).
type TransportError struct {
	Provider string
	Cause    error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("provider %q: transport error: %v", e.Provider, e.Cause)
}
func (e *TransportError) Unwrap() error { return e.Cause }

// HTTPStatusError means the upstream answered but with a non-2xx status.
type HTTPStatusError struct {
	Provider string
	Status   int
	Body     string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("provider %q: HTTP %d: %s", e.Provider, e.Status, truncate(e.Body))
}

// RPCObjectError means the upstream replied 2xx with a JSON-RPC "error" object.
type RPCObjectError struct {
	Provider string
	RPC      *jsonrpc.Error
}

func (e *RPCObjectError) Error() string {
	return fmt.Sprintf("provider %q: %v", e.Provider, e.RPC)
}

// InvalidResponseError means the upstream reply was not a valid JSON-RPC
// response (missing jsonrpc, both/neither result+error, bad id, ...).
type InvalidResponseError struct {
	Provider string
	Detail   string
}

func (e *InvalidResponseError) Error() string {
	return fmt.Sprintf("provider %q: invalid upstream response: %s", e.Provider, e.Detail)
}

// Normalize converts an arbitrary error into a *GatewayError, mapping each
// upstream failure class to a stable code.
func Normalize(err error) *GatewayError {
	if err == nil {
		return nil
	}
	var gw *GatewayError
	if errors.As(err, &gw) {
		if gw.Message == "" {
			gw.Message = DefaultMessage(gw.Code)
		}
		return gw
	}
	var (
		timeout *TimeoutError
		trans   *TransportError
		status  *HTTPStatusError
		rpcErr  *RPCObjectError
		invalid *InvalidResponseError
	)
	switch {
	case errors.As(err, &timeout):
		return &GatewayError{Code: CodeUpstreamTimeout, Message: DefaultMessage(CodeUpstreamTimeout), Provider: timeout.Provider, Cause: timeout}
	case errors.Is(err, context.DeadlineExceeded):
		return New(CodeUpstreamTimeout, err)
	case errors.As(err, &trans):
		return &GatewayError{Code: CodeUpstreamTransport, Message: DefaultMessage(CodeUpstreamTransport), Provider: trans.Provider, Cause: trans}
	case errors.As(err, &status):
		return &GatewayError{Code: CodeUpstreamHTTPStatus, Message: DefaultMessage(CodeUpstreamHTTPStatus), Provider: status.Provider, Cause: status}
	case errors.As(err, &rpcErr):
		gw := &GatewayError{Code: CodeUpstreamRPCError, Message: DefaultMessage(CodeUpstreamRPCError), Provider: rpcErr.Provider, Cause: rpcErr}
		if rpcErr.RPC != nil {
			if raw, e := json.Marshal(rpcErr.RPC); e == nil {
				gw.Upstream = raw
			}
		}
		return gw
	case errors.As(err, &invalid):
		return &GatewayError{Code: CodeInvalidUpstream, Message: DefaultMessage(CodeInvalidUpstream), Provider: invalid.Provider, Cause: invalid}
	default:
		return New(CodeInternalError, err)
	}
}

// ToRPCError converts a normalized gateway error into a JSON-RPC error object.
// Upstream details (provider, upstream code, HTTP status) are placed in data
// so the cause category remains machine-readable.
func ToRPCError(e *GatewayError) *jsonrpc.Error {
	msg := e.Message
	if msg == "" {
		msg = DefaultMessage(e.Code)
	}
	out := &jsonrpc.Error{Code: int(e.Code), Message: msg}
	data := map[string]any{"category": categoryName(e.Code)}
	if e.Provider != "" {
		data["provider"] = e.Provider
	}
	if len(e.Upstream) > 0 {
		var up map[string]any
		if json.Unmarshal(e.Upstream, &up) == nil {
			data["upstream"] = up
		}
	}
	if e.Cause != nil {
		var statusErr *HTTPStatusError
		if errors.As(e.Cause, &statusErr) {
			data["http_status"] = statusErr.Status
		}
	}
	if raw, err := json.Marshal(data); err == nil {
		out.Data = raw
	}
	return out
}

func categoryName(c Code) string {
	switch c {
	case CodeJSONParseError:
		return "parse_error"
	case CodeInvalidRequest:
		return "invalid_request"
	case CodeMethodNotFound:
		return "method_not_supported"
	case CodeInvalidParams:
		return "invalid_params"
	case CodeInternalError:
		return "internal_error"
	case CodeRateLimited:
		return "rate_limited"
	case CodeAllProvidersDown:
		return "all_providers_down"
	case CodeUpstreamTimeout:
		return "upstream_timeout"
	case CodeUpstreamTransport:
		return "upstream_transport"
	case CodeUpstreamHTTPStatus:
		return "upstream_http_status"
	case CodeUpstreamRPCError:
		return "upstream_jsonrpc_error"
	case CodeInvalidUpstream:
		return "upstream_invalid_response"
	case CodeRetryNotAllowed:
		return "retry_not_allowed"
	default:
		return "unknown"
	}
}

func truncate(s string) string {
	const max = 200
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}
