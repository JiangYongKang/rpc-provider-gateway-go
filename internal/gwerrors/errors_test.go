package gwerrors

import (
	"context"
	"errors"
	"testing"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/jsonrpc"
)

func TestNormalizeMapsEachFailureClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Code
	}{
		{"timeout", &TimeoutError{Provider: "p"}, CodeUpstreamTimeout},
		{"deadline", context.DeadlineExceeded, CodeUpstreamTimeout},
		{"transport", &TransportError{Provider: "p"}, CodeUpstreamTransport},
		{"http", &HTTPStatusError{Provider: "p", Status: 500}, CodeUpstreamHTTPStatus},
		{"rpc", &RPCObjectError{Provider: "p", RPC: &jsonrpc.Error{Code: -32000, Message: "x"}}, CodeUpstreamRPCError},
		{"invalid", &InvalidResponseError{Provider: "p"}, CodeInvalidUpstream},
		{"gateway passthrough", ErrRateLimited, CodeRateLimited},
		{"unknown", errors.New("boom"), CodeInternalError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gw := Normalize(tc.err)
			if gw.Code != tc.want {
				t.Fatalf("want code %d, got %d", tc.want, gw.Code)
			}
		})
	}
}

func TestFailureReasonsAreDistinguishable(t *testing.T) {
	rl := Normalize(ErrRateLimited)
	down := Normalize(ErrAllProvidersDown)
	notFound := Normalize(ErrMethodNotFound)
	timeout := Normalize(&TimeoutError{})
	if rl.Code == down.Code || down.Code == notFound.Code || notFound.Code == timeout.Code {
		t.Fatal("rate-limit / all-down / method-not-found / timeout must use distinct codes")
	}
	if rl.Code != CodeRateLimited {
		t.Fatalf("rate limit code = %d", rl.Code)
	}
	if down.Code != CodeAllProvidersDown {
		t.Fatalf("all down code = %d", down.Code)
	}
	if notFound.Code != CodeMethodNotFound {
		t.Fatalf("method not found code = %d", notFound.Code)
	}
}

func TestToRPCErrorCarriesCategoryAndProvider(t *testing.T) {
	e := Normalize(&HTTPStatusError{Provider: "X", Status: 502, Body: "bad gateway"})
	rpc := ToRPCError(e)
	if rpc.Code != int(CodeUpstreamHTTPStatus) {
		t.Fatalf("rpc code %d", rpc.Code)
	}
	if !contains(string(rpc.Data), "upstream_http_status") || !contains(string(rpc.Data), `"provider":"X"`) ||
		!contains(string(rpc.Data), `"http_status":502`) {
		t.Fatalf("error data missing category/provider/status: %s", rpc.Data)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (indexOf(haystack, needle) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
