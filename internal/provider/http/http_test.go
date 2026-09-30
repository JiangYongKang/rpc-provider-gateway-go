package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/gwerrors"
)

func callExpect(t *testing.T, handler http.HandlerFunc, timeout time.Duration) error {
	t.Helper()
	srv := httptest.NewServer(handler)
	defer srv.Close()
	p := New("test", srv.URL, timeout)
	_, err := p.Call(context.Background(), "eth_blockNumber", json.RawMessage(`[]`))
	return err
}

func TestHTTPSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x99"}`))
	}))
	defer srv.Close()
	p := New("test", srv.URL, time.Second)
	res, err := p.Call(context.Background(), "eth_blockNumber", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != `"0x99"` {
		t.Fatalf("result %s", res)
	}
}

func TestHTTPRPCErrorClass(t *testing.T) {
	err := callExpect(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"upstream boom"}}`))
	}, time.Second)
	var rpcErr *gwerrors.RPCObjectError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("expected RPCObjectError, got %T %v", err, err)
	}
	if gwerrors.Normalize(err).Code != gwerrors.CodeUpstreamRPCError {
		t.Fatal("normalized code mismatch")
	}
}

func TestHTTPStatusClass(t *testing.T) {
	err := callExpect(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`upstream exploded`))
	}, time.Second)
	var st *gwerrors.HTTPStatusError
	if !errors.As(err, &st) || st.Status != 500 {
		t.Fatalf("expected HTTPStatusError 500, got %T %v", err, err)
	}
}

func TestHTTPTransportClass(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // connection refused
	p := New("test", srv.URL, time.Second)
	_, err := p.Call(context.Background(), "m", nil)
	if _, ok := err.(*gwerrors.TransportError); !ok {
		t.Fatalf("expected TransportError, got %T %v", err, err)
	}
}

func TestHTTPTimeoutClass(t *testing.T) {
	err := callExpect(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}, 50*time.Millisecond)
	var to *gwerrors.TimeoutError
	if !errors.As(err, &to) {
		t.Fatalf("expected TimeoutError, got %T %v", err, err)
	}
	if gwerrors.Normalize(err).Code != gwerrors.CodeUpstreamTimeout {
		t.Fatal("normalized timeout code mismatch")
	}
}

func TestHTTPInvalidBodies(t *testing.T) {
	cases := map[string]string{
		"bad json":     `{not json`,
		"no version":   `{"id":1,"result":"x"}`,
		"id mismatch":  `{"jsonrpc":"2.0","id":999,"result":"x"}`,
		"empty result": `{"jsonrpc":"2.0","id":1}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			err := callExpect(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(body))
			}, time.Second)
			if _, ok := err.(*gwerrors.InvalidResponseError); !ok {
				t.Fatalf("%s: expected InvalidResponseError, got %T %v", name, err, err)
			}
		})
	}
}

func TestUnavailableFlag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":1}`))
	}))
	defer srv.Close()
	p := New("test", srv.URL, time.Second)
	if !p.Available() {
		t.Fatal("should default available")
	}
	p.SetAvailable(false)
	if p.Available() {
		t.Fatal("should be unavailable after SetAvailable(false)")
	}
}
