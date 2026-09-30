// Package http implements provider.Provider over HTTP POST (JSON-RPC 2.0).
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	nethttp "net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/gwerrors"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/jsonrpc"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/provider"
)

// HTTPProvider calls a JSON-RPC endpoint over HTTP.
type HTTPProvider struct {
	name      string
	url       string
	timeout   time.Duration
	client    *nethttp.Client
	available atomic.Bool

	// requestID is the internal id used between gateway and upstream. It is
	// unrelated to the caller's id and is verified on the response.
	requestID atomic.Int64
}

var _ provider.Provider = (*HTTPProvider)(nil)

// New builds an HTTPProvider with its own http.Client and per-call timeout.
func New(name, url string, timeout time.Duration) *HTTPProvider {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	p := &HTTPProvider{
		name:    name,
		url:     url,
		timeout: timeout,
		client: &nethttp.Client{
			Timeout: timeout,
		},
	}
	p.available.Store(true)
	return p
}

// Name returns the provider name.
func (p *HTTPProvider) Name() string { return p.name }

// Available reports current availability.
func (p *HTTPProvider) Available() bool { return p.available.Load() }

// SetAvailable flips availability (used by health state / tests).
func (p *HTTPProvider) SetAvailable(v bool) { p.available.Store(v) }

// Call performs one JSON-RPC call. The returned error, if non-nil, is always
// one of the typed errors in gwerrors so the router can categorize it.
func (p *HTTPProvider) Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	id := p.requestID.Add(1)
	req := jsonrpc.Request{
		JSONRPC: jsonrpc.Version,
		ID:      jsonrpc.RawID(strconv.FormatInt(id, 10)),
		Method:  method,
		Params:  params,
	}
	body, err := json.Marshal(&req)
	if err != nil {
		return nil, &gwerrors.InvalidResponseError{Provider: p.name, Detail: "marshal request: " + err.Error()}
	}

	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	httpReq, err := nethttp.NewRequestWithContext(callCtx, nethttp.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return nil, &gwerrors.TransportError{Provider: p.name, Cause: err}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := p.client.Do(httpReq)
	if err != nil {
		if callCtx.Err() == context.DeadlineExceeded || ctx.Err() == context.DeadlineExceeded {
			return nil, &gwerrors.TimeoutError{Provider: p.name, Op: "POST " + p.url, Cause: err}
		}
		// A parent cancellation (e.g. non-idempotent single-shot already done)
		// stays a timeout/cancel category; surface as transport too.
		return nil, &gwerrors.TransportError{Provider: p.name, Cause: err}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, &gwerrors.TransportError{Provider: p.name, Cause: err}
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, &gwerrors.HTTPStatusError{Provider: p.name, Status: resp.StatusCode, Body: string(respBody)}
	}

	var rpcResp jsonrpc.Response
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil, &gwerrors.InvalidResponseError{Provider: p.name, Detail: "invalid JSON body: " + err.Error()}
	}
	if rpcResp.JSONRPC != jsonrpc.Version {
		return nil, &gwerrors.InvalidResponseError{Provider: p.name, Detail: `missing or invalid "jsonrpc" field`}
	}
	if string(rpcResp.ID) != string(req.ID) {
		return nil, &gwerrors.InvalidResponseError{
			Provider: p.name,
			Detail:   "response id " + rpcResp.ID.String() + " does not match request id " + req.ID.String(),
		}
	}
	if rpcResp.Error != nil {
		return nil, &gwerrors.RPCObjectError{Provider: p.name, RPC: rpcResp.Error}
	}
	if len(rpcResp.Result) == 0 {
		return nil, &gwerrors.InvalidResponseError{Provider: p.name, Detail: "response has neither result nor error"}
	}
	return rpcResp.Result, nil
}
