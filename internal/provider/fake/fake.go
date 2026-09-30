// Package fake provides a fully controllable in-process Provider for tests:
// behavior, latency, availability and a per-method call counter can all be
// scripted, so failover / non-idempotence / caching can be asserted exactly.
package fake

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/gwerrors"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/jsonrpc"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/provider"
)

// Behavior decides what a fake provider does for one call.
type Behavior int

const (
	// BehaviorOK returns a configurable successful result.
	BehaviorOK Behavior = iota
	// BehaviorRPCError returns a JSON-RPC error object.
	BehaviorRPCError
	// BehaviorTransport returns a transport-layer error.
	BehaviorTransport
	// BehaviorHTTP returns a non-2xx HTTP-status style error.
	BehaviorHTTP
	// BehaviorTimeout blocks until the context deadline (or longer).
	BehaviorTimeout
	// BehaviorInvalid returns an invalid-response error.
	BehaviorInvalid
)

// Rule scripts behavior per method (fallback used when no exact match).
type Rule struct {
	Behavior Behavior
	Result   json.RawMessage
	Delay    time.Duration
	RPCCode  int
	RPCMsg   string
	Status   int
}

// Provider is the controllable fake.
type Provider struct {
	name      string
	mu        sync.Mutex
	available bool
	rules     map[string]Rule
	fallback  Rule
	// calls counts total and per-method attempts actually executed.
	callsTotal atomic.Int64
	calls      map[string]*atomic.Int64
}

var _ provider.Provider = (*Provider)(nil)

// New creates a named fake with the given fallback rule.
func New(name string, fallback Rule) *Provider {
	return &Provider{
		name:      name,
		available: true,
		rules:     make(map[string]Rule),
		fallback:  fallback,
		calls:     make(map[string]*atomic.Int64),
	}
}

// SetRule scripts the behavior for one method.
func (p *Provider) SetRule(method string, r Rule) *Provider {
	p.mu.Lock()
	p.rules[method] = r
	p.mu.Unlock()
	return p
}

// SetFallback sets the rule used when no per-method rule exists.
func (p *Provider) SetFallback(r Rule) {
	p.mu.Lock()
	p.fallback = r
	p.mu.Unlock()
}

// Name returns the name.
func (p *Provider) Name() string { return p.name }

// Available reports availability.
func (p *Provider) Available() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.available
}

// SetAvailable flips availability.
func (p *Provider) SetAvailable(v bool) {
	p.mu.Lock()
	p.available = v
	p.mu.Unlock()
}

// Calls returns how many times a method was actually attempted.
func (p *Provider) Calls(method string) int64 {
	p.mu.Lock()
	c := p.calls[method]
	p.mu.Unlock()
	if c == nil {
		return 0
	}
	return c.Load()
}

// TotalCalls returns the total number of calls attempted.
func (p *Provider) TotalCalls() int64 { return p.callsTotal.Load() }

func (p *Provider) ruleFor(method string) Rule {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r, ok := p.rules[method]; ok {
		return r
	}
	return p.fallback
}

func (p *Provider) counter(method string) *atomic.Int64 {
	p.mu.Lock()
	c, ok := p.calls[method]
	if !ok {
		c = &atomic.Int64{}
		p.calls[method] = c
	}
	p.mu.Unlock()
	return c
}

// Call executes the scripted behavior, honoring context cancellation.
func (p *Provider) Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	if !p.Available() {
		return nil, &gwerrors.TransportError{Provider: p.name, Cause: errProviderUnavailable}
	}
	p.callsTotal.Add(1)
	p.counter(method).Add(1)

	r := p.ruleFor(method)
	if r.Delay > 0 {
		select {
		case <-time.After(r.Delay):
		case <-ctx.Done():
			return nil, &gwerrors.TimeoutError{Provider: p.name, Op: "call " + method, Cause: ctx.Err()}
		}
	}
	if r.Behavior == BehaviorTimeout {
		// Block beyond the deadline so the caller observes a timeout.
		<-ctx.Done()
		return nil, &gwerrors.TimeoutError{Provider: p.name, Op: "call " + method, Cause: ctx.Err()}
	}

	switch r.Behavior {
	case BehaviorOK:
		if r.Result == nil {
			return json.RawMessage(`"ok-from-` + p.name + `"`), nil
		}
		return r.Result, nil
	case BehaviorRPCError:
		code, msg := r.RPCCode, r.RPCMsg
		if code == 0 {
			code = -32000
		}
		if msg == "" {
			msg = "simulated json-rpc error"
		}
		return nil, &gwerrors.RPCObjectError{
			Provider: p.name,
			RPC:      &jsonrpc.Error{Code: code, Message: msg},
		}
	case BehaviorTransport:
		return nil, &gwerrors.TransportError{Provider: p.name, Cause: errSimulatedTransport}
	case BehaviorHTTP:
		status := r.Status
		if status == 0 {
			status = 503
		}
		return nil, &gwerrors.HTTPStatusError{Provider: p.name, Status: status, Body: "simulated http error"}
	case BehaviorInvalid:
		return nil, &gwerrors.InvalidResponseError{Provider: p.name, Detail: "simulated invalid response"}
	default:
		return nil, &gwerrors.TransportError{Provider: p.name, Cause: errSimulatedTransport}
	}
}
