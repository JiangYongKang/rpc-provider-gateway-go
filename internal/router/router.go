// Package router routes JSON-RPC calls to providers with failover.
//
// Failover policy:
//   - Providers are tried in configured order; unavailable providers are
//     skipped before any network call.
//   - Idempotent (read) methods fail over to the next provider on timeout,
//     transport error, non-2xx HTTP status, invalid response or JSON-RPC
//     error.
//   - Non-idempotent (send) methods are sent to exactly ONE provider. If that
//     single attempt fails for any reason, the failure is reported as-is and
//     NOT retried, so the caller is never unsure how many times the request
//     was sent.
package router

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/gwerrors"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/provider"
)

// Router selects providers and performs failover.
type Router struct {
	providers    []provider.Provider
	timeout      time.Duration
	nonRetryable map[string]bool
	supported    map[string]bool // when non-nil, only these methods are served
	// methodProviders maps a method to the subset of providers that serve it;
	// a missing/empty entry means "all providers".
	methodProviders map[string][]provider.Provider
}

// New builds a Router.
//
// providers is the ordered candidate list. nonRetryableMethods are methods
// that must be sent at most once. supportedMethods, when non-empty, is the
// gateway-level allow-list (methods outside it get method-not-supported).
// methodFilter optionally maps a method name to the provider names that serve
// it.
func New(providers []provider.Provider, timeout time.Duration, nonRetryableMethods, supportedMethods []string, methodFilter map[string][]string) *Router {
	r := &Router{
		providers:       providers,
		timeout:         timeout,
		nonRetryable:    make(map[string]bool, len(nonRetryableMethods)),
		methodProviders: make(map[string][]provider.Provider),
	}
	for _, m := range nonRetryableMethods {
		r.nonRetryable[m] = true
	}
	if len(supportedMethods) > 0 {
		r.supported = make(map[string]bool, len(supportedMethods))
		for _, m := range supportedMethods {
			r.supported[m] = true
		}
	}
	byName := make(map[string]provider.Provider, len(providers))
	for _, p := range providers {
		byName[p.Name()] = p
	}
	for method, names := range methodFilter {
		for _, name := range names {
			if p, ok := byName[name]; ok {
				r.methodProviders[method] = append(r.methodProviders[method], p)
			}
		}
	}
	return r
}

// IsNonRetryable reports whether a method is classified non-idempotent.
func (r *Router) IsNonRetryable(method string) bool { return r.nonRetryable[method] }

// ServesMethod reports whether the method is on the gateway allow-list (when
// configured) and at least one provider serves it.
func (r *Router) ServesMethod(method string) bool {
	if r.supported != nil && !r.supported[method] {
		return false
	}
	if len(r.methodProviders) == 0 {
		for _, p := range r.providers {
			if p.Available() {
				return true
			}
		}
		return false
	}
	for _, p := range r.methodProviders[method] {
		if p.Available() {
			return true
		}
	}
	return false
}

// candidates returns the ordered providers that serve the method.
func (r *Router) candidates(method string) []provider.Provider {
	if len(r.methodProviders) == 0 {
		return r.providers // no filtering configured: every provider serves everything
	}
	return r.methodProviders[method] // explicit filter; empty means unsupported
}

// Dispatch sends the call with the failover policy. It returns the first
// successful result, the ordered trace of selection decisions and, on total
// failure, a normalized *gwerrors.GatewayError.
func (r *Router) Dispatch(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, []provider.Stat, error) {
	candidates := r.candidates(method)
	trace := make([]provider.Stat, 0, len(candidates))
	nonRetryable := r.nonRetryable[method]

	var lastErr error
	attempted := 0

	for idx, p := range candidates {
		if !p.Available() {
			trace = append(trace, provider.Stat{
				Provider: p.Name(), Attempt: idx + 1,
				Reason: "skipped: provider marked unavailable",
			})
			continue
		}

		// Non-idempotent call: choose the first available provider and send
		// exactly once. No failover, ever.
		if nonRetryable && attempted > 0 {
			trace = append(trace, provider.Stat{
				Provider: p.Name(), Attempt: idx + 1,
				Reason: "not attempted: non-idempotent method is sent at most once",
			})
			continue
		}

		attempted++
		callCtx := ctx
		var cancel context.CancelFunc
		if r.timeout > 0 {
			callCtx, cancel = context.WithTimeout(ctx, r.timeout)
		}
		reason := "selected: first available provider for method"
		if attempted > 1 {
			reason = "failover: previous provider failed, trying next available provider"
		}
		trace = append(trace, provider.Stat{Provider: p.Name(), Attempt: idx + 1, Reason: reason})

		result, err := p.Call(callCtx, method, params)
		if cancel != nil {
			cancel()
		}
		if err == nil {
			trace[len(trace)-1].Reason = reason + " -> success"
			return result, trace, nil
		}

		normalized := gwerrors.Normalize(err)
		lastErr = normalized

		if nonRetryable {
			// Report the single failure honestly. Do not try another provider.
			gw := gwerrors.New(gwerrors.CodeRetryNotAllowed, fmt.Errorf(
				"single attempt to %q failed (%s); not retrying non-idempotent method",
				p.Name(), categoryOf(normalized.Code)))
			gw.Provider = p.Name()
			gw.Upstream = normalized.Upstream
			trace[len(trace)-1].Reason = fmt.Sprintf(
				"failed once: %s; non-idempotent method is sent at most once, returning failure without retry", normalized.Message)
			return nil, trace, gw
		}

		trace[len(trace)-1].Reason = fmt.Sprintf("failed: %s (%d); continuing failover", normalized.Message, normalized.Code)
		// continue to next candidate
	}

	if attempted == 0 {
		return nil, trace, gwerrors.ErrAllProvidersDown
	}
	gw := gwerrors.New(gwerrors.CodeAllProvidersDown, lastErr)
	if ge, ok := lastErr.(*gwerrors.GatewayError); ok {
		gw.Message = fmt.Sprintf("all providers failed; last provider %q: %s", ge.Provider, ge.Message)
	}
	return nil, trace, gw
}

func categoryOf(c gwerrors.Code) string {
	return gwerrors.DefaultMessage(c)
}
