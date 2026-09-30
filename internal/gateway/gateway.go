// Package gateway is the HTTP entry point. It parses JSON-RPC 2.0
// (single or batch), enforces concurrency/RPS quotas, validates method
// support, consults the read-only result cache and dispatches calls through
// the router. Request ids and responses are always matched one-to-one.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/cache"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/config"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/gwerrors"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/jsonrpc"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/provider"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/ratelimit"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/router"
)

// Logger logs per-request decisions (request id, chosen provider, reason).
type Logger interface {
	Printf(format string, args ...any)
}

// Gateway is the JSON-RPC HTTP gateway.
type Gateway struct {
	cfg       *config.Config
	router    *router.Router
	cache     *cache.Cache
	limiter   *ratelimit.Limiter
	logger    Logger
	cacheable map[string]bool
}

// SupportedMethods returns the gateway-level method allow-list: every method
// explicitly configured as idempotent, non-retryable or cacheable. An empty
// list means "allow everything".
func SupportedMethods(cfg *config.Config) []string {
	set := map[string]bool{}
	for _, m := range cfg.IdempotentMethods {
		set[m] = true
	}
	for _, m := range cfg.NonRetryableMethods {
		set[m] = true
	}
	for _, m := range cfg.Cache.Methods {
		set[m] = true
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	return out
}

// New builds a Gateway.
func New(cfg *config.Config, r *router.Router, c *cache.Cache, l *ratelimit.Limiter, logger Logger) *Gateway {
	g := &Gateway{
		cfg:       cfg,
		router:    r,
		cache:     c,
		limiter:   l,
		logger:    logger,
		cacheable: make(map[string]bool),
	}
	for _, m := range cfg.Cache.Methods {
		g.cacheable[m] = true
	}
	return g
}

// Handler returns the HTTP handler.
func (g *Gateway) Handler() http.Handler {
	return http.HandlerFunc(g.handle)
}

func (g *Gateway) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeJSON(w, http.StatusOK, jsonrpc.MakeError(nil, &jsonrpc.Error{
			Code: int(gwerrors.CodeJSONParseError), Message: "cannot read request body: " + err.Error(),
		}))
		return
	}

	reqs, batch, perr := jsonrpc.ParseRequests(body)
	if perr != nil {
		writeJSON(w, http.StatusOK, jsonrpc.MakeError(nil, &jsonrpc.Error{
			Code: int(gwerrors.CodeJSONParseError), Message: perr.Error(),
		}))
		return
	}

	if batch && len(reqs) > g.cfg.MaxBatchSize {
		writeJSON(w, http.StatusOK, jsonrpc.MakeError(nil, &jsonrpc.Error{
			Code:    int(gwerrors.CodeInvalidRequest),
			Message: "batch size exceeds configured maximum",
		}))
		return
	}

	if !batch {
		res := g.processOne(r.Context(), reqs[0])
		if res == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, res)
		return
	}

	// Batch: each request is processed independently; result positions match
	// request positions so ids can never be swapped. Notifications yield nil
	// and are omitted from the reply.
	results := make([]*jsonrpc.Response, len(reqs))
	var wg sync.WaitGroup
	for i, req := range reqs {
		wg.Add(1)
		go func(i int, req *jsonrpc.Request) {
			defer wg.Done()
			results[i] = g.processOne(r.Context(), req)
		}(i, req)
	}
	wg.Wait()

	out := make([]*jsonrpc.Response, 0, len(results))
	for _, res := range results {
		if res != nil {
			out = append(out, res)
		}
	}
	if len(out) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// processOne handles one request. It always returns a response carrying the
// same id; nil means the request was a notification.
func (g *Gateway) processOne(ctx context.Context, req *jsonrpc.Request) *jsonrpc.Response {
	if req.IsNotification() {
		g.logf(req, "", "notification: accepted without response")
		return nil
	}
	id := req.ID

	// 1) quota: reject explicitly with a distinguishable code.
	release, err := g.limiter.Acquire(ctx)
	if err != nil {
		gw := gwerrors.Normalize(err)
		if q, ok := ratelimit.IsRateLimit(err); ok {
			gw = gwerrors.New(gwerrors.CodeRateLimited, q)
			gw.Message = q.Error()
		}
		g.logf(req, "", "rejected: "+gw.Error())
		return jsonrpc.MakeError(id, gwerrors.ToRPCError(gw))
	}
	defer release()

	// 2) method support: distinguishable from upstream failures.
	if !g.router.ServesMethod(req.Method) {
		g.logf(req, "", "rejected: method not supported by any provider")
		return jsonrpc.MakeError(id, gwerrors.ToRPCError(gwerrors.ErrMethodNotFound))
	}

	// 3) read-only cache (send methods are never cached).
	cacheEnabled := g.cfg.Cache.Enabled && g.cacheable[req.Method] && !g.router.IsNonRetryable(req.Method)
	var cacheKey string
	if cacheEnabled {
		cacheKey = cache.Key(req.Method, req.Params)
		if v, ok := g.cache.Get(cacheKey); ok {
			g.logf(req, "", "cache HIT for "+req.Method)
			return jsonrpc.MakeResult(id, v)
		}
		g.logf(req, "", "cache MISS for "+req.Method)
	}

	// 4) dispatch (failover for read methods; single shot for send methods).
	result, trace, derr := g.router.Dispatch(ctx, req.Method, req.Params)
	for _, st := range trace {
		g.logf(req, st.Provider, st.Reason)
	}
	if derr != nil {
		gw := gwerrors.Normalize(derr)
		g.logf(req, lastProvider(trace), fmt.Sprintf("final: error code=%d (%s)", gw.Code, gw.Message))
		return jsonrpc.MakeError(id, gwerrors.ToRPCError(gw))
	}

	// 5) cache the fresh read-only result.
	if cacheEnabled {
		g.cache.Set(cacheKey, result, g.cfg.Cache.TTL.Duration)
	}

	g.logf(req, successfulProvider(trace), "final: success")
	return jsonrpc.MakeResult(id, result)
}

func lastProvider(trace []provider.Stat) string {
	if len(trace) == 0 {
		return ""
	}
	return trace[len(trace)-1].Provider
}

func successfulProvider(trace []provider.Stat) string {
	return lastProvider(trace)
}

func (g *Gateway) logf(req *jsonrpc.Request, providerName, msg string) {
	if g.logger == nil {
		return
	}
	id := "<notification>"
	method := ""
	if req != nil {
		if len(req.ID) > 0 {
			id = req.ID.String()
		}
		method = req.Method
	}
	if providerName == "" {
		providerName = "-"
	}
	g.logger.Printf("[id=%s method=%s provider=%s] %s", id, method, providerName, msg)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
