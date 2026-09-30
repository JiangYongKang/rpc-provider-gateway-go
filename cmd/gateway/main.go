// Command gateway runs the local JSON-RPC provider gateway.
//
// Usage:
//
//	gateway -config configs/config.json   # real providers from a config file
//	gateway -demo                          # in-memory fake providers, zero setup
//	gateway -demo -addr :8090
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/rpc-provider-gateway-go/internal/cache"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/config"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/gateway"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/provider"
	httpprovider "github.com/highcumontoa/rpc-provider-gateway-go/internal/provider/http"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/ratelimit"
	"github.com/highcumontoa/rpc-provider-gateway-go/internal/router"
)

func main() {
	var (
		configPath = flag.String("config", "", "path to JSON config file")
		demo       = flag.Bool("demo", false, "run with built-in local fake providers")
		addr       = flag.String("addr", "", "listen address (overrides config)")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "gateway ", log.LstdFlags|log.Lmicroseconds)

	var cfg *config.Config
	var providers []*httpprovider.HTTPProvider
	if *demo {
		cfg, providers = buildDemoConfig()
	} else {
		var err error
		if *configPath == "" {
			log.Fatal("provide -config <file> or use -demo")
		}
		cfg, err = config.Load(*configPath)
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
		for _, pc := range cfg.Providers {
			providers = append(providers, httpprovider.New(pc.Name, pc.URL, pc.Timeout.Duration))
		}
	}
	if *addr != "" {
		cfg.ListenAddr = *addr
	}

	// provider interface list
	pi := make([]provider.Provider, 0, len(providers))
	for _, p := range providers {
		pi = append(pi, p)
	}

	filter := buildMethodFilter(cfg)
	rtr := router.New(pi, cfg.DefaultTimeout.Duration, cfg.NonRetryableMethods, gateway.SupportedMethods(cfg), filter)
	c := cache.New()
	l := ratelimit.New(cfg.MaxConcurrent, cfg.RequestsPerSecond)
	gw := gateway.New(cfg, rtr, c, l, logger)

	log.Printf("listening on %s (%d providers, cache=%v ttl=%s, maxConcurrent=%d rps=%d)",
		cfg.ListenAddr, len(providers), cfg.Cache.Enabled, cfg.Cache.TTL.Duration,
		cfg.MaxConcurrent, cfg.RequestsPerSecond)
	if err := nethttp.ListenAndServe(cfg.ListenAddr, gw.Handler()); err != nil {
		log.Fatal(err)
	}
}

func buildMethodFilter(cfg *config.Config) map[string][]string {
	filter := map[string][]string{}
	for _, p := range cfg.Providers {
		for _, m := range p.Methods {
			filter[m] = append(filter[m], p.Name)
		}
	}
	if len(filter) == 0 {
		return nil
	}
	return filter
}

// demo fake providers -------------------------------------------------------

func buildDemoConfig() (*config.Config, []*httpprovider.HTTPProvider) {
	cfg := config.Default()
	cfg.ListenAddr = ":8080"

	var callCount atomic.Int64

	// Provider A: always fails with a JSON-RPC error for read methods and
	// responds to sendRawTransaction, so single-shot behavior can be observed.
	srvA := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		callCount.Add(1)
		body, _ := io.ReadAll(r.Body)
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.Unmarshal(body, &req)
		if req.Method == "eth_sendRawTransaction" {
			// Record the attempt count in the response so tests can prove it
			// was sent exactly once.
			writeRPC(w, req.ID, json.RawMessage(`"0xSENT_ONCE_`+itoa(callCount.Load())+`"`), nil)
			return
		}
		writeRPC(w, req.ID, nil, &rpcErr{Code: -32000, Message: "provider A: simulated upstream error"})
	}))

	// Provider B: times out every read call (very slow), send calls fail.
	srvB := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.Unmarshal(body, &req)
		if req.Method == "eth_sendRawTransaction" {
			writeRPC(w, req.ID, nil, &rpcErr{Code: -32000, Message: "provider B: send rejected"})
			return
		}
		time.Sleep(2 * time.Second) // exceeds the 400ms provider timeout below
		writeRPC(w, req.ID, json.RawMessage(`"B-late"`), nil)
	}))

	// Provider C: healthy, answers reads.
	srvC := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.Unmarshal(body, &req)
		writeRPC(w, req.ID, json.RawMessage(`"C-ok: `+jsonString(req.Method)+`"`), nil)
	}))

	cfg.Providers = []config.ProviderConfig{
		{Name: "A-failing", URL: srvA.URL, Timeout: config.Duration{Duration: 400 * time.Millisecond}, Healthy: true},
		{Name: "B-slow", URL: srvB.URL, Timeout: config.Duration{Duration: 400 * time.Millisecond}, Healthy: true},
		{Name: "C-healthy", URL: srvC.URL, Timeout: config.Duration{Duration: 400 * time.Millisecond}, Healthy: true},
	}
	ps := []*httpprovider.HTTPProvider{
		httpprovider.New("A-failing", srvA.URL, 400*time.Millisecond),
		httpprovider.New("B-slow", srvB.URL, 400*time.Millisecond),
		httpprovider.New("C-healthy", srvC.URL, 400*time.Millisecond),
	}
	return cfg, ps
}

type rpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func writeRPC(w nethttp.ResponseWriter, id json.RawMessage, result json.RawMessage, e *rpcErr) {
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(idOrNull(id))}
	if e != nil {
		resp["error"] = e
	} else {
		resp["result"] = result
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func idOrNull(id json.RawMessage) []byte {
	if len(id) == 0 {
		return []byte("null")
	}
	return id
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
