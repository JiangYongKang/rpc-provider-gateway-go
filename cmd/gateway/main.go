package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/highcumontoa/rpc-provider-gateway-go"
)

func main() {
	configPath := flag.String("config", "config.json", "path to config file")
	flag.Parse()

	cfg, err := gateway.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	logger := log.New(os.Stderr, "[gateway] ", log.LstdFlags|log.Lmicroseconds)

	gw, err := cfg.Build(logger)
	if err != nil {
		log.Fatalf("build gateway: %v", err)
	}
	srv := gateway.NewHTTPServer(gw, cfg.ListenAddr, cfg.Path)
	if err := srv.Start(); err != nil {
		log.Fatalf("start server: %v", err)
	}
	logger.Printf("JSON-RPC gateway listening on %s%s", srv.Addr(), cfg.Path)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Printf("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
