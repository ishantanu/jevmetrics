package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ishantanu/jevmetrics/internal/config"
	"github.com/ishantanu/jevmetrics/internal/evaluator"
	"github.com/ishantanu/jevmetrics/internal/exporter"
	promclient "github.com/ishantanu/jevmetrics/internal/prom"
)

func main() {
	cfgPath := os.Getenv("JEVMETRICS_CONFIG")
	if cfgPath == "" {
		cfgPath = "config.example.json"
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	client := promclient.NewClient(
		cfg.Prometheus.URL,
		cfg.Prometheus.TenantHeader,
		cfg.Prometheus.Tenant,
	)

	var eval evaluator.Evaluator
	switch cfg.Evaluator.Type {
	case "baseline":
		eval = evaluator.NewBaseline()
		log.Printf("using baseline evaluator")
	case "jev":
		apiKey := os.Getenv("TYPESAFE_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("JEV_API_KEY") // convenient alias
		}
		timeout, _ := time.ParseDuration(cfg.Evaluator.Timeout)
		jevEval, err := evaluator.NewJev(evaluator.JevConfig{
			APIKey:  apiKey,
			BaseURL: cfg.Evaluator.BaseURL,
			Model:   cfg.Evaluator.Model,
			Timeout: timeout,
		})
		if err != nil {
			log.Fatalf("configure Jev evaluator: %v", err)
		}
		if cfg.Evaluator.FallbackBaseline {
			eval = &evaluator.Fallback{Primary: jevEval, Fallback: evaluator.NewBaseline()}
			log.Printf("using Jev evaluator model=%s with baseline fallback", cfg.Evaluator.Model)
		} else {
			eval = jevEval
			log.Printf("using Jev evaluator model=%s", cfg.Evaluator.Model)
		}
	}

	exp := exporter.New(client, eval, cfg)

	mux := http.NewServeMux()
	mux.Handle("/metrics", exp)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	go exp.Run(ctx)

	go func() {
		log.Printf("jevmetrics listening on %s", cfg.Server.Listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}
