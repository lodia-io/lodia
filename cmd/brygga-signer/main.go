// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Command brygga-signer signs the ServiceAccount tokens of the kube-apiserver
// in its pod, over a Unix socket, with the keys of the Secret <cp>-sa-keys.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/segla-io/brygga/internal/signer"
)

func main() {
	var socket, keysDir, probeAddr string
	var maxTokenExpiration, refreshHint time.Duration
	flag.StringVar(&socket, "socket", "", "Path of the Unix socket kube-apiserver dials through --service-account-signing-endpoint.")
	flag.StringVar(&keysDir, "keys-dir", "", "Directory of the mounted Secret <cp>-sa-keys: signing.key and verify-*.pub.")
	flag.DurationVar(&maxTokenExpiration, "max-token-expiration", 8760*time.Hour, "Longest token lifetime the signer accepts; kube-apiserver defaults --service-account-max-token-expiration to it.")
	flag.DurationVar(&refreshHint, "refresh-hint", time.Minute, "How often kube-apiserver re-fetches the verification keys.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address of the health and readiness probes.")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	switch {
	case socket == "" || keysDir == "":
		log.Error("--socket and --keys-dir are required")
		os.Exit(2)
	case maxTokenExpiration < 10*time.Minute:
		log.Error("--max-token-expiration must be at least 10m", "value", maxTokenExpiration)
		os.Exit(2)
	case refreshHint < time.Second:
		log.Error("--refresh-hint must be at least 1s", "value", refreshHint)
		os.Exit(2)
	}

	if err := run(socket, keysDir, probeAddr, maxTokenExpiration, refreshHint, log); err != nil {
		log.Error("stopped", "error", err)
		os.Exit(1)
	}
}

func run(socket, keysDir, probeAddr string, maxTokenExpiration, refreshHint time.Duration, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	s := signer.New(keysDir, maxTokenExpiration, refreshHint, log)
	lis, err := signer.Listen(socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socket, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.Ready() {
			http.Error(w, "no key set loaded", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	probes := &http.Server{Addr: probeAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return s.Watch(ctx) })
	g.Go(func() error { return s.Serve(ctx, lis) })
	g.Go(func() error {
		if err := probes.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		return probes.Shutdown(context.WithoutCancel(ctx))
	})
	log.InfoContext(ctx, "serving", "socket", socket, "keysDir", keysDir)
	return g.Wait()
}
