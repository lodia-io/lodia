// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command lodia-tunnel carries a tenant's in-cluster API traffic through its
// Gateway: serve beside each API server, addr and proxy on each node, and probe
// as proxy's health check.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"os/signal"
	"syscall"

	"go.lodia.io/lodia/internal/tunnel"
)

const usage = "usage: lodia-tunnel serve|addr|proxy|probe [flags]"

const (
	exitFailed = 1
	exitUsage  = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, usage)
		return exitUsage
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	switch args[0] {
	case "serve":
		return serve(ctx, args[1:], stderr, log)
	case "addr":
		return addr(ctx, args[1:], stderr, log)
	case "proxy":
		return proxy(ctx, args[1:], stderr, log)
	case "probe":
		return probe(ctx, args[1:], stderr, log)
	default:
		_, _ = fmt.Fprintln(stderr, usage)
		return exitUsage
	}
}

func serve(ctx context.Context, args []string, stderr io.Writer, log *slog.Logger) int {
	flags := newFlags("serve", stderr)
	listen := flags.String("listen", ":6444", "Address the outer TLS sessions arrive on.")
	hostname := flags.String("hostname", "", "The tunnel's host; any other server name is refused. Required.")
	certDir := flags.String("cert-dir", "", "Directory of the mounted serving pair: tls.crt and tls.key. Required.")
	clientCA := flags.String("client-ca", "", "Directory of the mounted CA of the tunnel's clients: ca.crt. Required.")
	upstream := flags.String("upstream", "127.0.0.1:6443", "Address of the API server each session is spliced to.")
	healthAddr := flags.String("health-probe-bind-address", ":8135", "Address of /healthz, which answers while the accept loop runs.")
	if code, done := parse(flags, args); done {
		return code
	}
	if *hostname == "" || *certDir == "" || *clientCA == "" {
		log.ErrorContext(ctx, "--hostname, --cert-dir and --client-ca are required")
		return exitUsage
	}
	return finish(log, tunnel.NewServer(*listen, *hostname, *certDir, *clientCA, *upstream, *healthAddr, log).Run(ctx))
}

func addr(ctx context.Context, args []string, stderr io.Writer, log *slog.Logger) int {
	flags := newFlags("addr", stderr)
	address := flags.String("address", "", "The IP address to hold on lo. Required.")
	check := flags.Bool("check", false, "Exit 0 if the address is on lo, and do nothing else.")
	if code, done := parse(flags, args); done {
		return code
	}
	ip, err := netip.ParseAddr(*address)
	if err != nil {
		log.ErrorContext(ctx, "--address must be an IP address", "error", err)
		return exitUsage
	}
	a := tunnel.NewAddress(ip, tunnel.NewKernel(), log)
	if *check {
		return finish(log, a.Check())
	}
	return finish(log, a.Run(ctx))
}

func proxy(ctx context.Context, args []string, stderr io.Writer, log *slog.Logger) int {
	flags := newFlags("proxy", stderr)
	listen := flags.String("listen", "", "The advertise address and port pods connect to. Required.")
	upstream := flags.String("upstream", "", "The tunnel's host:port. Required.")
	certDir := flags.String("cert-dir", "", "Directory of the mounted client pair: tls.crt and tls.key. Required.")
	ca := flags.String("ca", "", "Directory of the mounted CA of the upstream: ca.crt. Required.")
	socket := flags.String("socket", "", "Path of the Unix socket probe asks. Required.")
	if code, done := parse(flags, args); done {
		return code
	}
	if *listen == "" || *upstream == "" || *certDir == "" || *ca == "" || *socket == "" {
		log.ErrorContext(ctx, "--listen, --upstream, --cert-dir, --ca and --socket are required")
		return exitUsage
	}
	p, err := tunnel.NewProxy(*listen, *upstream, *certDir, *ca, *socket, log)
	if err != nil {
		log.ErrorContext(ctx, "--upstream must be host:port", "error", err)
		return exitUsage
	}
	return finish(log, p.Run(ctx))
}

func probe(ctx context.Context, args []string, stderr io.Writer, log *slog.Logger) int {
	flags := newFlags("probe", stderr)
	socket := flags.String("socket", "", "Path of the Unix socket proxy answers on. Required.")
	if code, done := parse(flags, args); done {
		return code
	}
	if *socket == "" {
		log.ErrorContext(ctx, "--socket is required")
		return exitUsage
	}
	return finish(log, tunnel.Probe(ctx, *socket))
}

func newFlags(mode string, stderr io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	flags.SetOutput(stderr)
	return flags
}

func parse(flags *flag.FlagSet, args []string) (code int, done bool) {
	switch err := flags.Parse(args); {
	case errors.Is(err, flag.ErrHelp):
		return 0, true
	case err != nil:
		return exitUsage, true
	case flags.NArg() > 0:
		_, _ = fmt.Fprintf(flags.Output(), "unexpected argument %q\n", flags.Arg(0))
		return exitUsage, true
	}
	return 0, false
}

func finish(log *slog.Logger, err error) int {
	if err != nil {
		log.Error("stopped", "error", err)
		return exitFailed
	}
	return 0
}
