// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.lodia.io/lodia/internal/certs"
)

const runMainEnv = "LODIA_TUNNEL_TEST_RUN_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(runMainEnv) != "" {
		main()
		return
	}
	os.Exit(m.Run())
}

func TestUsageExitsTwo(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no mode", nil, "usage"},
		{"an unknown mode", []string{"tunnel"}, "usage"},
		{"an unknown flag on probe", []string{"probe", "--nope"}, "-nope"},
		{"an unknown flag on serve", []string{"serve", "--nope"}, "-nope"},
		{"an unknown flag on addr", []string{"addr", "--nope"}, "-nope"},
		{"an unknown flag on proxy", []string{"proxy", "--nope"}, "-nope"},
		{"a stray argument", []string{"probe", "--socket=s", "stray"}, "stray"},
		{"serve without --hostname", []string{"serve", "--cert-dir=c", "--client-ca=a"}, "--hostname"},
		{"serve without --cert-dir", []string{"serve", "--hostname=h", "--client-ca=a"}, "--cert-dir"},
		{"serve without --client-ca", []string{"serve", "--hostname=h", "--cert-dir=c"}, "--client-ca"},
		{"addr without --address", []string{"addr"}, "--address"},
		{"addr without --address, checking", []string{"addr", "--check"}, "--address"},
		{"addr with no IP", []string{"addr", "--address=tunnel.lodia.test"}, "--address"},
		{"proxy without --listen", []string{"proxy", "--upstream=u:1", "--cert-dir=c", "--ca=a", "--socket=s"}, "--listen"},
		{"proxy without --upstream", []string{"proxy", "--listen=l:1", "--cert-dir=c", "--ca=a", "--socket=s"}, "--upstream"},
		{"proxy without --cert-dir", []string{"proxy", "--listen=l:1", "--upstream=u:1", "--ca=a", "--socket=s"}, "--cert-dir"},
		{"proxy without --ca", []string{"proxy", "--listen=l:1", "--upstream=u:1", "--cert-dir=c", "--socket=s"}, "--ca"},
		{"proxy without --socket", []string{"proxy", "--listen=l:1", "--upstream=u:1", "--cert-dir=c", "--ca=a"}, "--socket"},
		{"proxy with an upstream without a port", []string{"proxy", "--listen=l:1", "--upstream=u", "--cert-dir=c", "--ca=a", "--socket=s"}, "--upstream"},
		{"probe without --socket", []string{"probe"}, "--socket"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stderr strings.Builder
			if code := run(tc.args, &stderr); code != exitUsage {
				t.Fatalf("exit %d, want %d; %s", code, exitUsage, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr %q does not name %q", stderr.String(), tc.want)
			}
		})
	}
}

func TestHelpExitsZero(t *testing.T) {
	if code := run([]string{"serve", "-h"}, io.Discard); code != 0 {
		t.Fatalf("exit %d", code)
	}
}

func TestServeDefaultsToThePortsItsPodIsRenderedWith(t *testing.T) {
	var stderr strings.Builder
	if code := run([]string{"serve", "-h"}, &stderr); code != 0 {
		t.Fatalf("exit %d", code)
	}
	for name, value := range map[string]string{
		"listen":                    ":6444",
		"upstream":                  "127.0.0.1:6443",
		"health-probe-bind-address": ":8135",
	} {
		want := regexp.MustCompile("-" + name + ` string\n.*\(default "` + regexp.QuoteMeta(value) + `"\)`)
		if !want.MatchString(stderr.String()) {
			t.Errorf("--%s does not default to %s:\n%s", name, value, stderr.String())
		}
	}
}

func TestAddrCheckAnswersOnLinuxAndFailsElsewhere(t *testing.T) {
	onLo := exitFailed
	if runtime.GOOS == "linux" {
		onLo = 0
	}
	if code := run([]string{"addr", "--address=127.0.0.1", "--check"}, io.Discard); code != onLo {
		t.Fatalf("exit %d for an address on lo, want %d", code, onLo)
	}
	if code := run([]string{"addr", "--address=192.0.2.1", "--check"}, io.Discard); code != exitFailed {
		t.Fatalf("exit %d for an address not on lo", code)
	}
}

func TestProbeWithNoSocketExitsOne(t *testing.T) {
	if code := run([]string{"probe", "--socket=" + filepath.Join(t.TempDir(), "s")}, io.Discard); code != exitFailed {
		t.Fatalf("exit %d", code)
	}
}

func command(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], args...)
	cmd.Env = append(os.Environ(), runMainEnv+"=1")
	return cmd
}

func start(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := command(t, args...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("%v:\n%s", args, stderr.String())
		}
	})
	return cmd
}

func exitCode(t *testing.T, args ...string) int {
	t.Helper()
	err := command(t, args...).Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &exit):
		return exit.ExitCode()
	}
	t.Fatal(err)
	return -1
}

func writeDir(t *testing.T, files map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func pairDir(t *testing.T, leaf *certs.Certificate) string {
	t.Helper()
	cert, err := certs.CertPEM(leaf.Cert)
	if err != nil {
		t.Fatal(err)
	}
	key, err := certs.KeyPEM(leaf.Key)
	if err != nil {
		t.Fatal(err)
	}
	return writeDir(t, map[string][]byte{"tls.crt": cert, "tls.key": key})
}

func caDir(t *testing.T, ca *certs.Certificate) string {
	t.Helper()
	cert, err := certs.CertPEM(ca.Cert)
	if err != nil {
		t.Fatal(err)
	}
	return writeDir(t, map[string][]byte{"ca.crt": cert})
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestTheModesWorkUnderTheirFlags(t *testing.T) {
	cluster, err := certs.NewCA("cluster", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tunnelCA, err := certs.NewCA("tunnel", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	servingLeaf, err := cluster.Issue(certs.Profile{CommonName: "localhost", DNSNames: []string{"localhost"}, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	clientLeaf, err := tunnelCA.Issue(certs.Profile{CommonName: "lodia-tunnel-client", Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = upstream.Close() }()
	go func() {
		for {
			c, err := upstream.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				_, _ = io.Copy(c, c)
			}()
		}
	}()

	serveListen, health, proxyListen := freeAddr(t), freeAddr(t), freeAddr(t)
	_, servePort, err := net.SplitHostPort(serveListen)
	if err != nil {
		t.Fatal(err)
	}
	socketDir, err := os.MkdirTemp("/tmp", "lodia-tunnel-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "health.sock")

	if code := exitCode(t, "probe", "--socket="+socket); code != exitFailed {
		t.Fatalf("probe before proxy exits %d", code)
	}

	serve := start(t, "serve",
		"--listen="+serveListen, "--hostname=localhost", "--cert-dir="+pairDir(t, servingLeaf), "--client-ca="+caDir(t, tunnelCA),
		"--upstream="+upstream.Addr().String(), "--health-probe-bind-address="+health)
	eventually(t, "/healthz", func() bool {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+health+"/healthz", http.NoBody)
		if err != nil {
			return false
		}
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	})

	proxy := start(t, "proxy",
		"--listen="+proxyListen, "--upstream="+net.JoinHostPort("localhost", servePort), "--cert-dir="+pairDir(t, clientLeaf), "--ca="+caDir(t, cluster), "--socket="+socket)
	eventually(t, "probe", func() bool { return exitCode(t, "probe", "--socket="+socket) == 0 })

	echoed := func() error {
		c, err := net.DialTimeout("tcp", proxyListen, 10*time.Second)
		if err != nil {
			return err
		}
		defer func() { _ = c.Close() }()
		if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return err
		}
		if _, err := c.Write([]byte("ping")); err != nil {
			return err
		}
		got := make([]byte, 4)
		_, err = io.ReadFull(c, got)
		return err
	}
	eventually(t, "a byte through proxy and serve", func() bool { return echoed() == nil })

	refused := &tls.Config{ServerName: "localhost", RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS13}
	refused.RootCAs.AddCert(cluster.Cert)
	raw, err := net.DialTimeout("tcp", serveListen, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	c := tls.Client(raw, refused)
	if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("ping")); err == nil {
		_, err = c.Read(make([]byte, 1))
		if err == nil {
			t.Fatal("serve took a session with no client certificate")
		}
	}

	for name, cmd := range map[string]*exec.Cmd{"proxy": proxy, "serve": serve} {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		exited := make(chan error, 1)
		go func() { exited <- cmd.Wait() }()
		select {
		case err := <-exited:
			if err != nil {
				t.Fatalf("%s exited with %v on SIGTERM", name, err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("%s did not exit on SIGTERM", name)
		}
	}
	if code := exitCode(t, "probe", "--socket="+socket); code != exitFailed {
		t.Fatalf("probe after proxy exits %d", code)
	}
}
