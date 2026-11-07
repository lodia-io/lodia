// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.lodia.io/lodia/internal/certs"
)

const (
	tunnelHost = "tunnel.lodia.test"
	waitFor    = 10 * time.Second
)

var loopbackIP = net.IPv4(127, 0, 0, 1)

func newCA(t *testing.T, name string) *certs.Certificate {
	t.Helper()
	ca, err := certs.NewCA(name, time.Now(), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func issue(t *testing.T, ca *certs.Certificate, p certs.Profile, issued time.Time) *certs.Certificate {
	t.Helper()
	leaf, err := ca.Issue(p, issued, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return leaf
}

func serving(t *testing.T, ca *certs.Certificate) *certs.Certificate {
	t.Helper()
	return issue(t, ca, certs.Profile{
		CommonName: tunnelHost, DNSNames: []string{tunnelHost}, IPAddresses: []net.IP{loopbackIP},
		Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, time.Now())
}

func client(t *testing.T, ca *certs.Certificate) *certs.Certificate {
	t.Helper()
	return issue(t, ca, certs.Profile{CommonName: "lodia-tunnel-client", Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, time.Now())
}

func expired(t *testing.T, ca *certs.Certificate, p certs.Profile) *certs.Certificate {
	t.Helper()
	return issue(t, ca, p, time.Now().Add(-2*time.Hour))
}

func pairFiles(t *testing.T, leaf *certs.Certificate) map[string][]byte {
	t.Helper()
	cert, err := certs.CertPEM(leaf.Cert)
	if err != nil {
		t.Fatal(err)
	}
	key, err := certs.KeyPEM(leaf.Key)
	if err != nil {
		t.Fatal(err)
	}
	return map[string][]byte{tlsCertFile: cert, tlsKeyFile: key}
}

func caFiles(t *testing.T, cas ...*certs.Certificate) map[string][]byte {
	t.Helper()
	var bundle []byte
	for _, ca := range cas {
		cert, err := certs.CertPEM(ca.Cert)
		if err != nil {
			t.Fatal(err)
		}
		bundle = append(bundle, cert...)
	}
	return map[string][]byte{caCertFile: bundle}
}

func tlsPair(t *testing.T, leaf *certs.Certificate) tls.Certificate {
	t.Helper()
	files := pairFiles(t, leaf)
	pair, err := tls.X509KeyPair(files[tlsCertFile], files[tlsKeyFile])
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

func roots(cas ...*certs.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, ca := range cas {
		pool.AddCert(ca.Cert)
	}
	return pool
}

func writeVolume(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	version, err := os.MkdirTemp(dir, "..version-")
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(version, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old, _ := os.Readlink(filepath.Join(dir, dataLink))
	swapData(t, dir, filepath.Base(version))
	for _, name := range []string{tlsCertFile, tlsKeyFile, caCertFile} {
		link := filepath.Join(dir, name)
		_, wanted := files[name]
		_, lerr := os.Lstat(link)
		switch {
		case wanted && lerr != nil:
			if err := os.Symlink(filepath.Join(dataLink, name), link); err != nil {
				t.Fatal(err)
			}
		case !wanted && lerr == nil:
			if err := os.Remove(link); err != nil {
				t.Fatal(err)
			}
		}
	}
	if old != "" && old != dataLink {
		if err := os.RemoveAll(filepath.Join(dir, old)); err != nil {
			t.Fatal(err)
		}
	}
}

func swapData(t *testing.T, dir, target string) {
	t.Helper()
	if err := os.Symlink(target, filepath.Join(dir, "..data_tmp")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "..data_tmp"), filepath.Join(dir, dataLink)); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
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

func socketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "lodia-tunnel-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "health.sock")
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuffer) count(line string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Count(l.buf.String(), line)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func newLog(t *testing.T) (*slog.Logger, *logBuffer) {
	t.Helper()
	buf := &logBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("process log:\n%s", buf)
		}
	})
	return slog.New(slog.NewTextHandler(buf, nil)), buf
}

func dialTLS(t *testing.T, addr string, cfg *tls.Config) (*tls.Conn, error) {
	t.Helper()
	raw, err := net.DialTimeout("tcp", addr, waitFor)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = raw.Close() })
	c := tls.Client(raw, cfg)
	if err := c.SetDeadline(time.Now().Add(waitFor)); err != nil {
		return nil, err
	}
	if err := c.Handshake(); err != nil {
		return nil, err
	}
	return c, nil
}

func roundTrip(c net.Conn) error {
	if err := c.SetDeadline(time.Now().Add(waitFor)); err != nil {
		return err
	}
	if _, err := c.Write([]byte("ping")); err != nil {
		return err
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(c, got); err != nil {
		return err
	}
	if string(got) != "ping" {
		return fmt.Errorf("echoed %q", got)
	}
	return nil
}

func dialTCP(t *testing.T, addr string) *net.TCPConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, waitFor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c.(*net.TCPConn)
}

func tripThrough(t *testing.T, addr string) error {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, waitFor)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	return roundTrip(c)
}

func requireClosed(t *testing.T, c net.Conn) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(waitFor)); err != nil {
		return
	}
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("read data, want the connection closed")
	} else if isTimeout(err) {
		t.Fatal("the connection stayed open")
	}
}

func requireOpen(t *testing.T, c net.Conn) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(make([]byte, 1)); !isTimeout(err) {
		t.Fatalf("read returned %v, want the connection still open", err)
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func discardLog() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func badPairVolumes(t *testing.T, ca *certs.Certificate) map[string]func(dir string) {
	t.Helper()
	good := pairFiles(t, serving(t, ca))
	other := pairFiles(t, serving(t, ca))
	return map[string]func(dir string){
		"a bad write": func(dir string) {
			writeVolume(t, dir, map[string][]byte{tlsCertFile: []byte("garbage"), tlsKeyFile: good[tlsKeyFile]})
		},
		"a half-written swap without its key": func(dir string) {
			writeVolume(t, dir, map[string][]byte{tlsCertFile: good[tlsCertFile]})
		},
		"a swap that mixes two versions": func(dir string) {
			writeVolume(t, dir, map[string][]byte{tlsCertFile: good[tlsCertFile], tlsKeyFile: other[tlsKeyFile]})
		},
		"a dangling ..data":   func(dir string) { danglingData(t, dir) },
		"a ..data that loops": func(dir string) { loopingData(t, dir) },
	}
}

func loopingData(t *testing.T, dir string) {
	t.Helper()
	swapData(t, dir, dataLink)
	touch(t, dir)
}

func danglingData(t *testing.T, dir string) {
	t.Helper()
	swapData(t, dir, "..gone")
	touch(t, dir)
}

func repairVolume(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	writeVolume(t, dir, files)
	touch(t, dir)
}

func touch(t *testing.T, dir string) {
	t.Helper()
	f, err := os.CreateTemp(dir, "..kqueue-sees-no-swap-of-a-dangling-link-")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func badCAVolumes(t *testing.T, ca *certs.Certificate) map[string]func(dir string) {
	t.Helper()
	good := caFiles(t, ca)[caCertFile]
	return map[string]func(dir string){
		"a bad write":   func(dir string) { writeVolume(t, dir, map[string][]byte{caCertFile: []byte("garbage")}) },
		"an empty file": func(dir string) { writeVolume(t, dir, map[string][]byte{caCertFile: {}}) },
		"a bundle cut in half": func(dir string) {
			writeVolume(t, dir, map[string][]byte{caCertFile: append(bytes.Clone(good), good[:len(good)/2]...)})
		},
		"a bundle with a key in it": func(dir string) {
			writeVolume(t, dir, map[string][]byte{caCertFile: append(bytes.Clone(good), pairFiles(t, client(t, ca))[tlsKeyFile]...)})
		},
		"a half-written swap without its file": func(dir string) { writeVolume(t, dir, map[string][]byte{"other": good}) },
		"a dangling ..data":                    func(dir string) { danglingData(t, dir) },
		"a ..data that loops":                  func(dir string) { loopingData(t, dir) },
	}
}

func reset(t *testing.T, c *net.TCPConn) {
	t.Helper()
	if err := c.SetLinger(0); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
