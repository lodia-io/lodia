// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package testetcd starts etcd's release binary at versions.EtcdVersion, the
// etcd that etcd-druid's etcd-wrapper runs in production, for the tests of
// Lodia's calls to etcd. It belongs to test files: no other package imports
// it.
//
// Members talk to each other over unix sockets in the cluster's directory,
// which no other process can take. A client URL is a loopback port, since etcd
// refuses an advertised URL that is not host:port
// (https://github.com/etcd-io/etcd/blob/v3.5.27/server/embed/config.go#L698-L701):
// it is picked before the member starts, and a member that finds it taken
// starts again on another.
package testetcd

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"go.lodia.io/lodia/internal/versions"
)

// BinaryEnv names the environment variable that holds the path of the etcd
// binary; make test sets it.
const BinaryEnv = "LODIA_ETCD_BINARY"

// readyLine is what a member logs once its server is ready, as ReadyNotify
// says, which only the process that holds the client port can print
// (https://github.com/etcd-io/etcd/blob/v3.5.27/server/embed/serve.go#L119-L124).
const readyLine = "ready to serve client requests"

const (
	readyWithin  = time.Minute
	stopWithin   = 10 * time.Second
	bindAttempts = 5
	logKept      = 1 << 20
	logShown     = 32 << 10
)

// Config sets what a member does beyond the defaults of etcd.
type Config struct {
	// QuotaBackendBytes is --quota-backend-bytes; zero leaves etcd's own.
	QuotaBackendBytes int64
	// AdvertiseClientURL is advertised in place of the member's client URL.
	AdvertiseClientURL string
	// NoStrictReconfigCheck is --strict-reconfig-check=false.
	NoStrictReconfigCheck bool
	// Name, PeerURL and Token are what the member bootstraps under in place
	// of m<i>, its socket and the test's name. It still listens on its
	// socket, which is enough for a member alone: no member dials its own
	// peer URL.
	Name, PeerURL, Token string
}

// Cluster is the members that share one directory and one token.
type Cluster struct {
	Members []*Member

	t       testing.TB
	all     []*Member
	dir     string
	token   string
	sockets atomic.Int32
	port    func() (string, error)
	wanted  string
}

// Member is one etcd process.
type Member struct {
	Name string
	// ClientURL is where the member listens, once it has started.
	ClientURL string
	// PeerURL is where the other members reach it, from Prepare on.
	PeerURL string

	listenPeer string
	cluster    *Cluster
	cfg        Config
	attempts   int

	mu       sync.Mutex
	proc     *process
	stopping bool
}

type process struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc
	log    *tail
	done   chan struct{}
	err    error
}

// Start starts one member that forms a cluster of its own.
func Start(t testing.TB, cfg Config) *Member {
	t.Helper()
	return StartCluster(t, 1, func(_ int, c *Config) { *c = cfg }).Members[0]
}

// StartCluster starts n members, m0 to m<n-1>, and returns when each is ready
// to serve clients. configure sets the Config of member i.
func StartCluster(t testing.TB, n int, configure func(i int, cfg *Config)) *Cluster {
	t.Helper()
	c := newCluster(t)
	for i := range n {
		var cfg Config
		if configure != nil {
			configure(i, &cfg)
		}
		c.Members = append(c.Members, c.Prepare("m"+strconv.Itoa(i), cfg))
	}
	c.boot(c.Members, "new", c.initialCluster(c.Members))
	return c
}

// Prepare names a member that has not started and gives it a peer URL, which
// MemberAdd takes. Join starts it.
func (c *Cluster) Prepare(name string, cfg Config) *Member {
	c.t.Helper()
	socket := "unix://127.0.0.1:" + strconv.Itoa(int(c.sockets.Add(1)))
	m := &Member{
		Name:       cmp.Or(cfg.Name, name),
		PeerURL:    cmp.Or(cfg.PeerURL, socket),
		listenPeer: socket,
		cluster:    c,
		cfg:        cfg,
	}
	c.all = append(c.all, m)
	return m
}

// Join starts a prepared member as a member of the cluster the others already
// form, and returns when it is ready to serve clients.
func (m *Member) Join() {
	c := m.cluster
	c.t.Helper()
	members := append(slices.Clone(c.Members), m)
	c.boot([]*Member{m}, "existing", c.initialCluster(members))
	c.Members = members
}

// Stop ends the member's process; stopping a stopped member does nothing.
func (m *Member) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopLocked()
}

func newCluster(t testing.TB) *Cluster {
	t.Helper()
	dir, err := os.MkdirTemp("", "etcd-")
	if err != nil {
		t.Fatal(err)
	}
	c := &Cluster{t: t, dir: dir, token: strings.ReplaceAll(t.Name(), "/", "-"), port: freePort, wanted: versions.EtcdVersion}
	t.Cleanup(func() { c.cleanup() })
	return c
}

func (c *Cluster) cleanup() {
	for _, m := range c.all {
		m.mu.Lock()
		if m.proc != nil && !m.stopping && !c.t.Failed() {
			select {
			case <-m.proc.done:
				if m.proc.err != nil {
					c.t.Errorf("etcd %s died before the test ended: %v", m.Name, m.proc.err)
				}
			default:
			}
		}
		m.stopLocked()
		if c.t.Failed() && m.proc != nil {
			c.t.Logf("etcd %s's log, last %d bytes:\n%s", m.Name, logShown, m.proc.log.last(logShown))
		}
		m.mu.Unlock()
	}
	_ = os.RemoveAll(c.dir)
}

func (c *Cluster) initialCluster(members []*Member) string {
	pairs := make([]string, len(members))
	for i, m := range members {
		pairs[i] = m.Name + "=" + m.PeerURL
	}
	return strings.Join(pairs, ",")
}

func (c *Cluster) binary() string {
	c.t.Helper()
	binary := os.Getenv(BinaryEnv)
	if binary == "" {
		c.t.Fatalf("%s is not set: make test fetches etcd %s and sets it, and `export %s=\"$(go run ./test/etcdbinary work/etcd)\"` does the same by hand", BinaryEnv, versions.EtcdVersion, BinaryEnv)
	}
	return binary
}

func (c *Cluster) boot(members []*Member, state, initial string) {
	c.t.Helper()
	binary := c.binary()
	for _, m := range members {
		m.launch(binary, state, initial)
	}
	pending := slices.Clone(members)
	deadline := time.Now().Add(readyWithin)
	for len(pending) > 0 {
		var waiting []*Member
		for _, m := range pending {
			switch err := m.check(); {
			case err == nil:
			case errors.Is(err, errPortTaken) && m.attempts < bindAttempts:
				m.launch(binary, state, initial)
				waiting = append(waiting, m)
			case errors.Is(err, errNotReady):
				waiting = append(waiting, m)
			default:
				c.t.Fatalf("etcd %s: %v\n%s", m.Name, err, m.logTail())
			}
		}
		pending = waiting
		if len(pending) > 0 {
			if time.Now().After(deadline) {
				c.t.Fatalf("etcd %s was not ready in %s\n%s", pending[0].Name, readyWithin, pending[0].logTail())
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
}

var (
	errNotReady  = errors.New("not ready")
	errPortTaken = errors.New("its client port is taken")
)

func (m *Member) launch(binary, state, initial string) {
	c := m.cluster
	c.t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopLocked()
	m.stopping = false
	m.attempts++
	port, err := c.port()
	if err != nil {
		c.t.Fatal(err)
	}
	m.ClientURL = "http://127.0.0.1:" + port
	data := filepath.Join(c.dir, m.Name)
	if err := os.RemoveAll(data); err != nil {
		c.t.Fatal(err)
	}
	advertise := m.ClientURL
	if m.cfg.AdvertiseClientURL != "" {
		advertise = m.cfg.AdvertiseClientURL
	}
	args := []string{
		"--name", m.Name,
		"--data-dir", data,
		"--unsafe-no-fsync",
		"--listen-client-urls", m.ClientURL,
		"--advertise-client-urls", advertise,
		"--listen-peer-urls", m.listenPeer,
		"--initial-advertise-peer-urls", m.PeerURL,
		"--initial-cluster", initial,
		"--initial-cluster-state", state,
		"--initial-cluster-token", cmp.Or(m.cfg.Token, c.token),
	}
	if m.cfg.QuotaBackendBytes > 0 {
		args = append(args, "--quota-backend-bytes", strconv.FormatInt(m.cfg.QuotaBackendBytes, 10))
	}
	if m.cfg.NoStrictReconfigCheck {
		args = append(args, "--strict-reconfig-check=false")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, binary, args...) //nolint:gosec // G204: the binary is the one BinaryEnv names
	cmd.Dir = c.dir
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = stopWithin
	endWithTheTestBinary(cmd)
	p := &process{cmd: cmd, cancel: cancel, log: &tail{}, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = p.log, p.log
	if err := cmd.Start(); err != nil {
		cancel()
		c.t.Fatalf("starting %s: %v", binary, err)
	}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	m.proc = p
}

func freePort() (string, error) {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer func() { _ = l.Close() }()
	_, port, err := net.SplitHostPort(l.Addr().String())
	return port, err
}

func (m *Member) check() error {
	m.mu.Lock()
	p := m.proc
	m.mu.Unlock()
	select {
	case <-p.done:
		if p.log.contains("address already in use") {
			return errPortTaken
		}
		if p.err == nil {
			return errors.New("exited with status 0")
		}
		return fmt.Errorf("exited: %w", p.err)
	default:
	}
	if !p.log.contains(readyLine) {
		return errNotReady
	}
	return m.version()
}

func (m *Member) version() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{m.ClientURL}, DialTimeout: 5 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		return errNotReady
	}
	defer func() { _ = c.Close() }()
	status, err := c.Status(ctx, m.ClientURL)
	if err != nil {
		return errNotReady
	}
	if want := strings.TrimPrefix(m.cluster.wanted, "v"); status.Version != want {
		return fmt.Errorf("%s is etcd %s, want %s", os.Getenv(BinaryEnv), status.Version, want)
	}
	return nil
}

func (m *Member) logTail() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.proc.log.last(logShown)
}

func (m *Member) stopLocked() {
	p := m.proc
	if p == nil || m.stopping {
		return
	}
	m.stopping = true
	p.cancel()
	<-p.done
}

type tail struct {
	mu  sync.Mutex
	buf []byte
}

func (l *tail) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, p...)
	if len(l.buf) > logKept {
		l.buf = bytes.Clone(l.buf[len(l.buf)-logKept/2:])
	}
	return len(p), nil
}

func (l *tail) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return bytes.Contains(l.buf, []byte(s))
}

func (l *tail) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return string(l.buf)
}

func (l *tail) last(n int) string {
	s := l.String()
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}
