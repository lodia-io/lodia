// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	tlsCertFile = "tls.crt"
	tlsKeyFile  = "tls.key"
	caCertFile  = "ca.crt"
	dataLink    = "..data"

	reloadRetry = 5 * time.Second
)

// source is the parsed content of a mounted directory, which keeps its last
// good content when a later version does not load.
type source[T any] struct {
	dir   string
	files []string
	parse func(raw [][]byte) (*T, error)
	retry time.Duration
	log   *slog.Logger

	current atomic.Pointer[T]
	loaded  chan struct{}
	raw     [][]byte
}

func pairSource(dir string, retry time.Duration, log *slog.Logger) *source[tls.Certificate] {
	return &source[tls.Certificate]{
		dir: dir, files: []string{tlsCertFile, tlsKeyFile}, parse: parsePair, retry: retry, log: log, loaded: make(chan struct{}),
	}
}

func caSource(dir string, retry time.Duration, log *slog.Logger) *source[x509.CertPool] {
	return &source[x509.CertPool]{
		dir: dir, files: []string{caCertFile}, parse: parseCA, retry: retry, log: log, loaded: make(chan struct{}),
	}
}

func parsePair(raw [][]byte) (*tls.Certificate, error) {
	pair, err := tls.X509KeyPair(raw[0], raw[1])
	if err != nil {
		return nil, err
	}
	return &pair, nil
}

// parseCA refuses a bundle with anything it cannot parse, which
// AppendCertsFromPEM would skip: during a rotation that drops a CA in use.
func parseCA(raw [][]byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	rest := bytes.TrimSpace(raw[0])
	if len(rest) == 0 {
		return nil, errors.New("no certificates")
	}
	for len(rest) > 0 {
		block, tail := pem.Decode(rest)
		if block == nil {
			return nil, errors.New("not PEM")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		pool.AddCert(cert)
		rest = bytes.TrimSpace(tail)
	}
	return pool, nil
}

// watch loads the directory, then reloads it whenever it changes, until ctx is
// done. Without a watch or after a failed load it retries on a timer, since no
// event may follow: the kubelet's swap is over. A watch error reloads too, since
// the one inotify reports means it dropped events.
func (s *source[T]) watch(ctx context.Context) {
	s.watchWith(ctx, s.newWatcher)
}

func (s *source[T]) watchWith(ctx context.Context, newWatcher func(context.Context) *fsnotify.Watcher) {
	var w *fsnotify.Watcher
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()
	for {
		if w == nil {
			w = newWatcher(ctx)
		}
		ok := s.reload(ctx)

		var events chan fsnotify.Event
		var errs chan error
		var retry <-chan time.Time
		if w != nil {
			events, errs = w.Events, w.Errors
		}
		if w == nil || !ok {
			retry = time.After(s.retry)
		}
		select {
		case <-ctx.Done():
			return
		case <-events:
		case err := <-errs:
			s.log.WarnContext(ctx, "watch failed, reloading", "dir", s.dir, "error", err)
		case <-retry:
		}
	}
}

func (s *source[T]) newWatcher(ctx context.Context) *fsnotify.Watcher {
	w, err := fsnotify.NewWatcher()
	if err == nil {
		if err = w.Add(s.dir); err == nil {
			return w
		}
		_ = w.Close()
	}
	s.log.WarnContext(ctx, "not watching, retrying", "dir", s.dir, "error", err)
	return nil
}

// reload returns whether the directory's content is the one in use.
func (s *source[T]) reload(ctx context.Context) bool {
	raw, err := s.read()
	if err == nil && slices.EqualFunc(raw, s.raw, bytes.Equal) {
		return true
	}
	var next *T
	if err == nil {
		next, err = s.parse(raw)
	}
	if err != nil {
		s.log.ErrorContext(ctx, "not loaded, keeping the last good content", "dir", s.dir, "error", err)
		return false
	}
	first := s.current.Load() == nil
	s.raw = raw
	s.current.Store(next)
	s.log.InfoContext(ctx, "loaded", "dir", s.dir)
	if first {
		close(s.loaded)
	}
	return true
}

// read goes through one resolved ..data, the link the kubelet swaps, so a swap
// cannot mix two versions of the volume.
func (s *source[T]) read() ([][]byte, error) {
	dir := s.dir
	if resolved, err := filepath.EvalSymlinks(filepath.Join(dir, dataLink)); err == nil {
		dir = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	raw := make([][]byte, len(s.files))
	for i, name := range s.files {
		var err error
		if raw[i], err = os.ReadFile(filepath.Join(dir, name)); err != nil {
			return nil, err
		}
	}
	return raw, nil
}

func allLoaded(ctx context.Context, loaded ...<-chan struct{}) bool {
	for _, l := range loaded {
		select {
		case <-l:
		case <-ctx.Done():
			return false
		}
	}
	return true
}
