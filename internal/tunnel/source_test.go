// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"crypto/x509"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestAWatchErrorReloadsSinceEventsWereDropped(t *testing.T) {
	ca := newCA(t, "first")
	dir := t.TempDir()
	writeVolume(t, dir, caFiles(t, ca))
	log, logs := newLog(t)
	s := caSource(dir, time.Hour, log)

	failures := make(chan error)
	silent := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.watchWith(ctx, func(context.Context) *fsnotify.Watcher {
			w, err := fsnotify.NewWatcher()
			if err != nil {
				t.Error(err)
				return nil
			}
			if err := w.Add(silent); err != nil {
				t.Error(err)
			}
			w.Errors = failures
			return w
		})
	}()
	defer func() {
		cancel()
		<-done
	}()
	<-s.loaded
	first := s.current.Load()

	writeVolume(t, dir, caFiles(t, newCA(t, "second")))
	select {
	case failures <- errors.New("events dropped"):
	case <-time.After(waitFor):
		t.Fatal("the watch is not listening")
	}
	eventually(t, "the reload", func() bool { return s.current.Load() != first })
	if logs.count("watch failed") != 1 {
		t.Fatal("the watch error is not in the log")
	}
}

func TestAnUnchangedVolumeIsNotLoadedAgain(t *testing.T) {
	ca := newCA(t, "only")
	dir := t.TempDir()
	files := caFiles(t, ca)
	writeVolume(t, dir, files)
	log, _ := newLog(t)
	s := caSource(dir, time.Hour, log)
	s.reload(context.Background())
	first := s.current.Load()
	if first == nil {
		t.Fatal("nothing loaded")
	}
	writeVolume(t, dir, files)
	s.reload(context.Background())
	if s.current.Load() != first {
		t.Fatal("the same content was parsed and stored again")
	}
}

func TestItRetriesUntilLoadedWithoutAnEvent(t *testing.T) {
	dir := t.TempDir()
	writeVolume(t, dir, map[string][]byte{caCertFile: []byte("garbage")})
	log, _ := newLog(t)
	s := caSource(dir, 10*time.Millisecond, log)
	silent := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.watchWith(ctx, func(context.Context) *fsnotify.Watcher {
			w, err := fsnotify.NewWatcher()
			if err != nil {
				t.Error(err)
				return nil
			}
			if err := w.Add(silent); err != nil {
				t.Error(err)
			}
			return w
		})
	}()
	defer func() {
		cancel()
		<-done
	}()

	writeVolume(t, dir, caFiles(t, newCA(t, "good")))
	select {
	case <-s.loaded:
	case <-time.After(waitFor):
		t.Fatal("not loaded")
	}
}

func TestAFailedReloadIsRetriedWithoutAnEventUntilItLoads(t *testing.T) {
	dir := t.TempDir()
	writeVolume(t, dir, caFiles(t, newCA(t, "first")))
	log, logs := newLog(t)
	s := caSource(dir, 10*time.Millisecond, log)
	var failing atomic.Bool
	parse := s.parse
	s.parse = func(raw [][]byte) (*x509.CertPool, error) {
		if failing.Load() {
			return nil, syscall.EMFILE
		}
		return parse(raw)
	}
	events := make(chan fsnotify.Event)
	silent := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.watchWith(ctx, func(context.Context) *fsnotify.Watcher {
			w, err := fsnotify.NewWatcher()
			if err != nil {
				t.Error(err)
				return nil
			}
			if err := w.Add(silent); err != nil {
				t.Error(err)
			}
			w.Events = events
			return w
		})
	}()
	defer func() {
		cancel()
		<-done
	}()
	<-s.loaded
	first := s.current.Load()

	failing.Store(true)
	writeVolume(t, dir, caFiles(t, newCA(t, "second")))
	select {
	case events <- fsnotify.Event{}:
	case <-time.After(waitFor):
		t.Fatal("the watch is not listening")
	}
	eventually(t, "the failed reload", func() bool { return logs.count("not loaded") > 0 })
	if s.current.Load() != first {
		t.Fatal("a failed reload replaced the last good content")
	}
	failing.Store(false)
	eventually(t, "the retry", func() bool { return s.current.Load() != first })

	second := s.current.Load()
	writeVolume(t, dir, caFiles(t, newCA(t, "third")))
	time.Sleep(100 * time.Millisecond)
	if s.current.Load() != second {
		t.Fatal("still retrying after a good load, with a watch to wake it")
	}
}

func TestItReadsThroughDataNotTheLinksBesideIt(t *testing.T) {
	dir := t.TempDir()
	writeVolume(t, dir, caFiles(t, newCA(t, "good")))
	link := filepath.Join(dir, caCertFile)
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(link, []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	log, _ := newLog(t)
	s := caSource(dir, time.Hour, log)
	s.reload(context.Background())
	if s.current.Load() == nil {
		t.Fatal("read the file beside ..data")
	}
}

func TestAMissingWatchIsRetriedAfterItHasLoaded(t *testing.T) {
	dir := t.TempDir()
	writeVolume(t, dir, caFiles(t, newCA(t, "first")))
	log, _ := newLog(t)
	s := caSource(dir, 10*time.Millisecond, log)
	var attempts atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.watchWith(ctx, func(ctx context.Context) *fsnotify.Watcher {
			if attempts.Add(1) < 3 {
				return nil
			}
			return s.newWatcher(ctx)
		})
	}()
	defer func() {
		cancel()
		<-done
	}()
	<-s.loaded
	eventually(t, "a watch after the failed attempts", func() bool { return attempts.Load() == 3 })
	first := s.current.Load()
	writeVolume(t, dir, caFiles(t, newCA(t, "second")))
	eventually(t, "the reload", func() bool { return s.current.Load() != first })
}

func TestADirectoryThatAppearsLateIsWatchedOnceItExists(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "volume")
	log, _ := newLog(t)
	s := caSource(dir, 10*time.Millisecond, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.watch(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeVolume(t, dir, caFiles(t, newCA(t, "first")))
	<-s.loaded
	first := s.current.Load()
	eventually(t, "a reload through the watch", func() bool {
		if s.current.Load() != first {
			return true
		}
		writeVolume(t, dir, caFiles(t, newCA(t, "second")))
		return false
	})
}
