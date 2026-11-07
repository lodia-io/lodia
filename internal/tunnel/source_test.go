// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"errors"
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
