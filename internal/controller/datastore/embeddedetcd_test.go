// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

var peerSockets atomic.Int32

func etcdConfig(t *testing.T) *embed.Config {
	t.Helper()
	sockets, err := os.MkdirTemp("", "etcd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockets) })
	cfg := embed.NewConfig()
	cfg.UnsafeNoFsync = true
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("etcd-server"))
	cfg.Dir = t.TempDir()
	clientSocket := url.URL{Scheme: "unix", Path: filepath.Join(sockets, "client")}
	cfg.ListenClientUrls = []url.URL{clientSocket}
	cfg.AdvertiseClientUrls = []url.URL{clientSocket}
	peer := url.URL{Scheme: "http", Host: "127.0.0.1:0"}
	cfg.ListenPeerUrls = []url.URL{peer}
	cfg.AdvertisePeerUrls = []url.URL{peer}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	return cfg
}

func socketPeer(cfg *embed.Config) {
	peer := url.URL{Scheme: "unix", Host: fmt.Sprintf("127.0.0.1:%d%05d", os.Getpid(), peerSockets.Add(1))}
	cfg.ListenPeerUrls = []url.URL{peer}
	cfg.AdvertisePeerUrls = []url.URL{peer}
}

func runEtcd(t *testing.T, cfg *embed.Config) *kubernetes.Client {
	t.Helper()
	if cfg == nil {
		cfg = etcdConfig(t)
	}
	startMembers(t, cfg)
	c, err := kubernetes.New(clientv3.Config{Endpoints: []string{clientURL(cfg)}, DialTimeout: 10 * time.Second,
		Logger: zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("etcd-client")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
