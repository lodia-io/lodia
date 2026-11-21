// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"go.lodia.io/lodia/internal/testetcd"
)

func runEtcd(t *testing.T, cfg *testetcd.Config) *kubernetes.Client {
	t.Helper()
	var config testetcd.Config
	if cfg != nil {
		config = *cfg
	}
	member := testetcd.Start(t, config)
	c, err := kubernetes.New(clientv3.Config{Endpoints: []string{member.ClientURL}, DialTimeout: 10 * time.Second,
		Logger: zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("etcd-client")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
