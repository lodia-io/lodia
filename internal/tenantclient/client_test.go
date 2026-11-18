// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantclient

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestAControlPlaneKeepsItsTenantClientWhileItsKubeconfigStays(t *testing.T) {
	clients := NewClients(Options{Connections: 1, Timeout: time.Minute})
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	kubeconfig := func(server string) []byte {
		t.Helper()
		out, err := clientcmd.Write(clientcmdapi.Config{
			Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: server}},
			AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {Token: "token"}},
			Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
			CurrentContext: "tenant",
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	of := func(server string) *Client {
		t.Helper()
		c, err := clients.Of(key, kubeconfig(server))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	first := of("https://dev-kube-apiserver.team-a.svc:443")
	if of("https://dev-kube-apiserver.team-a.svc:443") != first {
		t.Error("the same kubeconfig built a second client")
	}
	second := of("https://dev-kube-apiserver.team-a.svc:6443")
	if second == first {
		t.Error("a new kubeconfig kept the previous client")
	}

	clients.Forget(key)
	if of("https://dev-kube-apiserver.team-a.svc:6443") == second {
		t.Error("a forgotten ControlPlane kept its client")
	}
}

func TestTheDialFunctionOfAClientGetsAContextThatEndsWithinTimeoutWhicheverDialFunctionItIs(t *testing.T) {
	const timeout = 7 * time.Second
	for _, c := range []struct {
		name   string
		parent func() (context.Context, context.CancelFunc)
		most   time.Duration
	}{
		{"net/http's context, with no deadline", func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) }, timeout},
		{"a context that ends sooner", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Second)
		}, time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			var left time.Duration
			var bounded bool
			opts := Options{Timeout: timeout, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				deadline, ok := ctx.Deadline()
				bounded, left = ok, time.Until(deadline)
				return nil, errors.New("no connection")
			}}
			ctx, cancel := c.parent()
			defer cancel()
			if _, err := opts.dialContext(ctx, "tcp", "tenant.example:6443"); err == nil {
				t.Fatal("the dial function that refuses returned a connection")
			}
			if !bounded || left <= 0 || left > c.most {
				t.Errorf("the dial function got a context that ends in %v, bounded %t, want one that ends in at most %v", left, bounded, c.most)
			}
		})
	}
}

func TestTheProductionDialerOfAClientIsBoundByTimeoutWhenNoDialFunctionIsSet(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	for _, c := range []struct {
		timeout  time.Duration
		connects bool
	}{
		{time.Nanosecond, false},
		{time.Minute, true},
	} {
		conn, err := Options{Timeout: c.timeout}.dialContext(context.Background(), "tcp", listener.Addr().String())
		if conn != nil {
			_ = conn.Close()
		}
		if (err == nil) != c.connects {
			t.Errorf("a dial of a Timeout of %v to a listener that answers ended in %v, want a connection: %t", c.timeout, err, c.connects)
		}
	}
}
