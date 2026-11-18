// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func TestAWebhookActsOutsideWhenItCallsAURLOrFailsClosed(t *testing.T) {
	service := `"clientConfig":{"service":{"namespace":"shop","name":"hooks"}}`
	for _, row := range []struct {
		name    string
		webhook string
		want    bool
	}{
		{"a Service that fails open", `{` + service + `,"failurePolicy":"Ignore"}`, false},
		{"a Service that fails closed", `{` + service + `,"failurePolicy":"Fail"}`, true},
		{"a Service without a failure policy", `{` + service + `}`, true},
		{"a URL that fails open", `{"clientConfig":{"url":"https://hooks.example.com/validate"},"failurePolicy":"Ignore"}`, true},
		{"a null URL that fails open", `{"clientConfig":{"url":null,"service":{"name":"hooks"}},"failurePolicy":"Ignore"}`, false},
	} {
		w, err := webhookOf([]byte(row.webhook))
		if err != nil {
			t.Fatal(err)
		}
		if got := w.actsOutside(); got != row.want {
			t.Errorf("%s acts outside: %t, want %t", row.name, got, row.want)
		}
	}
}

func TestEveryKindOfAHeldRestoreSaysOrMoreWhenItsPageHasANextAndOnlyThen(t *testing.T) {
	sizedForTheBounds(t)
	const (
		nothing = "0 LoadBalancer Services; 0 PersistentVolumes with reclaimPolicy Delete; 0 pending PersistentVolumeClaims" +
			"; 0 webhooks that call a URL or fail closed; 0 APIServices served by a Service; 0 CronJobs not suspended"
		configuration = `{"metadata":{"name":"c"},"webhooks":[{"name":"w"}]}`
		webhooks      = "webhooks that call a URL or fail closed"
	)
	for _, c := range []struct{ path, item, kind, name string }{
		{servicesPath, `{"metadata":{"namespace":"n","name":"s"},"spec":{"type":"LoadBalancer"}}`, "LoadBalancer Services", "n/s"},
		{volumesPath, `{"metadata":{"name":"v"},"spec":{"persistentVolumeReclaimPolicy":"Delete"}}`, "PersistentVolumes with reclaimPolicy Delete", "v"},
		{claimsPath, `{"metadata":{"namespace":"n","name":"c"},"status":{"phase":"Pending"}}`, "pending PersistentVolumeClaims", "n/c"},
		{validatingPath, configuration, webhooks, "c/w"},
		{mutatingPath, configuration, webhooks, "c/w"},
		{apiServicePath, `{"metadata":{"name":"a"},"spec":{"service":{}}}`, "APIServices served by a Service", "a"},
		{cronJobsPath, `{"metadata":{"namespace":"n","name":"j"},"spec":{}}`, "CronJobs not suspended", "n/j"},
	} {
		for _, page := range []struct{ name, next, count, ellipsis string }{
			{"a last page", "", "1", ""},
			{"a page with a next", "next", "1 or more", "…"},
		} {
			t.Run(path.Base(c.path)+", "+page.name, func(t *testing.T) {
				body := `{"metadata":{"continue":"` + page.next + `"},"items":[` + c.item + `]}`
				tenant := servingPaths(t, map[string][]byte{c.path: []byte(body)})
				want := strings.Replace(nothing, "0 "+c.kind, page.count+" "+c.kind+" ("+c.name+page.ellipsis+")", 1)
				if got := (&Reconciler{Timeout: time.Minute}).actedOn(t.Context(), tenant); got != want {
					t.Errorf("the message says %q, want %q", got, want)
				}
			})
		}
	}
}

type requestConn struct {
	net.Conn
	inFlight, most *atomic.Int32
	state          atomic.Int32
}

func (c *requestConn) Write(b []byte) (int, error) {
	if c.state.CompareAndSwap(0, 1) {
		n := c.inFlight.Add(1)
		for m := c.most.Load(); n > m && !c.most.CompareAndSwap(m, n); m = c.most.Load() {
		}
	}
	return c.Conn.Write(b)
}

func (c *requestConn) Close() error {
	if c.state.Swap(2) == 1 {
		c.inFlight.Add(-1)
	}
	return c.Conn.Close()
}

func TestHeldRestoresListAtMostHeldRestoresAtOnceWhateverTheWorkers(t *testing.T) {
	var inFlight, most atomic.Int32
	hang := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		<-req.Context().Done()
	})
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &requestConn{Conn: conn, inFlight: &inFlight, most: &most}, nil
	}
	const timeout = 500 * time.Millisecond
	r := &Reconciler{Timeout: timeout}
	clients := &Reconciler{Timeout: 10 * time.Minute, DialContext: dial}
	var passes sync.WaitGroup
	took := make([]time.Duration, 2*heldRestores)
	said := make([]string, len(took))
	for i := range took {
		tenant, err := clients.tenant(types.NamespacedName{Namespace: "team-a", Name: strconv.Itoa(i)}, serveTenant(t, hang))
		if err != nil {
			t.Fatal(err)
		}
		passes.Go(func() {
			start := time.Now()
			said[i] = r.actedOn(t.Context(), tenant)
			took[i] = time.Since(start)
		})
	}
	passes.Wait()
	if got, want := most.Load(), int32(heldRestores*restoreReads); got != want {
		t.Errorf("%d passes over held restores had %d lists in flight at once, want %d, those of %d passes", len(took), got, want, heldRestores)
	}
	for i := range took {
		if took[i] > timeout+300*time.Millisecond || strings.Count(said[i], "could not be listed") != 6 {
			t.Errorf("a pass took %s and said %q, want every kind not listed within its Timeout", took[i], said[i])
		}
	}
}
