// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"encoding/pem"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"go.lodia.io/lodia/internal/tenantclient"
)

const (
	coreConfigMaps = `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[{"metadata":{},"versions":[{"version":"v1","resources":[{"resource":"configmaps","scope":"Namespaced","verbs":["get","list"]}]}]}]}`
	noGroups       = `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[]}`
	emptyList      = `{"kind":"List","metadata":{},"items":[]}`
	configMapsPath = "/api/v1/configmaps"
)

type answer func(w http.ResponseWriter, r *http.Request)

type side struct {
	mu      sync.Mutex
	answers map[string][]answer
	reads   []string
}

func (s *side) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.reads = append(s.reads, r.URL.RequestURI())
	queue := s.answers[r.URL.Path]
	var next answer
	switch {
	case len(queue) > 1:
		next, s.answers[r.URL.Path] = queue[0], queue[1:]
	case len(queue) == 1:
		next = queue[0]
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if next == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	next(w, r)
}

func (s *side) read() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.reads)
}

func stub(t *testing.T, answers map[string][]answer) (*tenantclient.Client, *side) {
	t.Helper()
	s := &side{answers: map[string][]answer{
		"/api":  {body(coreConfigMaps)},
		"/apis": {body(noGroups)},
		"/apis/apiregistration.k8s.io/v1/apiservices":             {body(emptyList)},
		"/apis/apiextensions.k8s.io/v1/customresourcedefinitions": {body(emptyList)},
		configMapsPath: {rows(2, "")},
	}}
	maps.Copy(s.answers, answers)
	server := httptest.NewTLSServer(http.HandlerFunc(s.serveHTTP))
	t.Cleanup(server.Close)
	cfg := &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	c, err := tenantclient.New(cfg, tenantclient.Options{Connections: 1, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, s
}

func body(b string) answer {
	return func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(b)) }
}

func rows(n int, next string) answer {
	return func(w http.ResponseWriter, _ *http.Request) {
		cells := strings.TrimSuffix(strings.Repeat(`{"cells":["a"]},`, n), ",")
		_, _ = w.Write([]byte(`{"kind":"Table","apiVersion":"meta.k8s.io/v1","metadata":{"continue":"` + next + `"},"columnDefinitions":[],"rows":[` + cells + `]}`))
	}
}

func status(code int, reason, message string) answer {
	return func(w http.ResponseWriter, _ *http.Request) {
		if code == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "1")
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"` + message + `","reason":"` + reason + `","code":` + strconv.Itoa(code) + `}`))
	}
}

func dropped(w http.ResponseWriter, _ *http.Request) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

var undecodable = status(http.StatusInternalServerError, "InternalError", "proto: illegal wireType 6")

func transients() map[string][]answer {
	return map[string][]answer{
		"429":           {status(http.StatusTooManyRequests, "TooManyRequests", "too many requests")},
		"503":           {status(http.StatusServiceUnavailable, "ServiceUnavailable", "apiserver is shutting down")},
		"504":           {status(http.StatusGatewayTimeout, "Timeout", "request did not complete within the allotted timeout")},
		"ServerTimeout": {status(http.StatusInternalServerError, "ServerTimeout", "the server cannot complete the requested operation at this time, try again later")},
		"no answer":     {dropped, dropped},
	}
}

func sweptAll(t *testing.T, s *sweep) (again int) {
	t.Helper()
	for range 20 {
		switch err := s.run(t.Context()); {
		case err == nil:
			return again
		case errors.Is(err, errAgain):
			again++
		default:
			t.Fatal(err)
		}
	}
	t.Fatal("the sweep did not end")
	return again
}

func TestATransientErrorThroughTheCopyIsReadAgain(t *testing.T) {
	for name, once := range transients() {
		t.Run(name, func(t *testing.T) {
			copyClient, _ := stub(t, map[string][]answer{configMapsPath: append(once, rows(2, ""))})
			tenant, _ := stub(t, nil)
			s := &sweep{copy: copyClient, tenant: tenant}
			if again := sweptAll(t, s); again != 1 || len(s.stopping) > 0 || len(s.failed) > 0 || s.objects != 2 {
				t.Errorf("the sweep sent %d reads again and ends with %d objects, failed %v, stopping %q, want 1, 2 and nothing", again, s.objects, s.failed, s.stopping)
			}
		})
	}
}

func TestATransientErrorThroughTheCopysDiscoveryIsReadAgain(t *testing.T) {
	copyClient, _ := stub(t, map[string][]answer{"/api": {status(http.StatusServiceUnavailable, "ServiceUnavailable", "apiserver is shutting down"), body(coreConfigMaps)}})
	tenant, _ := stub(t, nil)
	s := &sweep{copy: copyClient, tenant: tenant}
	if again := sweptAll(t, s); again != 1 || len(s.stopping) > 0 || s.objects != 2 {
		t.Errorf("the sweep sent %d reads again and ends with %d objects, stopping %q, want 1, 2 and nothing", again, s.objects, s.stopping)
	}
}

func TestAnErrorThroughTheCopyThatIsNotTransientIsAFinding(t *testing.T) {
	copyClient, _ := stub(t, map[string][]answer{configMapsPath: {undecodable}})
	tenant, _ := stub(t, nil)
	s := &sweep{copy: copyClient, tenant: tenant}
	if again := sweptAll(t, s); again != 0 || !slices.Equal(s.stopping, []string{"configmaps of v1 lists through the tenant and not through the copy: proto: illegal wireType 6"}) {
		t.Errorf("the sweep sent %d reads again and stops on %q", again, s.stopping)
	}
}

func TestATransientErrorThroughTheTenantIsReadAgainAndTheCopysFailureStops(t *testing.T) {
	for name, once := range transients() {
		t.Run(name, func(t *testing.T) {
			copyClient, _ := stub(t, map[string][]answer{configMapsPath: {undecodable}})
			tenant, _ := stub(t, map[string][]answer{configMapsPath: append(once, rows(2, ""))})
			s := &sweep{copy: copyClient, tenant: tenant}
			if again := sweptAll(t, s); again != 1 || len(s.reported) > 0 || !slices.Equal(s.stopping, []string{"configmaps of v1 lists through the tenant and not through the copy: proto: illegal wireType 6"}) {
				t.Errorf("the sweep sent %d reads again, stops on %q and reports %q, want 1 and the copy's failure stopping", again, s.stopping, s.reported)
			}
		})
	}
}

func TestATenantsExpiredTokenListsItAgainFromItsStart(t *testing.T) {
	copyClient, _ := stub(t, map[string][]answer{configMapsPath: {undecodable}})
	tenant, reads := stub(t, map[string][]answer{configMapsPath: {rows(500, "t1"), status(http.StatusGone, "Expired", "the provided continue parameter is too old"), rows(500, "t2"), rows(1, "")}})
	s := &sweep{copy: copyClient, tenant: tenant}
	sweptAll(t, s)
	if len(s.reported) > 0 || len(s.stopping) != 1 {
		t.Errorf("the sweep stops on %q and reports %q, want the copy's failure stopping", s.stopping, s.reported)
	}
	var lists []string
	for _, read := range reads.read() {
		if strings.HasPrefix(read, configMapsPath) {
			lists = append(lists, read)
		}
	}
	if len(lists) != 4 || !strings.Contains(lists[1], "continue=t1") || strings.Contains(lists[2], "continue=") || !strings.Contains(lists[3], "continue=t2") {
		t.Errorf("the tenant's lists are %q, want a first page, the expired token, then the list again from its start", lists)
	}
}

func TestAResourceThatFailsThroughBothIsReportedWithEachSidesError(t *testing.T) {
	copyClient, _ := stub(t, map[string][]answer{configMapsPath: {undecodable}})
	tenant, _ := stub(t, map[string][]answer{configMapsPath: {status(http.StatusInternalServerError, "InternalError", "illegal base64 data at input byte 4")}})
	s := &sweep{copy: copyClient, tenant: tenant}
	sweptAll(t, s)
	want := "configmaps of v1 fails through the tenant too: illegal base64 data at input byte 4; through the copy: proto: illegal wireType 6"
	if len(s.stopping) > 0 || !slices.Equal(s.reported, []string{want}) {
		t.Errorf("the sweep stops on %q and reports %q, want %q reported", s.stopping, s.reported, want)
	}
}

func TestAReadThatStillFailsStopsTheStepOnlyWithAnAnswer(t *testing.T) {
	for _, c := range []struct {
		name         string
		copy, tenant []answer
		what         string
		stops        bool
	}{
		{"a list through the copy answered 503", []answer{status(http.StatusServiceUnavailable, "ServiceUnavailable", "injected")}, nil, "the list of configmaps of v1", true},
		{"a list through the copy without an answer", []answer{dropped}, nil, "the list of configmaps of v1", false},
		{"the tenant's list of a resource the copy failed", []answer{undecodable}, []answer{status(http.StatusTooManyRequests, "TooManyRequests", "injected")}, "the tenant's list of configmaps of v1 (through the copy: proto: illegal wireType 6)", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			copyClient, _ := stub(t, map[string][]answer{configMapsPath: c.copy})
			answers := map[string][]answer{}
			if c.tenant != nil {
				answers[configMapsPath] = c.tenant
			}
			tenant, _ := stub(t, answers)
			s := &sweep{copy: copyClient, tenant: tenant}
			for range 3 {
				if err := s.run(t.Context()); !errors.Is(err, errAgain) {
					t.Fatalf("the pass ends with %v, want the read sent again", err)
				}
			}
			if s.failing == nil || s.failing.what != c.what || s.failing.stops != c.stops {
				t.Errorf("the sweep sends again %+v, want %q stopping the step at the deadline: %t", s.failing, c.what, c.stops)
			}
		})
	}
}

func TestASweepGoesOnWhereThePassBudgetEndedIt(t *testing.T) {
	pass, end := context.WithCancel(t.Context())
	endPass := func(w http.ResponseWriter, r *http.Request) {
		end()
		<-r.Context().Done()
	}
	copyClient, reads := stub(t, map[string][]answer{configMapsPath: {rows(500, "c1"), endPass, rows(3, "")}})
	tenant, _ := stub(t, nil)
	s := &sweep{copy: copyClient, tenant: tenant}
	if err := s.run(pass); !errors.Is(err, errBudget) {
		t.Fatalf("the pass ends with %v, want errBudget", err)
	}
	sweptAll(t, s)
	var lists []string
	for _, read := range reads.read() {
		if strings.HasPrefix(read, configMapsPath) {
			lists = append(lists, read)
		}
	}
	if len(s.failed) > 0 || len(s.stopping) > 0 || s.objects != 503 || len(lists) != 3 || !strings.Contains(lists[2], "continue=c1") {
		t.Errorf("the sweep ends with %d objects, failed %v and stopping %q after the lists %q, want 503 objects and the second page read again with its token", s.objects, s.failed, s.stopping, lists)
	}
}

func crdsOfTheCopy(crds ...string) answer {
	var items []string
	for _, crd := range crds {
		plural, version, _ := strings.Cut(crd, "/")
		items = append(items, `{"spec":{"group":"example.com","names":{"plural":"`+plural+`"},"versions":[{"name":"`+version+`","served":true}]}}`)
	}
	return body(`{"kind":"List","metadata":{},"items":[` + strings.Join(items, ",") + `]}`)
}

func serves(versions map[string][]string) answer {
	var out []string
	for _, version := range slices.Sorted(maps.Keys(versions)) {
		var resources []string
		for _, r := range versions[version] {
			resources = append(resources, `{"resource":"`+r+`","verbs":["list"]}`)
		}
		out = append(out, `{"version":"`+version+`","resources":[`+strings.Join(resources, ",")+`]}`)
	}
	if len(out) == 0 {
		return body(noGroups)
	}
	return body(`{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[{"metadata":{"name":"example.com"},"versions":[` + strings.Join(out, ",") + `]}]}`)
}

func TestACustomResourceTheTenantServesStopsTheStepOnlyWhenTheCopysOwnCRDServesIt(t *testing.T) {
	for _, c := range []struct {
		name               string
		crds               []string
		copyServes, tenant map[string][]string
		stopping, reported []string
	}{
		{"a CRD the copy holds and does not serve, its group-version missing", []string{"as/v1"}, nil, map[string][]string{"v1": {"as"}},
			[]string{"the copy does not serve as of example.com/v1, which its own CustomResourceDefinition serves"}, nil},
		{"a CRD the copy holds and does not serve, beside one it serves", []string{"as/v1", "bs/v1"}, map[string][]string{"v1": {"as"}}, map[string][]string{"v1": {"as", "bs"}},
			[]string{"the copy does not serve bs of example.com/v1, which its own CustomResourceDefinition serves"}, nil},
		{"a CRD created after the snapshot beside one the copy serves", []string{"as/v1"}, map[string][]string{"v1": {"as"}}, map[string][]string{"v1": {"as", "bs"}},
			nil, []string{"only the tenant serves bs of example.com/v1"}},
		{"a version added after the snapshot to a CRD the copy holds", []string{"as/v1"}, map[string][]string{"v1": {"as"}}, map[string][]string{"v1": {"as"}, "v2": {"as"}},
			nil, []string{"only the tenant serves example.com/v2"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			copyClient, _ := stub(t, map[string][]answer{
				"/apis": {serves(c.copyServes)},
				"/apis/apiextensions.k8s.io/v1/customresourcedefinitions": {crdsOfTheCopy(c.crds...)},
				"/apis/example.com/v1/as":                                 {rows(1, "")},
			})
			tenant, _ := stub(t, map[string][]answer{"/apis": {serves(c.tenant)}})
			s := &sweep{copy: copyClient, tenant: tenant}
			sweptAll(t, s)
			if !slices.Equal(s.stopping, c.stopping) || !slices.Equal(s.reported, c.reported) {
				t.Errorf("the sweep stops on %q and reports %q, want %q and %q", s.stopping, s.reported, c.stopping, c.reported)
			}
		})
	}
}
