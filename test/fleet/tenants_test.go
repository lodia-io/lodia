// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package fleet

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/controller/tenant"
)

const (
	pathService      = "service"
	pathAPI          = "api"
	pathTunnel       = "tunnel"
	pathKonnectivity = "konnectivity"
)

// hangFor is how long a hanging tenant holds a connection: past any Timeout
// of the operator's.
const hangFor = 15 * time.Second

// slowApply is how long a slow tenant takes to answer each of Tenant's
// applies.
const slowApply = 150 * time.Millisecond

type tenantServer struct {
	admin     client.Client
	discovery *discovery
	nodes     int
	listener  net.Listener
	conns     chan net.Conn
	server    *http.Server

	mu      sync.Mutex
	tenants map[string]*stubTenant
	certs   map[string]*tls.Certificate

	version    atomic.Int64
	unexpected atomic.Int64
	padding    []byte
	record     recorder

	closing    atomic.Bool
	handshakes atomic.Int64
	statesMu   sync.Mutex
	states     map[net.Conn]http.ConnState
}

type stubTenant struct {
	namespace string
	hanging   atomic.Bool
	bounded   atomic.Bool
	slow      atomic.Bool
	matching  atomic.Pointer[tenantApply]

	mu      sync.Mutex
	objects map[string]stored
}

// tenantApply is what the stub does with an apply of Tenant's to key whose
// body holds contains: it refuses it, or it holds the first one until release
// is closed and closes seen as it comes.
type tenantApply struct {
	key, contains string
	refuse        bool
	seen, release chan struct{}
	once          sync.Once
}

type stored struct {
	kind string
	body []byte
}

type connKey struct{}

type tenantConn struct {
	net.Conn
	tenant *stubTenant
	path   string
	last   atomic.Value
}

func startTenants(t *testing.T, admin client.Client, cfg *rest.Config, nodes int) *tenantServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := &tenantServer{
		admin: admin, discovery: &discovery{client: httpClient, host: strings.TrimSuffix(cfg.Host, "/"), cached: map[string]*answer{}}, nodes: nodes,
		listener: listener, conns: make(chan net.Conn), tenants: map[string]*stubTenant{}, certs: map[string]*tls.Certificate{},
		padding: bytes.Repeat([]byte{'x'}, boundedBody), record: recorder{last: map[string]time.Time{}}, states: map[net.Conn]http.ConnState{},
	}
	s.server = &http.Server{
		Handler:           s,
		ReadHeaderTimeout: hangFor,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, connKey{}, c.(*tenantConn))
		},
		ConnState: func(c net.Conn, state http.ConnState) {
			s.statesMu.Lock()
			defer s.statesMu.Unlock()
			if state == http.StateClosed || state == http.StateHijacked {
				delete(s.states, c)
				return
			}
			s.states[c] = state
		},
	}
	go func() { _ = s.server.Serve(&chanListener{s: s, closed: make(chan struct{})}) }()
	go s.accept()
	t.Cleanup(func() {
		_ = listener.Close()
		_ = s.server.Close()
	})
	return s
}

func (s *tenantServer) address() string { return s.listener.Addr().String() }

func (s *tenantServer) idle() map[string]int64 {
	s.statesMu.Lock()
	defer s.statesMu.Unlock()
	n := map[string]int64{}
	for c, state := range s.states {
		if state == http.StateIdle {
			client, _ := c.(*tenantConn).last.Load().(string)
			n[client]++
		}
	}
	return n
}

func (s *tenantServer) add(namespace string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants[namespace] = &stubTenant{namespace: namespace, objects: map[string]stored{}}
}

func (s *tenantServer) tenant(namespace string) *stubTenant {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tenants[namespace]
}

func (s *tenantServer) classify(name string) (*stubTenant, string) {
	labels := strings.Split(name, ".")
	switch {
	case len(labels) == 3 && labels[2] == "svc" && labels[0] == lodiav1alpha1.APIServerServiceName(controlPlaneName):
		return s.tenant(labels[1]), pathService
	case len(labels) == 4 && labels[2]+"."+labels[3] == hostSuffix:
		switch labels[0] {
		case pathAPI, pathTunnel, pathKonnectivity:
			return s.tenant(labels[1]), labels[0]
		}
	}
	return nil, ""
}

func (s *tenantServer) accept() {
	for {
		raw, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handshake(raw)
	}
}

func (s *tenantServer) handshake(raw net.Conn) {
	var tenant *stubTenant
	var path string
	outer := tls.Server(raw, &tls.Config{MinVersion: tls.VersionTLS12, GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		tenant, path = s.classify(hello.ServerName)
		if tenant == nil {
			s.unexpected.Add(1)
			return nil, fmt.Errorf("no tenant answers %q", hello.ServerName)
		}
		if tenant.hanging.Load() {
			time.Sleep(hangFor)
			return nil, errors.New("hanging")
		}
		return s.serving(tenant, path)
	}})
	_ = raw.SetDeadline(time.Now().Add(hangFor))
	if err := outer.Handshake(); err != nil {
		_ = raw.Close()
		return
	}
	conn := net.Conn(outer)
	switch path {
	case pathKonnectivity:
		_, _ = io.Copy(io.Discard, outer)
		_ = raw.Close()
		return
	case pathTunnel:
		inner := tls.Server(outer, &tls.Config{MinVersion: tls.VersionTLS12, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return s.serving(tenant, pathAPI)
		}})
		if err := inner.Handshake(); err != nil {
			_ = raw.Close()
			return
		}
		conn, path = inner, pathAPI
	}
	s.handshakes.Add(1)
	_ = raw.SetDeadline(time.Time{})
	select {
	case s.conns <- &tenantConn{Conn: conn, tenant: tenant, path: path}:
	case <-time.After(hangFor):
		_ = raw.Close()
	}
}

func (s *tenantServer) serving(tenant *stubTenant, path string) (*tls.Config, error) {
	secret := lodiav1alpha1.APIServerServingSecret
	switch path {
	case pathTunnel:
		secret = lodiav1alpha1.TunnelServingSecret
	case pathKonnectivity:
		secret = lodiav1alpha1.KonnectivityServerSecret
	}
	key := tenant.namespace + "/" + secret.Name(controlPlaneName)
	s.mu.Lock()
	cert := s.certs[key]
	s.mu.Unlock()
	if cert == nil {
		read := &corev1.Secret{}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.admin.Get(ctx, client.ObjectKey{Namespace: tenant.namespace, Name: secret.Name(controlPlaneName)}, read); err != nil {
			return nil, err
		}
		pair, err := tls.X509KeyPair(read.Data[corev1.TLSCertKey], read.Data[corev1.TLSPrivateKeyKey])
		if err != nil {
			return nil, err
		}
		cert = &pair
		s.mu.Lock()
		s.certs[key] = cert
		s.mu.Unlock()
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{*cert}, ClientAuth: tls.RequestClientCert, NextProtos: []string{"http/1.1"}}, nil
}

type chanListener struct {
	s      *tenantServer
	once   sync.Once
	closed chan struct{}
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.s.conns:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *chanListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *chanListener) Addr() net.Addr { return l.s.listener.Addr() }

const (
	leasePath     = "/apis/coordination.k8s.io/v1/namespaces/kube-system/leases/"
	nodeLeasePath = "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases"
	nodesPath     = "/api/v1/nodes"
)

func (s *tenantServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn := r.Context().Value(connKey{}).(*tenantConn)
	t := conn.tenant
	if t.hanging.Load() {
		select {
		case <-r.Context().Done():
		case <-time.After(hangFor):
		}
		return
	}
	if s.closing.Load() {
		w.Header().Set("Connection", "close")
	}
	conn.last.Store(clientOf(r.URL.Path))
	switch path := r.URL.Path; {
	case path == contract.APIServerReadyzPath:
		if conn.path == pathService {
			s.record.saw(kindProbe, t.namespace)
		}
		_, _ = io.WriteString(w, "ok")
	case r.Method == http.MethodGet && (path == leasePath+contract.ControllerManagerLease || path == leasePath+contract.SchedulerLease):
		s.writeJSON(w, http.StatusOK, s.leaderLease(strings.TrimPrefix(path, leasePath)))
	case r.Method == http.MethodGet && path == nodeLeasePath:
		if r.URL.Query().Get("continue") == "" {
			s.record.saw(kindNodeLeases, t.namespace)
		}
		s.writeList(w, t, s.nodeLeases())
	case r.Method == http.MethodGet && path == nodesPath:
		s.writeList(w, t, s.nodeList())
	default:
		res, ok := parseResource(path)
		if !ok {
			s.discovery.serve(w, r)
			return
		}
		s.serveResource(w, r, t, res)
	}
}

func clientOf(path string) string {
	switch {
	case path == contract.APIServerReadyzPath:
		return "probe"
	case strings.HasPrefix(path, leasePath):
		return "health"
	case path == nodeLeasePath || path == nodesPath:
		return "health-nodes"
	case strings.Contains(path, "/clusterrolebindings"):
		return "pki"
	}
	return "tenant"
}

func (s *tenantServer) writeJSON(w http.ResponseWriter, code int, obj any) {
	body, err := json.Marshal(obj)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func (s *tenantServer) writeList(w http.ResponseWriter, t *stubTenant, list any) {
	body, err := json.Marshal(list)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if !t.bounded.Load() {
		_, _ = w.Write(body)
		return
	}
	padding := s.padding[:boundedBody-len(body)-len(`,"padding":""`)]
	_, _ = w.Write(body[:len(body)-1])
	_, _ = io.WriteString(w, `,"padding":"`)
	_, _ = w.Write(padding)
	_, _ = io.WriteString(w, `"}`)
}

// leaderLease is renewed every 2 s, as Workloads renders the components'
// retry period.
func (s *tenantServer) leaderLease(name string) *coordinationv1.Lease {
	now := time.Now()
	return &coordinationv1.Lease{
		TypeMeta: metav1.TypeMeta{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: metav1.NamespaceSystem, UID: types.UID("lease-" + name),
			ResourceVersion: strconv.FormatInt(now.Unix()/2, 10),
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       new(controlPlaneName + "-" + name + "-0_" + string(uuid.NewUUID())),
			LeaseDurationSeconds: new(int32(15)),
			RenewTime:            &metav1.MicroTime{Time: now},
		},
	}
}

// nodeLeases are renewed every 10 s, as a kubelet renews its Lease.
func (s *tenantServer) nodeLeases() *coordinationv1.LeaseList {
	now := time.Now()
	list := &coordinationv1.LeaseList{TypeMeta: metav1.TypeMeta{APIVersion: "coordination.k8s.io/v1", Kind: "LeaseList"}, ListMeta: metav1.ListMeta{ResourceVersion: strconv.FormatInt(now.Unix(), 10)}}
	for i := range s.nodes {
		name := nodeName(i)
		list.Items = append(list.Items, coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: corev1.NamespaceNodeLease, ResourceVersion: strconv.FormatInt((now.Unix()+int64(i))/10, 10)},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: new(name), LeaseDurationSeconds: new(int32(40)), RenewTime: &metav1.MicroTime{Time: now}},
		})
	}
	return list
}

func (s *tenantServer) nodeList() *corev1.NodeList {
	list := &corev1.NodeList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "NodeList"}, ListMeta: metav1.ListMeta{ResourceVersion: strconv.FormatInt(time.Now().Unix(), 10)}}
	for i := range s.nodes {
		list.Items = append(list.Items, *kubeletNode(nodeName(i), time.Now()))
	}
	return list
}

func nodeName(i int) string { return fmt.Sprintf("node-%02d", i) }

type apiPath struct {
	group, version, plural, namespace, name, subresource string
}

func (r apiPath) key() string {
	return r.group + "/" + r.plural + "/" + r.namespace + "/" + r.name
}

func parseResource(path string) (apiPath, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	var r apiPath
	switch {
	case len(parts) >= 3 && parts[0] == "api":
		r.version, parts = parts[1], parts[2:]
	case len(parts) >= 4 && parts[0] == "apis":
		r.group, r.version, parts = parts[1], parts[2], parts[3:]
	default:
		return r, false
	}
	if len(parts) >= 3 && parts[0] == "namespaces" {
		r.namespace, parts = parts[1], parts[2:]
	}
	r.plural = parts[0]
	if len(parts) > 1 {
		r.name = parts[1]
	}
	if len(parts) > 2 {
		r.subresource = parts[2]
	}
	return r, true
}

func (s *tenantServer) serveResource(w http.ResponseWriter, r *http.Request, t *stubTenant, res apiPath) {
	switch r.Method {
	case http.MethodGet:
		if res.name == "" {
			s.writeJSON(w, http.StatusOK, t.list(res))
			return
		}
		if obj, ok := t.get(res.key()); ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(obj.body)
			return
		}
		writeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound, fmt.Sprintf("%s %q not found", res.plural, res.name))
	case http.MethodPatch, http.MethodPost, http.MethodPut:
		if r.Method == http.MethodPatch && !strings.HasPrefix(r.Header.Get("Content-Type"), "application/apply-patch") {
			s.unexpected.Add(1)
			writeStatus(w, http.StatusUnsupportedMediaType, metav1.StatusReasonUnsupportedMediaType, "only an apply is served")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, err.Error())
			return
		}
		if r.URL.Query().Get("fieldManager") == tenant.FieldManager {
			if t.slow.Load() {
				time.Sleep(slowApply)
			}
			if m := t.matching.Load(); m != nil && res.key() == m.key && bytes.Contains(body, []byte(m.contains)) {
				if m.refuse {
					writeStatus(w, http.StatusUnprocessableEntity, metav1.StatusReasonInvalid, "refused by the test")
					return
				}
				m.once.Do(func() { close(m.seen) })
				<-m.release
			}
		}
		obj, created, err := s.store(t, res, body, r.URL.Query().Get("fieldManager"))
		if err != nil {
			writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, err.Error())
			return
		}
		code := http.StatusOK
		if created {
			code = http.StatusCreated
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write(obj)
	case http.MethodDelete:
		if !t.remove(res.key()) {
			writeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound, fmt.Sprintf("%s %q not found", res.plural, res.name))
			return
		}
		s.writeJSON(w, http.StatusOK, &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess})
	default:
		s.unexpected.Add(1)
		writeStatus(w, http.StatusMethodNotAllowed, metav1.StatusReasonMethodNotAllowed, r.Method)
	}
}

func writeStatus(w http.ResponseWriter, code int, reason metav1.StatusReason, message string) {
	body, _ := json.Marshal(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Code: int32(code), Reason: reason, Message: message})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func (s *tenantServer) store(t *stubTenant, res apiPath, body []byte, manager string) ([]byte, bool, error) {
	raw, err := yaml.YAMLToJSON(body)
	if err != nil {
		return nil, false, err
	}
	obj := map[string]any{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, false, err
	}
	metadata, _ := obj["metadata"].(map[string]any)
	if metadata == nil {
		return nil, false, errors.New("no metadata")
	}
	if res.name == "" {
		res.name, _ = metadata["name"].(string)
	}
	kind, _ := obj["kind"].(string)
	t.mu.Lock()
	defer t.mu.Unlock()
	previous, existed := t.objects[res.key()]
	generation, uid, created := int64(1), string(uuid.NewUUID()), time.Now().UTC().Format(time.RFC3339)
	if existed {
		old := map[string]any{}
		if err := json.Unmarshal(previous.body, &old); err != nil {
			return nil, false, err
		}
		oldMeta := old["metadata"].(map[string]any)
		generation, uid, created = int64(oldMeta["generation"].(float64)), oldMeta["uid"].(string), oldMeta["creationTimestamp"].(string)
		if !sameContent(old, obj) {
			generation++
		}
	}
	metadata["uid"], metadata["generation"], metadata["creationTimestamp"] = uid, generation, created
	metadata["resourceVersion"] = strconv.FormatInt(s.version.Add(1), 10)
	metadata["managedFields"] = []any{map[string]any{
		"manager": manager, "operation": "Apply", "apiVersion": obj["apiVersion"], "time": created, "fieldsType": "FieldsV1", "fieldsV1": map[string]any{},
	}}
	spec, _ := obj["spec"].(map[string]any)
	switch kind {
	case "Deployment":
		replicas := int64(1)
		if n, ok := spec["replicas"].(float64); ok {
			replicas = int64(n)
		}
		obj["status"] = map[string]any{"observedGeneration": generation, "replicas": replicas, "updatedReplicas": replicas, "readyReplicas": replicas, "availableReplicas": replicas}
	case "DaemonSet":
		n := s.nodes
		obj["status"] = map[string]any{"observedGeneration": generation, "desiredNumberScheduled": n, "currentNumberScheduled": n, "numberReady": n, "updatedNumberScheduled": n, "numberAvailable": n}
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, false, err
	}
	t.objects[res.key()] = stored{kind: kind, body: out}
	return out, !existed, nil
}

func sameContent(a, b map[string]any) bool {
	strip := func(obj map[string]any) []byte {
		copied := map[string]any{}
		for k, v := range obj {
			if k != "metadata" && k != "status" {
				copied[k] = v
			}
		}
		out, _ := json.Marshal(copied)
		return out
	}
	return bytes.Equal(strip(a), strip(b))
}

func (t *stubTenant) get(key string) (stored, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	obj, ok := t.objects[key]
	return obj, ok
}

func (t *stubTenant) remove(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.objects[key]
	delete(t.objects, key)
	return ok
}

func (t *stubTenant) list(res apiPath) map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
	prefix := res.group + "/" + res.plural + "/" + res.namespace + "/"
	items := []json.RawMessage{}
	kind := ""
	for key, obj := range t.objects {
		if strings.HasPrefix(key, prefix) || res.namespace == "" && strings.HasPrefix(key, res.group+"/"+res.plural+"/") {
			items, kind = append(items, obj.body), obj.kind
		}
	}
	return map[string]any{"kind": kind + "List", "apiVersion": strings.TrimPrefix(res.group+"/"+res.version, "/"), "metadata": map[string]any{}, "items": items}
}

// discovery answers the discovery documents the management API server
// answers, fetched once for each path and Accept: the tenant runs the same
// kinds.
type discovery struct {
	client *http.Client
	host   string

	mu     sync.Mutex
	cached map[string]*answer
}

type answer struct {
	code        int
	contentType string
	body        []byte
}

func (d *discovery) serve(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path + "\x00" + r.Header.Get("Accept")
	d.mu.Lock()
	a := d.cached[key]
	if a == nil {
		var err error
		if a, err = d.fetch(r.Context(), r.URL.Path, r.Header.Get("Accept")); err != nil {
			d.mu.Unlock()
			writeStatus(w, http.StatusServiceUnavailable, metav1.StatusReasonServiceUnavailable, err.Error())
			return
		}
		d.cached[key] = a
	}
	d.mu.Unlock()
	w.Header().Set("Content-Type", a.contentType)
	w.WriteHeader(a.code)
	_, _ = w.Write(a.body)
}

func (d *discovery) fetch(ctx context.Context, path, accept string) (*answer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.host+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &answer{code: resp.StatusCode, contentType: resp.Header.Get("Content-Type"), body: body}, nil
}

const (
	kindProbe      = "probe"
	kindNodeLeases = "node leases"
)

type recorder struct {
	mu        sync.Mutex
	last      map[string]time.Time
	intervals map[string][]time.Duration
}

func (r *recorder) saw(kind, namespace string) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	key := kind + "/" + namespace
	if last, ok := r.last[key]; ok && r.intervals != nil {
		r.intervals[kind+"/"+classOf(namespace)] = append(r.intervals[kind+"/"+classOf(namespace)], now.Sub(last))
	}
	r.last[key] = now
}

func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.intervals = map[string][]time.Duration{}
}

func (r *recorder) take() map[string][]time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.intervals
	r.intervals = nil
	return out
}

var classes sync.Map

func classOf(namespace string) string {
	if c, ok := classes.Load(namespace); ok {
		return c.(string)
	}
	return classHonest
}

const (
	classHonest  = "honest"
	classHanging = "hanging"
	classBounded = "bounded"
)
