// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"
	gozap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"go.lodia.io/lodia/internal/operator"
)

// operatorEnv makes the test binary the operator: it holds the settings the
// fleet hands it, as JSON.
const operatorEnv = "LODIA_FLEET_OPERATOR"

type operatorSettings struct {
	Kubeconfig            string `json:"kubeconfig"`
	Tenants               string `json:"tenants"`
	Etcd                  string `json:"etcd"`
	Version               string `json:"version"`
	MaxConcurrentRollouts int    `json:"maxConcurrentRollouts"`
	Stats                 string `json:"stats"`
}

func logger(w io.Writer) logr.Logger {
	return zap.New(zap.WriteTo(w), zap.Level(zapcore.ErrorLevel))
}

func runOperator(raw string) int {
	var s operatorSettings
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	ctrl.SetLogger(logger(os.Stderr))
	parent := os.Getppid()
	go func() {
		for range time.Tick(time.Second) {
			if os.Getppid() != parent {
				os.Exit(1)
			}
		}
	}()
	if err := operate(s); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func operate(s operatorSettings) error {
	admin, err := clientcmd.BuildConfigFromFlags("", s.Kubeconfig)
	if err != nil {
		return err
	}
	cfg := asOperator(admin)
	cfg.QPS = -1
	counted := &requests{all: map[string]int64{}, tracked: map[string]int64{}}
	cfg.Wrap(counted.wrap)
	gatewayAPI, err := operator.GatewayAPI(cfg)
	if err != nil {
		return err
	}
	cache, err := operator.Cache(gatewayAPI)
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: operator.Scheme(), Metrics: metricsserver.Options{BindAddress: "0"}, Cache: cache})
	if err != nil {
		return err
	}
	config := operator.DefaultConfig()
	config.Fleet.MaxConcurrentRollouts = s.MaxConcurrentRollouts
	in := operator.Inputs{
		Config: config, Version: s.Version, SignerImage: "registry.example/lodia-signer:fleet", TunnelImage: "registry.example/lodia-tunnel:fleet",
		EtcdDruidVersion: druidVersion, EtcdDruidNamespace: druidNamespace, EtcdDruidDeployment: druidDeployment, OperatorNamespace: operatorNamespace, GatewayAPI: gatewayAPI,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", s.Tenants)
		},
	}
	sources, err := operator.Setup(mgr, in)
	if err != nil {
		return err
	}
	sources.Etcd.EtcdClient = sampleEtcd(sources.Health.Secrets, s.Etcd)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	server := &http.Server{Handler: statsHandler(counted), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(listener) }()
	if err := os.WriteFile(s.Stats, []byte(listener.Addr().String()), 0o600); err != nil {
		return err
	}
	return mgr.Start(ctrl.SetupSignalHandler())
}

// sampleEtcd reads the two Secrets datastore.DialEtcd reads, and connects to
// the one member that stands in for every tenant's etcd, without TLS.
func sampleEtcd(reader client.Reader, endpoint string) func(context.Context, *druidv1alpha1.Etcd) (*clientv3.Client, error) {
	return func(ctx context.Context, etcd *druidv1alpha1.Etcd) (*clientv3.Client, error) {
		refs := etcd.Spec.Etcd.ClientUrlTLS
		if refs == nil {
			return nil, fmt.Errorf("no client TLS in Etcd %s", etcd.Name)
		}
		for _, name := range []string{refs.ClientTLSSecretRef.Name, refs.TLSCASecretRef.Name} {
			if err := reader.Get(ctx, client.ObjectKey{Namespace: etcd.Namespace, Name: name}, &corev1.Secret{}); err != nil {
				return nil, err
			}
		}
		return clientv3.New(clientv3.Config{Context: ctx, Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, Logger: gozap.NewNop()})
	}
}

type requests struct {
	mu      sync.Mutex
	all     map[string]int64
	tracked map[string]int64
	track   string
}

func (c *requests) wrap(next http.RoundTripper) http.RoundTripper {
	return roundTripper(func(req *http.Request) (*http.Response, error) {
		key, namespace := requestKey(req)
		c.mu.Lock()
		c.all[key]++
		if c.track != "" && namespace == c.track {
			c.tracked[key]++
		}
		c.mu.Unlock()
		return next.RoundTrip(req)
	})
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func requestKey(req *http.Request) (string, string) {
	res, ok := parseResource(req.URL.Path)
	if !ok {
		return "other", ""
	}
	verb := strings.ToLower(req.Method)
	switch {
	case req.Method == http.MethodGet && req.URL.Query().Get("watch") == "true":
		verb = "watch"
	case req.Method == http.MethodGet && res.name == "":
		verb = "list"
	case req.Method == http.MethodPost:
		verb = "create"
	case req.Method == http.MethodPut:
		verb = "update"
	}
	name := res.plural
	if res.subresource != "" {
		name += "/" + res.subresource
	}
	return verb + " " + name, res.namespace
}

type operatorStats struct {
	Requests   map[string]int64   `json:"requests"`
	Tracked    map[string]int64   `json:"tracked"`
	Reconciles map[string]float64 `json:"reconciles"`
	Errors     map[string]float64 `json:"errors"`
	Depth      map[string]float64 `json:"depth"`
	Workers    map[string]float64 `json:"workers"`
	HeapInuse  uint64             `json:"heapInuse"`
	HeapLive   uint64             `json:"heapLive"`
	HeapAlloc  uint64             `json:"heapAlloc"`
	Stacks     uint64             `json:"stacks"`
	Sys        uint64             `json:"sys"`
	Goroutines int                `json:"goroutines"`
	NumGC      uint32             `json:"numGC"`
	CPU        float64            `json:"cpu"`
	at         time.Time
}

func statsHandler(counted *requests) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/track", func(w http.ResponseWriter, r *http.Request) {
		counted.mu.Lock()
		counted.track = r.URL.Query().Get("namespace")
		counted.tracked = map[string]int64{}
		counted.mu.Unlock()
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("gc") {
			runtime.GC()
			debug.FreeOSMemory()
		}
		s := operatorStats{Reconciles: map[string]float64{}, Errors: map[string]float64{}, Depth: map[string]float64{}, Workers: map[string]float64{}}
		counted.mu.Lock()
		s.Requests, s.Tracked = maps.Clone(counted.all), maps.Clone(counted.tracked)
		counted.mu.Unlock()
		families, err := ctrlmetrics.Registry.Gather()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, f := range families {
			for _, m := range f.GetMetric() {
				labels := map[string]string{}
				for _, l := range m.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				switch f.GetName() {
				case "controller_runtime_reconcile_total":
					s.Reconciles[labels["controller"]] += m.GetCounter().GetValue()
				case "controller_runtime_reconcile_errors_total":
					s.Errors[labels["controller"]] += m.GetCounter().GetValue()
				case "workqueue_depth":
					s.Depth[labels["name"]] += m.GetGauge().GetValue()
				case "controller_runtime_active_workers":
					s.Workers[labels["controller"]] += m.GetGauge().GetValue()
				}
			}
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		live := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
		metrics.Read(live)
		s.HeapInuse, s.HeapAlloc, s.Stacks, s.Sys, s.NumGC = ms.HeapInuse, ms.HeapAlloc, ms.StackInuse, ms.Sys, ms.NumGC
		s.HeapLive, s.Goroutines = live[0].Value.Uint64(), runtime.NumGoroutine()
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err == nil {
			s.CPU = time.Duration(usage.Utime.Nano() + usage.Stime.Nano()).Seconds()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s)
	})
	return mux
}

type operatorProcess struct {
	cmd     *exec.Cmd
	stats   string
	done    chan error
	started time.Time
	client  *http.Client
}

func startOperator(t *testing.T, dir string, cfg *rest.Config, s operatorSettings) *operatorProcess {
	t.Helper()
	kubeconfig, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"fleet": {Server: cfg.Host, CertificateAuthorityData: cfg.CAData}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"admin": {ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData}},
		Contexts:       map[string]*clientcmdapi.Context{"fleet": {Cluster: "fleet", AuthInfo: "admin"}},
		CurrentContext: "fleet",
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Kubeconfig = filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(s.Kubeconfig, kubeconfig, 0o600); err != nil {
		t.Fatal(err)
	}
	s.Stats = filepath.Join(dir, fmt.Sprintf("stats-%d", time.Now().UnixNano()))
	settings, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	logs, err := os.OpenFile(filepath.Join(dir, "operator.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), operatorEnv+"="+string(settings))
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &operatorProcess{cmd: cmd, done: make(chan error, 1), started: time.Now(), client: &http.Client{Timeout: 10 * time.Second}}
	go func() {
		p.done <- cmd.Wait()
		_ = logs.Close()
	}()
	t.Cleanup(func() { p.stop(t) })
	deadline := time.Now().Add(time.Minute)
	for {
		if address, err := os.ReadFile(s.Stats); err == nil && len(address) > 0 {
			p.stats = "http://" + string(address)
			return p
		}
		select {
		case err := <-p.done:
			p.done <- err
			t.Fatalf("the operator exited before it served its stats: %v; its log is %s", err, logs.Name())
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("the operator served no stats within a minute")
		}
	}
}

func (p *operatorProcess) stop(t *testing.T) {
	if p.cmd.ProcessState != nil {
		return
	}
	select {
	case err := <-p.done:
		p.done <- err
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-p.done:
		p.done <- err
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Error("the operator did not stop within 30 s of SIGTERM")
		p.done <- <-p.done
	}
}

func (p *operatorProcess) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.stats+path, nil)
	if err != nil {
		return err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s answered %s", path, resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (p *operatorProcess) read(ctx context.Context) (operatorStats, error) {
	var s operatorStats
	err := p.get(ctx, "/stats", &s)
	s.at = time.Now()
	return s, err
}

func (p *operatorProcess) rss(ctx context.Context) (uint64, error) {
	out, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(p.cmd.Process.Pid)).Output()
	if err != nil {
		return 0, err
	}
	kib, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, errors.New("ps reported no resident set: " + string(out))
	}
	return kib << 10, nil
}
