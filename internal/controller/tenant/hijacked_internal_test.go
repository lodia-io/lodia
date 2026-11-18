// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/tenantclient"
)

// standingIn is a server of the test's own where a tenant's API server is, as
// the administrator of a tenant can put one in place of a built-in group.
func standingIn(t *testing.T, handler http.Handler) tenantAPI {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	c, err := tenantclient.NewClients(tenantclient.Options{Connections: 1, Timeout: time.Minute}).Of(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
	if err != nil {
		t.Fatal(err)
	}
	return tenantAPI{client: c, timeout: time.Minute}
}

func kubeconfigFor(t *testing.T, server string) []byte {
	t.Helper()
	kubeconfig, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: server}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {}},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	return kubeconfig
}

func answering(body []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

var (
	proxy      = entry{Group: "apps", Version: "v1", Kind: "DaemonSet", Namespace: "kube-system", Name: "kube-proxy"}
	dns        = entry{Group: "apps", Version: "v1", Kind: "Deployment", Namespace: "kube-system", Name: "coredns"}
	configMap  = entry{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: "coredns"}
	allVerbs   = []string{"read", "apply", "delete"}
	somewhere  = &object{uid: "7a3c", resourceVersion: "42"}
	sentinel   = "BODYBYTES"
	sentinelOf = strings.Repeat(sentinel, 4096/len(sentinel))
)

func TestAWorkloadAnswerPastTheBoundIsNotReadWhole(t *testing.T) {
	var sent atomic.Int64
	api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		chunk := []byte(`{"metadata":{"generation":1},"spec":{"padding":"` + strings.Repeat("x", 64<<10-40) + `"}}`)
		for sent.Load() < 3*tenantclient.MaxResponseBytes {
			n, err := w.Write(chunk)
			sent.Add(int64(n))
			if err != nil {
				return
			}
		}
	}))
	_, err := rollout(t.Context(), api, &lodiav1alpha1.ControlPlane{}, []entry{proxy}, nil)
	if !errors.Is(err, tenantclient.ErrResponseTooLong) {
		t.Fatalf("a DaemonSet of 24 MiB was read as %v, want the error of the bound", err)
	}
	if reason, message, answered := outcome(err); reason != lodiav1alpha1.ReasonApplying || answered || strings.Contains(message, "xxxx") {
		t.Errorf("the pass reports %q: %q, answered %t", reason, message, answered)
	}
	if got := sent.Load(); got > 2*tenantclient.MaxResponseBytes {
		t.Errorf("the server wrote %d bytes before the client gave up, want the bound of %d and what the connection buffers", got, tenantclient.MaxResponseBytes)
	}
}

func TestAFailedAnswerThatIsNoStatusCarriesNoByteOfItsBody(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusForbidden} {
		for _, verb := range allVerbs {
			t.Run(strconv.Itoa(code)+", "+verb, func(t *testing.T) {
				api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "text/plain")
					w.WriteHeader(code)
					_, _ = w.Write([]byte(sentinelOf))
				}))
				var err error
				switch verb {
				case "read":
					_, err = readEntry(t.Context(), api, configMap)
				case "apply":
					err = apply(t.Context(), api, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "coredns", "namespace": "kube-system"}}})
				case "delete":
					err = api.delete(t.Context(), configMap, somewhere)
				}
				if err == nil {
					t.Fatal("a failed answer was no error")
				}
				reason, message, _ := outcome(failed(configMap, err))
				var logged bytes.Buffer
				crzap.New(crzap.WriteTo(&logged)).Error(failed(configMap, err), "the tenant refused")
				for what, text := range map[string]string{"error": err.Error(), "message": message, "log": logged.String(), "reason": reason} {
					if strings.Contains(text, sentinel) {
						t.Errorf("the %s carries the body: %.200q", what, text)
					}
				}
				if logged.Len() > 1024 {
					t.Errorf("the log line is %d bytes", logged.Len())
				}
			})
		}
	}
}

func TestAnObjectWithMoreFieldManagersThanTheMessageKeepsIsAnotherOwnersUnlessLodiaAppliedIt(t *testing.T) {
	managers := func(lodiaLast bool) []byte {
		var entries []string
		for i := 25; i > 0; i-- {
			entries = append(entries, fmt.Sprintf(`{"manager":"manager-%02d","operation":"Update"}`, i))
		}
		entries = append(entries, `{"manager":"manager-01","operation":"Apply"}`, `{"operation":"Update"}`)
		if lodiaLast {
			entries = append(entries, `{"manager":"`+FieldManager+`","operation":"Apply"}`)
		}
		return []byte(`{"metadata":{"uid":"7a3c","resourceVersion":"42","managedFields":[` + strings.Join(entries, ",") + `]}}`)
	}
	obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "coredns", "namespace": "kube-system"}}}
	others, err := foreign(t.Context(), standingIn(t, answering(managers(true))), []*unstructured.Unstructured{obj}, nil)
	if err != nil || len(others) != 0 {
		t.Fatalf("an object Lodia applied, in the last of its 28 managedFields entries, is %v, %v", others, err)
	}

	others, err = foreign(t.Context(), standingIn(t, answering(managers(false))), []*unstructured.Unstructured{obj}, nil)
	if err != nil || len(others) != 1 {
		t.Fatalf("an object Lodia did not apply is %v, %v", others, err)
	}
	want := "its field managers are manager-01, manager-02, manager-03, manager-04, manager-05, manager-06, manager-07, manager-08, manager-09, manager-10 and 15 more entries"
	if got := others[0].owners(); got != want {
		t.Errorf("the owners are %q, want %q", got, want)
	}
}

func TestAManagedFieldsOfAHundredThousandEntriesKeepTenNames(t *testing.T) {
	var entries strings.Builder
	const count = 100000
	for i := range count {
		if i > 0 {
			entries.WriteString(",")
		}
		entries.WriteString(`{"manager":"m-` + strconv.Itoa(i%3000) + `","operation":"Update"}`)
	}
	o, err := tenantclient.Decode([]byte(`{"metadata":{"managedFields":[`+entries.String()+`]}}`), nil, objectOf)
	if err != nil {
		t.Fatal(err)
	}
	if o.applied || len(o.managers.names) != maxManagers || o.managers.total != count {
		t.Errorf("kept %d names of %d entries, applied %t", len(o.managers.names), o.managers.total, o.applied)
	}
}

func TestWhatAnObjectIsReadIntoIsWhatTenantKeepsAndNothingElse(t *testing.T) {
	padding := `[` + strings.Repeat(`{},`, 1<<20) + `{}]`
	const most = 256 << 10
	for _, c := range []struct {
		name string
		body string
		read func([]byte) error
	}{
		{"an object", `{"metadata":{"uid":"7a3c","resourceVersion":"42","annotations":{"a":` + padding + `}},"spec":{"a":` + padding + `},"data":{"a":` + padding + `}}`, func(b []byte) error {
			_, err := tenantclient.Decode(b, nil, objectOf)
			return err
		}},
		{"a Deployment", `{"metadata":{"generation":1,"managedFields":` + padding + `},"spec":{"replicas":1,"template":{"a":` + padding + `}},"status":{"conditions":` + padding + `}}`, func(b []byte) error {
			_, err := tenantclient.Decode(b, nil, workloadOf("Deployment"))
			return err
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := []byte(c.body)
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			err := c.read(body)
			runtime.ReadMemStats(&after)
			if err != nil {
				t.Fatal(err)
			}
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > most {
				t.Errorf("reading %d bytes allocated %d bytes, want at most %d", len(body), allocated, most)
			}
		})
	}
}

func TestAnInventoryPastItsBoundIsOneThatCannotBeRead(t *testing.T) {
	inventoryOfSize := func(n int) []byte {
		entries := make([]entry, n)
		for i := range entries {
			entries[i] = entry{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: "cm-" + strconv.Itoa(i)}
		}
		text, err := json.Marshal(entries)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(map[string]any{"metadata": map[string]any{"name": inventoryName}, "data": map[string]string{inventoryKey: string(text)}})
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	for _, c := range []struct {
		entries int
		fails   bool
	}{{maxEntries, false}, {maxEntries + 1, true}} {
		t.Run(strconv.Itoa(c.entries)+" entries", func(t *testing.T) {
			api := standingIn(t, answering(inventoryOfSize(c.entries)))
			got, err := readInventory(t.Context(), api)
			if c.fails {
				var f *failure
				if !errors.As(err, &f) || f.verdict != "cannot be read: delete it, and every object is applied again" || f.reason != lodiav1alpha1.ReasonApplying {
					t.Fatalf("an inventory of %d entries was read as %v, want one that cannot be read", c.entries, err)
				}
				return
			}
			if err != nil || len(got) != c.entries {
				t.Fatalf("an inventory of %d entries was read as %d entries and %v", c.entries, len(got), err)
			}
		})
	}
}

func TestAnInventoryEntryPastTheBoundOfANameCannotBeRead(t *testing.T) {
	for _, name := range []string{strings.Repeat("n", 254), strings.Repeat("é", 2049)} {
		text, err := json.Marshal([]entry{{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: name}})
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(map[string]any{"data": map[string]string{inventoryKey: string(text)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := readInventory(t.Context(), standingIn(t, answering(body))); err == nil {
			t.Errorf("an entry named with %d bytes was read", len(name))
		}
	}
}

func TestAnApplyIsServerSideByPathAsLodiasFieldManagerAndForces(t *testing.T) {
	var mu sync.Mutex
	var seen []*http.Request
	var bodies [][]byte
	api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen, bodies = append(seen, r), append(bodies, body)
		mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
	for _, obj := range []*unstructured.Unstructured{
		{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "coredns", "namespace": "kube-system"}}},
		{Object: map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": map[string]any{"name": "lodia:proxy"}}},
	} {
		if err := apply(t.Context(), api, obj); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []string{"/api/v1/namespaces/kube-system/configmaps/coredns", "/apis/rbac.authorization.k8s.io/v1/clusterroles/lodia:proxy"} {
		r := seen[i]
		if r.Method != http.MethodPatch || r.URL.Path != want || r.Header.Get("Content-Type") != "application/apply-patch+yaml" {
			t.Errorf("sent %s %s as %q, want PATCH %s as application/apply-patch+yaml", r.Method, r.URL.Path, r.Header.Get("Content-Type"), want)
		}
		if got := r.URL.Query(); got.Get("fieldManager") != FieldManager || got.Get("force") != "true" {
			t.Errorf("sent the query %v, want the field manager %s and force", got, FieldManager)
		}
		if !json.Valid(bodies[i]) || !strings.Contains(string(bodies[i]), `"kind":`) {
			t.Errorf("sent the body %q", bodies[i])
		}
	}
}

func TestADeleteIsConditionalOnWhatWasReadAndLeavesTheDependentsToTheBackground(t *testing.T) {
	var got *http.Request
	var body []byte
	api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		body, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{}`))
	}))
	if err := api.delete(t.Context(), dns, somewhere); err != nil {
		t.Fatal(err)
	}
	if got.Method != http.MethodDelete || got.URL.Path != "/apis/apps/v1/namespaces/kube-system/deployments/coredns" {
		t.Errorf("sent %s %s", got.Method, got.URL.Path)
	}
	var options metav1.DeleteOptions
	if err := json.Unmarshal(body, &options); err != nil {
		t.Fatalf("the body %q: %v", body, err)
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "7a3c" || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != "42" {
		t.Errorf("the preconditions are %+v, want the UID 7a3c and the resourceVersion 42 it read", options.Preconditions)
	}
	if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationBackground {
		t.Errorf("the propagation is %v, want Background", options.PropagationPolicy)
	}
}

func TestAKindOutsideTheTableIsNeverSentAndAnEntryOfAnotherScopeIsNone(t *testing.T) {
	var sent atomic.Int32
	api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sent.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	for _, e := range []entry{
		{Group: "example.com", Version: "v1", Kind: "Widget", Namespace: "default", Name: "w"},
		{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole", Namespace: "default", Name: "wrong-scope"},
		{Version: "v1", Kind: "ConfigMap", Name: "no-namespace"},
		{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: "../../secrets"},
		{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system"},
	} {
		if body, err := api.read(t.Context(), e); body != nil || err != nil {
			t.Errorf("%v read as %q, %v, want an object that is not there", e, body, err)
		}
	}
	if n := sent.Load(); n != 0 {
		t.Errorf("%d requests were sent for entries Tenant does not address", n)
	}
}

func TestAnInventoryOfManySmallEntriesIsCountedAndNotDecodedWhole(t *testing.T) {
	const most = 4 << 20
	text := "[" + strings.Repeat("{},", 340000) + "{}]"
	body, err := json.Marshal(map[string]any{"data": map[string]string{inventoryKey: text}})
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = tenantclient.Decode(body, nil, inventoryFrom)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, errTooMany) {
		t.Fatalf("an inventory of 340,001 entries was read as %v, want the error of the bound", err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > most {
		t.Errorf("reading an inventory of %d bytes allocated %d bytes, want at most %d", len(text), allocated, most)
	}
}

func TestAnUpdateUnderLodiasManagerNameIsNotLodiasApply(t *testing.T) {
	for operation, want := range map[string]bool{"Apply": true, "Update": false} {
		o, err := tenantclient.Decode([]byte(`{"metadata":{"uid":"7a3c","managedFields":[{"manager":"`+FieldManager+`","operation":"`+operation+`"}]}}`), nil, objectOf)
		if err != nil {
			t.Fatal(err)
		}
		if o.applied != want {
			t.Errorf("an entry of %s under %s is Lodia's apply: %t, want %t", operation, FieldManager, o.applied, want)
		}
	}
}

func TestTenantsClientHasOneConnectionAndItsHandshakeEndsWithinTheDeliveryTimeout(t *testing.T) {
	r := &Reconciler{}
	api, err := r.reach(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, "https://dev-kube-apiserver.team-a.svc:443"), applyTimeout)
	if err != nil {
		t.Fatal(err)
	}
	bottom := api.client.HTTP().Transport
	for {
		wrapped, ok := bottom.(utilnet.RoundTripperWrapper)
		if !ok {
			break
		}
		bottom = wrapped.WrappedRoundTripper()
	}
	transport, ok := bottom.(*http.Transport)
	if !ok {
		t.Fatalf("the transport below Tenant's client is a %T, want an *http.Transport", bottom)
	}
	if transport.MaxConnsPerHost != 1 || transport.TLSHandshakeTimeout != deliveryTimeout {
		t.Errorf("Tenant's client has %d connections and a handshake of %s, want 1 and %s", transport.MaxConnsPerHost, transport.TLSHandshakeTimeout, deliveryTimeout)
	}
}
