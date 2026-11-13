// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"net/http"
	"slices"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"go.lodia.io/lodia/internal/apistatus"
)

func startAPIServer(t *testing.T) (*kubernetes.Clientset, *tenant) {
	t.Helper()
	env := &envtest.Environment{UseExistingCluster: new(false)}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	kubeconfig, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: cfg.Host, CertificateAuthorityData: cfg.CAData}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData}},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := (&NodesReconciler{}).tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	return kubernetes.NewForConfigOrDie(cfg), tenant
}

func causesOf(err error) []metav1.CauseType {
	var causes []metav1.CauseType
	if status, ok := err.(apierrors.APIStatus); ok && status.Status().Details != nil {
		for _, cause := range status.Status().Details.Causes {
			causes = append(causes, cause.Type)
		}
	}
	return causes
}

func TestTheCausesOfARealAPIServersRefusalAreWhatTheTypedClientReads(t *testing.T) {
	typed, tenant := startAPIServer(t)
	if _, err := typed.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "doomed"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := typed.CoreV1().Namespaces().Delete(t.Context(), "doomed", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	lease := `{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":"l","namespace":"doomed"}}`
	invalid := `{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":"Not_A_Name","namespace":"kube-system"},"spec":{"leaseDurationSeconds":-5}}`

	for _, c := range []struct {
		name      string
		namespace string
		body      string
		object    *coordinationv1.Lease
		is        func(error) bool
		refused   bool
		cause     metav1.CauseType
	}{
		{"a create in a namespace being deleted", "doomed", lease, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "l"}}, apierrors.IsForbidden, false, corev1.NamespaceTerminatingCause},
		{"an invalid create", "kube-system", invalid, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "Not_A_Name"}, Spec: coordinationv1.LeaseSpec{LeaseDurationSeconds: ptr.To[int32](-5)}}, apierrors.IsInvalid, true, metav1.CauseTypeFieldValueInvalid},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := tenant.do(t.Context(), tenant.api.Post().AbsPath(coordinationPath, c.namespace, "leases").SetHeader("Content-Type", "application/json").Body([]byte(c.body)))
			_, want := typed.CoordinationV1().Leases(c.namespace).Create(t.Context(), c.object, metav1.CreateOptions{})
			if !c.is(err) || !c.is(want) {
				t.Fatalf("the projection returned %v and the typed client %v, want the error the API's own check recognises", err, want)
			}
			if causes, typedCauses := causesOf(err), causesOf(want); !slices.Equal(causes, typedCauses) || !slices.Contains(causes, c.cause) {
				t.Errorf("the projection read the causes %v and the typed client %v, want both to hold %s", causes, typedCauses, c.cause)
			}
			if got := apistatus.Refused(err); got != c.refused {
				t.Errorf("Refused is %t, want %t", got, c.refused)
			}
		})
	}
}

type measuringHeads struct {
	next    http.RoundTripper
	biggest *int
	h2      *int
}

func (m measuringHeads) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := m.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	http1, http2 := len(resp.Proto)+1+len(resp.Status)+2+2, 0
	for name, values := range resp.Header {
		for _, value := range values {
			http1 += len(name) + 2 + len(value) + 2
			http2 += len(name) + len(value) + 32
		}
	}
	*m.biggest, *m.h2 = max(*m.biggest, http1), max(*m.h2, http2)
	return resp, nil
}

func TestAKubeAPIServersHeadersAreFarUnderTheBoundHealthReads(t *testing.T) {
	_, tenant := startAPIServer(t)
	var biggest, h2 int
	tenant.api.Client.Transport = measuringHeads{tenant.api.Client.Transport, &biggest, &h2}
	lease := func(name string) []byte {
		return []byte(`{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":"` + name + `","namespace":"kube-system"}}`)
	}
	_, _ = tenant.leasePage(t.Context(), metav1.ListOptions{Limit: leasePage})
	_, _ = tenant.nodePage(t.Context(), metav1.ListOptions{Limit: nodePage})
	_, _ = tenant.getLease(t.Context(), "absent")
	_, _ = tenant.createLease(t.Context(), lease("held"))
	_, _ = tenant.createLease(t.Context(), lease("held"))
	_, _ = tenant.createLease(t.Context(), []byte(`{"metadata":{"name":"Not_A_Name","namespace":"kube-system"}}`))
	_, _ = tenant.getLease(t.Context(), "held")
	_, _ = tenant.do(t.Context(), tenant.api.Patch(types.MergePatchType).AbsPath(coordinationPath, "kube-system", "leases", "held").Body([]byte(`{"metadata":{"resourceVersion":"1"},"spec":{"holderIdentity":"x"}}`)))
	t.Logf("the biggest head of 8 answers of a kube-apiserver, HTTP/1.1 counted: %d bytes; as an HTTP/2 header list: %d bytes; maxResponseHeaderBytes %d", biggest, h2, maxResponseHeaderBytes)
	if biggest == 0 || biggest*16 > maxResponseHeaderBytes || h2*16 > maxResponseHeaderBytes {
		t.Errorf("the biggest head is %d bytes, %d as an HTTP/2 list: want 1/16 of %d at most", biggest, h2, maxResponseHeaderBytes)
	}
}
