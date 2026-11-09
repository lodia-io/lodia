// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package testenv starts the API server a controller's tests run against, and
// runs the controller under its generated ClusterRole. It belongs to test
// files: no other package imports it, so none of it reaches a binary.
//
// A package's TestMain calls these in order. What fails prints and exits 1,
// after stopping what has started.
package testenv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gardener/etcd-druid/api/core/v1alpha1/crds"
	authorizationv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/operator"
)

func Available() bool {
	return os.Getenv("KUBEBUILDER_ASSETS") != ""
}

// SkipUnlessAvailable exits 0 without running any test when
// KUBEBUILDER_ASSETS is not set.
func SkipUnlessAvailable(what string) {
	if !Available() {
		fmt.Printf("skipping %s: KUBEBUILDER_ASSETS is not set; run make test\n", what)
		os.Exit(0)
	}
}

func Scheme(extra ...func(*runtime.Scheme) error) *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(lodiav1alpha1.AddToScheme(scheme))
	for _, add := range extra {
		utilruntime.Must(add(scheme))
	}
	return scheme
}

// DruidCRDs returns etcd-druid's CRDs for Kubernetes 1.35, the minor of
// ENVTEST_K8S_VERSION.
func DruidCRDs() ([]*apiextensionsv1.CustomResourceDefinition, error) {
	manifests, err := crds.GetAll("1.35")
	if err != nil {
		return nil, err
	}
	var out []*apiextensionsv1.CustomResourceDefinition
	for name, manifest := range manifests {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal([]byte(manifest), crd); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		out = append(out, crd)
	}
	return out, nil
}

// Options is what differs between suites: the kinds the clients know (Scheme()
// when nil), the CRDs served beside Lodia's, and whether the API server
// enforces owner references.
type Options struct {
	Scheme                 *runtime.Scheme
	CRDDirs                []string
	CRDs                   []*apiextensionsv1.CustomResourceDefinition
	EnforceOwnerReferences bool
}

type Env struct {
	Scheme *runtime.Scheme
	Client client.WithWatch

	env     *envtest.Environment
	cfg     *rest.Config
	root    string
	cancel  context.CancelFunc
	manager chan error
}

func Start(opts Options) *Env {
	root, err := repositoryRoot()
	if err != nil {
		exit("find the repository", err)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     append([]string{filepath.Join(root, "api", "crds")}, opts.CRDDirs...),
		ErrorIfCRDPathMissing: true,
		CRDs:                  opts.CRDs,
		UseExistingCluster:    new(false),
	}
	if opts.EnforceOwnerReferences {
		env.ControlPlane.GetAPIServer().Configure().Append("enable-admission-plugins", "OwnerReferencesPermissionEnforcement")
	}
	cfg, err := env.Start()
	if err != nil {
		exit("start envtest", err)
	}
	e := &Env{Scheme: opts.Scheme, env: env, cfg: cfg, root: root}
	if e.Scheme == nil {
		e.Scheme = Scheme()
	}
	if e.Client, err = client.NewWithWatch(cfg, client.Options{Scheme: e.Scheme}); err != nil {
		e.Fatal("client", err)
	}
	return e
}

// Fatal stops the manager and the API server, if started, and exits 1.
func (e *Env) Fatal(what string, err error) {
	if e.cancel != nil {
		e.cancel()
	}
	_ = e.env.Stop()
	exit(what, err)
}

// AsController returns a configuration that acts as the user lodia-<name>,
// bound to the ClusterRole in charts/lodia/files/rbac/<name>.yaml and
// nothing else.
func (e *Env) AsController(name string) *rest.Config {
	cfg, err := e.impersonate(name)
	if err != nil {
		e.Fatal("the controller's ClusterRole", err)
	}
	return cfg
}

func (e *Env) impersonate(name string) (*rest.Config, error) {
	user := "lodia-" + name
	ctx := context.Background()
	manifest, err := os.ReadFile(filepath.Join(e.root, "charts", "lodia", "files", "rbac", name+".yaml"))
	if err != nil {
		return nil, err
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.UnmarshalStrict(manifest, role); err != nil {
		return nil, err
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: user},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}
	for _, obj := range []client.Object{role, binding} {
		if err := e.Client.Create(ctx, obj); err != nil {
			return nil, err
		}
	}
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               user,
			ResourceAttributes: &authorizationv1.ResourceAttributes{Group: lodiav1alpha1.GroupVersion.Group, Resource: "controlplanes", Verb: "watch"},
		}}
		if err := e.Client.Create(ctx, review); err != nil {
			return false, err
		}
		return review.Status.Allowed, nil
	})
	if err != nil {
		return nil, err
	}
	controller := rest.CopyConfig(e.cfg)
	controller.Impersonate = rest.ImpersonationConfig{UserName: user}
	return controller, nil
}

// Manager returns a manager that acts as role, under the cache the operator
// builds and with its reads failing on a kind that has no informer.
func (e *Env) Manager(role *rest.Config, gatewayAPI bool) ctrl.Manager {
	cache, err := operator.Cache(gatewayAPI)
	if err != nil {
		e.Fatal("cache options", err)
	}
	cache.ReaderFailOnMissingInformer = true
	mgr, err := ctrl.NewManager(role, ctrl.Options{Scheme: e.Scheme, Metrics: metricsserver.Options{BindAddress: "0"}, Cache: cache})
	if err != nil {
		e.Fatal("manager", err)
	}
	return mgr
}

// StartManager runs mgr in the background, logging, until Finish, and returns
// the context that ends it.
func (e *Env) StartManager(mgr ctrl.Manager) context.Context {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.manager = make(chan error, 1)
	go func() { e.manager <- mgr.Start(ctx) }()
	return ctx
}

// Finish stops the manager, if started, and the API server, and returns code
// as the exit code unless the manager failed.
func (e *Env) Finish(code int) int {
	if e.cancel != nil {
		e.cancel()
		if err := <-e.manager; err != nil {
			fmt.Fprintln(os.Stderr, "manager:", err)
			code = 1
		}
	}
	if err := e.env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
	}
	return code
}

// Eventually fails the test unless check returns nil within 30 seconds.
func Eventually(t *testing.T, check func() error) {
	t.Helper()
	EventuallyWithin(t, 30*time.Second, check)
}

func EventuallyWithin(t *testing.T, timeout time.Duration, check func() error) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		last = check()
		return last == nil, nil
	})
	if err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}

func exit(what string, err error) {
	fmt.Fprintln(os.Stderr, what+":", err)
	os.Exit(1)
}

func repositoryRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above %s", dir)
		}
		dir = parent
	}
}
