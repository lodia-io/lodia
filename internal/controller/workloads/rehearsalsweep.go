// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/jsonscan"
	"go.lodia.io/lodia/internal/ownership"
	"go.lodia.io/lodia/internal/tenantclient"
)

const (
	// sweepBudget is how long a pass reads, within its minute; the sweep goes
	// on in the next pass, which comes at once.
	sweepBudget = 45 * time.Second
	// readTimeout is how long one read may take: a conversion webhook's call
	// waits at most 30 s
	// (https://github.com/kubernetes/kubernetes/blob/v1.37.2/staging/src/k8s.io/apiserver/pkg/util/webhook/webhook.go#L36-L38).
	readTimeout = 30 * time.Second
	sweepAgain  = time.Millisecond
	// readAgain is how soon a read whose error was transient is sent again.
	readAgain = 5 * time.Second

	listPage = 500
	// crdPage keeps a page of CustomResourceDefinitions, each at most etcd's
	// request size of 1.5 MiB, under tenantclient.MaxResponseBytes.
	crdPage = 5

	maxGroups    = 10000
	maxVersions  = 100
	maxResources = 10000
	maxVerbs     = 32
	// maxContinueRaw bounds a continue token, which carries the key of the
	// next object.
	maxContinueRaw = 64 << 10
	// maxKindRaw is APIGroupDiscoveryList, the longest kind a sweep compares
	// with, with every character escaped.
	maxKindRaw = 6 * len("APIGroupDiscoveryList")
	// maxFindings is how many findings of each kind a verdict names.
	maxFindings = 5

	discoveryAccept = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
	tableAccept     = "application/json;as=Table;v=v1;g=meta.k8s.io"
)

var (
	errBudget = errors.New("the pass's reads are over")
	errAgain  = errors.New("a read is sent again")
)

type stage int

const (
	discovering stage = iota
	readingAPIServices
	readingCRDs
	listing
	checking
	swept
)

// sweep is a copy's sweep, kept in memory between passes by the copy's UID: a
// restarted operator sweeps a copy from its start, and the copy's continue
// tokens hold, since it does not compact.
type sweep struct {
	controlPlane types.UID
	key          types.NamespacedName
	copy, tenant *tenantclient.Client
	stage        stage
	token        string

	copyAPIs, tenantAPIs map[string]served
	aggregated           map[string]bool
	// crds are the resources at each version the copy's own
	// CustomResourceDefinitions serve, keyed by group-version and plural.
	crds     map[string]bool
	webhooks map[string]bool

	lists   []list
	next    int
	objects int
	failed  []list
	checked int

	stopping, reported []string
	// failing is the read the sweep sends again.
	failing *failing
}

// failing is a read whose error was transient: what it read, its error, and
// whether it stops the step once the copy's 15 minutes are over, as a read
// through the copy on a connection it accepted and the tenant's check of a
// resource the copy failed do.
type failing struct {
	what, err string
	stops     bool
}

// served is a group-version in a discovery: whether it is stale, and its
// resources that list.
type served struct {
	group, version string
	stale          bool
	resources      []string
}

type group struct {
	name     string
	versions []served
}

type list struct {
	path, name, err string
}

func (r *RehearsalReconciler) sweep(ctx context.Context, cp *lodiav1alpha1.ControlPlane, version string, verdict *metav1.Condition, pod *corev1.Pod) (ctrl.Result, error) {
	s, err := r.sweepOf(ctx, cp, pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	left := copyDeadline - r.Clock.Since(pod.CreationTimestamp.Time)
	pass, cancel := context.WithTimeout(ctx, min(sweepBudget, left))
	defer cancel()
	switch err := s.run(pass); {
	case errors.Is(err, errBudget):
		return ctrl.Result{RequeueAfter: min(sweepAgain, left)}, r.rehearsingNow(ctx, cp, version, verdict)
	case errors.Is(err, errAgain):
		return ctrl.Result{RequeueAfter: min(readAgain, left)}, r.rehearsingNow(ctx, cp, version, verdict)
	case err != nil:
		return ctrl.Result{}, err
	}
	took := r.Clock.Since(pod.CreationTimestamp.Time).Round(time.Second)
	if len(s.stopping) > 0 {
		return r.conclude(ctx, cp, version, pod, metav1.ConditionFalse, lodiav1alpha1.ReasonListFailed,
			fmt.Sprintf("through the copy at %s: %s", version, findings(s.stopping))+s.reports())
	}
	return r.conclude(ctx, cp, version, pod, metav1.ConditionTrue, lodiav1alpha1.ReasonPassed,
		fmt.Sprintf("the copy at %s listed %d objects of %d resources, %s after its creation", version, s.objects, len(s.lists), took)+s.reports())
}

// expired is the verdict on a copy whose 15 minutes are over while its API
// server runs: ListFailed while a read its sweep sends again stops the step,
// and CopyFailed otherwise, with that read's error.
func (r *RehearsalReconciler) expired(ctx context.Context, cp *lodiav1alpha1.ControlPlane, version string, pod *corev1.Pod) (ctrl.Result, error) {
	r.mu.Lock()
	s := r.sweeps[pod.UID]
	r.mu.Unlock()
	if s != nil && s.failing != nil && s.failing.stops {
		s.stopping = append(s.stopping, fmt.Sprintf("%s still fails after %s: %s", s.failing.what, copyDeadline, s.failing.err))
		return r.conclude(ctx, cp, version, pod, metav1.ConditionFalse, lodiav1alpha1.ReasonListFailed,
			fmt.Sprintf("through the copy at %s: %s", version, findings(s.stopping))+s.reports())
	}
	message := fmt.Sprintf("the copy at %s gave no verdict within %s of its creation", version, copyDeadline)
	if s != nil && s.failing != nil {
		message += ": " + s.failing.what + ": " + s.failing.err
	}
	return r.conclude(ctx, cp, version, pod, metav1.ConditionFalse, lodiav1alpha1.ReasonCopyFailed, message)
}

func (s *sweep) reports() string {
	if len(s.reported) == 0 {
		return ""
	}
	return "; reported: " + findings(s.reported)
}

func findings(all []string) string {
	out := strings.Join(all[:min(len(all), maxFindings)], "; ")
	if len(all) > maxFindings {
		out += fmt.Sprintf("; and %d more", len(all)-maxFindings)
	}
	return out
}

// sweepOf is the copy's sweep, begun with a client of the copy at its pod's
// address and the API server's name, and one of the tenant, both from
// <cp>-lodia-kubeconfig.
func (r *RehearsalReconciler) sweepOf(ctx context.Context, cp *lodiav1alpha1.ControlPlane, pod *corev1.Pod) (*sweep, error) {
	r.mu.Lock()
	s := r.sweeps[pod.UID]
	r.mu.Unlock()
	if s != nil {
		return s, nil
	}
	kubeconfig, err := r.kubeconfig(ctx, cp)
	if err != nil {
		return nil, err
	}
	opts := tenantclient.Options{Connections: 1, Timeout: r.Timeout, Dial: r.Dial}
	cfg, err := tenantclient.Config(kubeconfig)
	if err != nil {
		return nil, err
	}
	server, err := url.Parse(cfg.Host)
	if err != nil {
		return nil, err
	}
	if cfg.ServerName == "" {
		cfg.ServerName = server.Hostname()
	}
	cfg.Host = "https://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(kubernetesPort))
	copyClient, err := tenantclient.New(cfg, opts)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tenants == nil {
		r.tenants = tenantclient.NewClients(opts)
	}
	tenant, err := r.tenants.Of(client.ObjectKeyFromObject(cp), kubeconfig)
	if err != nil {
		copyClient.Close()
		return nil, err
	}
	if r.sweeps == nil {
		r.sweeps = map[types.UID]*sweep{}
	}
	s = &sweep{controlPlane: cp.UID, key: client.ObjectKeyFromObject(cp), copy: copyClient, tenant: tenant}
	r.sweeps[pod.UID] = s
	return s, nil
}

// endSweeps forgets the sweeps of cp's copies but that of keep.
func (r *RehearsalReconciler) endSweeps(cp, keep types.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for uid, s := range r.sweeps {
		if s.controlPlane == cp && uid != keep {
			s.copy.Close()
			delete(r.sweeps, uid)
		}
	}
}

// kubeconfig is Lodia's kubeconfig of the tenant. A copy that shows it kept by
// another owner is read again live, and the pass decides on that.
func (r *RehearsalReconciler) kubeconfig(ctx context.Context, cp *lodiav1alpha1.ControlPlane) ([]byte, error) {
	key := client.ObjectKey{Namespace: cp.Namespace, Name: lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)}
	secret := &corev1.Secret{}
	err := r.Secrets.Get(ctx, key, secret)
	if err == nil && ownership.ControlledElsewhere(secret, cp) != nil {
		secret = &corev1.Secret{}
		err = r.Reader.Get(ctx, key, secret)
	}
	if err != nil {
		return nil, fmt.Errorf("read Secret %s: %w", key.Name, err)
	}
	if owner := ownership.ControlledElsewhere(secret, cp); owner != nil {
		return nil, fmt.Errorf("secret %s is kept by %s %s (UID %s)", key.Name, owner.Kind, owner.Name, owner.UID)
	}
	return secret.Data[lodiav1alpha1.KubeconfigKey], nil
}

// run reads on from where the sweep stands until it is swept, or returns
// errBudget once the pass's reads are over, or errAgain for a read whose error
// was transient, which the next pass sends again. Any other error of a read
// through the copy is a finding.
func (s *sweep) run(pass context.Context) error {
	for {
		var err error
		switch s.stage {
		case discovering:
			err = s.discover(pass)
		case readingAPIServices:
			err = s.readAPIServices(pass)
		case readingCRDs:
			err = s.readCRDs(pass)
		case listing:
			err = s.listCopy(pass)
		case checking:
			err = s.checkTenant(pass)
		case swept:
			return nil
		}
		if err != nil {
			return err
		}
		s.failing = nil
	}
}

// again keeps a read whose error was transient to be sent again.
func (s *sweep) again(what string, err error, stops bool) error {
	s.failing = &failing{what: what, err: quoted(err.Error()), stops: stops}
	return errAgain
}

// transient says whether a read may answer when sent again: one that got no
// answer, none in time or one cut mid-body, kube-apiserver's 429, 503, 504
// and ServerTimeout, and an answer that is no Status but carries Retry-After.
func transient(err error) bool {
	_, delay := apierrors.SuggestsClientDelay(err)
	return delay || isTransport(err) || cut(err) || errors.Is(err, errSlow) || apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(err) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err)
}

// cut says whether the body of an answer ended early, its connection closed
// or reset while it was read: kube-apiserver ends so an answer it began when
// the request times out
// (https://github.com/kubernetes/apiserver/blob/v0.37.2/pkg/server/filters/timeout.go#L274),
// and so does a replica that dies mid-answer.
func cut(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)
}

// read sends one read and gives its body. Its error is errBudget once the
// pass's reads are over, errSlow once the read itself is, and otherwise the
// answer's, or net/http's when no answer came.
func read(pass context.Context, c *tenantclient.Client, path, accept string, params ...string) ([]byte, error) {
	if pass.Err() != nil {
		return nil, errBudget
	}
	ctx, cancel := context.WithTimeout(pass, readTimeout)
	defer cancel()
	request := c.Get().AbsPath(path).SetHeader("Accept", accept)
	for i := 0; i+1 < len(params); i += 2 {
		if params[i+1] != "" {
			request = request.Param(params[i], params[i+1])
		}
	}
	body, err := c.Do(ctx, request)
	switch {
	case err != nil && pass.Err() != nil:
		return nil, errBudget
	case err != nil && ctx.Err() != nil:
		return nil, errSlow
	}
	return body, err
}

var errSlow = fmt.Errorf("no answer within %s", readTimeout)

// failedThroughCopy ends the sweep on a read of the copy that is not a list
// of a resource: without it nothing can be listed. A transient error is sent
// again.
func (s *sweep) failedThroughCopy(what string, err error) error {
	switch {
	case errors.Is(err, errBudget):
		return err
	case transient(err):
		return s.again(what, err, !isTransport(err))
	}
	s.stopping = append(s.stopping, what+": "+quoted(err.Error()))
	s.stage = swept
	return nil
}

func (s *sweep) discover(pass context.Context) error {
	copyAPIs, err := discovery(pass, s.copy)
	if err != nil {
		return s.failedThroughCopy("its discovery", err)
	}
	tenantAPIs, err := discovery(pass, s.tenant)
	if err != nil {
		if errors.Is(err, errBudget) {
			return err
		}
		return s.again("the tenant's discovery", err, false)
	}
	s.copyAPIs, s.tenantAPIs = copyAPIs, tenantAPIs
	s.aggregated, s.crds, s.webhooks = map[string]bool{}, map[string]bool{}, map[string]bool{}
	s.stage = readingAPIServices
	return nil
}

func discovery(pass context.Context, c *tenantclient.Client) (map[string]served, error) {
	out := map[string]served{}
	for _, path := range []string{"/api", "/apis"} {
		body, err := read(pass, c, path, discoveryAccept)
		groups, err := tenantclient.Decode(body, err, discoveryOf)
		if err != nil {
			return nil, err
		}
		for _, g := range *groups {
			for _, v := range g.versions {
				v.group = g.name
				out[groupVersion(g.name, v.version)] = v
			}
		}
	}
	return out, nil
}

func groupVersion(group, version string) string {
	if group == "" {
		return version
	}
	return group + "/" + version
}

func (s *sweep) readAPIServices(pass context.Context) error {
	body, err := read(pass, s.copy, "/apis/apiregistration.k8s.io/v1/apiservices", "application/json", "limit", strconv.Itoa(listPage), "continue", s.token)
	page, err := tenantclient.Decode(body, err, apiServicesOf)
	if err != nil {
		return s.failedThroughCopy("its list of APIServices", err)
	}
	for _, a := range page.items {
		if a.service {
			s.aggregated[groupVersion(a.group, a.version)] = true
		}
	}
	if s.token = page.next; s.token == "" {
		s.stage = readingCRDs
	}
	return nil
}

func (s *sweep) readCRDs(pass context.Context) error {
	body, err := read(pass, s.copy, "/apis/apiextensions.k8s.io/v1/customresourcedefinitions", "application/json", "limit", strconv.Itoa(crdPage), "continue", s.token)
	page, err := tenantclient.Decode(body, err, crdsOf)
	if err != nil {
		return s.failedThroughCopy("its list of CustomResourceDefinitions", err)
	}
	for _, c := range page.items {
		if c.webhook {
			s.webhooks[c.group+"/"+c.plural] = true
		}
		for _, v := range c.served {
			s.crds[groupVersion(c.group, v)+"/"+c.plural] = true
		}
	}
	if s.token = page.next; s.token == "" {
		s.plan()
		s.stage = listing
	}
	return nil
}

// plan lists every resource of every group-version the copy serves but those
// an APIService serves from a Service and those a webhook converts, which it
// reports, as it reports what one side serves alone, and stops on a resource
// the tenant serves that the copy's own CustomResourceDefinitions serve and
// its discovery does not: one created after the snapshot is the tenant's
// alone.
func (s *sweep) plan() {
	webhooks := map[string]bool{}
	for _, key := range slices.Sorted(maps.Keys(s.copyAPIs)) {
		copyServes, tenant := s.copyAPIs[key], s.tenantAPIs[key]
		_, both := s.tenantAPIs[key]
		switch {
		case s.aggregated[key]:
			s.reported = append(s.reported, key+" is served by an APIService that names a Service, not read through a copy")
			continue
		case copyServes.stale:
			s.reported = append(s.reported, "the copy's discovery marks "+key+" stale")
			continue
		case !both:
			s.reported = append(s.reported, "only the copy serves "+key)
		}
		for _, resource := range copyServes.resources {
			if s.webhooks[copyServes.group+"/"+resource] {
				if name := copyServes.group + "/" + resource; !webhooks[name] {
					webhooks[name] = true
					s.reported = append(s.reported, resource+"."+copyServes.group+" is converted by a webhook, not read through a copy")
				}
				continue
			}
			if both && !slices.Contains(tenant.resources, resource) {
				s.reported = append(s.reported, "only the copy serves "+resource+" of "+key)
			}
			s.lists = append(s.lists, list{path: resourcePath(copyServes.group, copyServes.version, resource), name: resource + " of " + key})
		}
	}
	for _, key := range slices.Sorted(maps.Keys(s.tenantAPIs)) {
		tenant := s.tenantAPIs[key]
		copyServes, both := s.copyAPIs[key]
		held := slices.ContainsFunc(tenant.resources, func(resource string) bool { return s.crds[key+"/"+resource] })
		switch {
		case s.aggregated[key] && both, tenant.stale:
		case !both && !held:
			s.reported = append(s.reported, "only the tenant serves "+key)
		default:
			for _, resource := range tenant.resources {
				switch {
				case slices.Contains(copyServes.resources, resource):
				case s.crds[key+"/"+resource]:
					s.stopping = append(s.stopping, "the copy does not serve "+resource+" of "+key+", which its own CustomResourceDefinition serves")
				default:
					s.reported = append(s.reported, "only the tenant serves "+resource+" of "+key)
				}
			}
		}
	}
}

func resourcePath(group, version, resource string) string {
	if group == "" {
		return "/api/" + version + "/" + resource
	}
	return "/apis/" + group + "/" + version + "/" + resource
}

// listCopy reads the next page of the resource listing now, as a Table of
// rows without objects, which it counts.
func (s *sweep) listCopy(pass context.Context) error {
	if s.next == len(s.lists) {
		s.stage = checking
		return nil
	}
	l := s.lists[s.next]
	page, err := readTable(pass, s.copy, l.path, s.token)
	switch {
	case errors.Is(err, errBudget):
		return err
	case transient(err):
		return s.again("the list of "+l.name, err, !isTransport(err))
	case err != nil:
		l.err = quoted(err.Error())
		s.failed = append(s.failed, l)
		s.next, s.token = s.next+1, ""
		return nil
	}
	s.objects += page.rows
	if s.token = page.next; s.token == "" {
		s.next++
	}
	return nil
}

// checkTenant lists through the tenant each resource that failed through the
// copy: one that lists there stops the step, and one whose list fails there
// too, for a reason that is not transient, fails before the step and is
// reported with each side's error.
func (s *sweep) checkTenant(pass context.Context) error {
	if s.checked == len(s.failed) {
		s.stage = swept
		return nil
	}
	l := s.failed[s.checked]
	page, err := readTable(pass, s.tenant, l.path, s.token)
	switch {
	case errors.Is(err, errBudget):
		return err
	case apierrors.IsResourceExpired(err):
		s.token = ""
		return nil
	case transient(err):
		return s.again("the tenant's list of "+l.name+" (through the copy: "+l.err+")", err, true)
	case err != nil:
		s.reported = append(s.reported, l.name+" fails through the tenant too: "+quoted(err.Error())+"; through the copy: "+l.err)
	case page.next != "":
		s.token = page.next
		return nil
	default:
		s.stopping = append(s.stopping, l.name+" lists through the tenant and not through the copy: "+l.err)
	}
	s.checked, s.token = s.checked+1, ""
	return nil
}

// isTransport says whether a read failed before an answer came.
func isTransport(err error) bool {
	_, ok := errors.AsType[*url.Error](err)
	return ok
}

type table struct {
	kind, next string
	rows       int
}

func readTable(pass context.Context, c *tenantclient.Client, path, token string) (*table, error) {
	body, err := read(pass, c, path, tableAccept, "limit", strconv.Itoa(listPage), "includeObject", "None", "continue", token)
	page, err := tenantclient.Decode(body, err, tableOf)
	if err == nil && page.kind != "Table" {
		err = errors.New("the answer is no Table")
	}
	return page, err
}

func tableOf(data []byte) (*table, error) {
	t := &table{}
	err := jsonscan.Walk("the Table", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "kind":
			t.kind, err = jsonscan.Word("kind", value, maxKindRaw)
		case "metadata":
			t.next, err = continueOf(value)
		case "rows":
			switch {
			case jsonscan.Null(value):
			case value[0] != '[':
				err = jsonscan.NotA("rows", "an array")
			default:
				for range jsonscan.Elements(value) {
					t.rows++
				}
			}
		}
		return err
	})
	return t, err
}

func continueOf(metadata []byte) (next string, err error) {
	err = jsonscan.Walk("metadata", metadata, func(key, value []byte) (err error) {
		if string(key) == "continue" {
			next, err = jsonscan.Text("metadata.continue", value, maxContinueRaw)
		}
		return err
	})
	return next, err
}

func discoveryOf(data []byte) (*[]group, error) {
	var (
		kind   string
		groups []group
	)
	err := jsonscan.Walk("the discovery", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "kind":
			kind, err = jsonscan.Word("kind", value, maxKindRaw)
		case "items":
			var items tenantclient.Array[group]
			items, err = tenantclient.ArrayOf("items", value, maxGroups, groupOf)
			if items.Over {
				err = fmt.Errorf("the discovery lists more than %d groups", maxGroups)
			}
			groups = items.Items
		}
		return err
	})
	if err == nil && kind != "APIGroupDiscoveryList" {
		err = errors.New("the answer is no APIGroupDiscoveryList")
	}
	return &groups, err
}

func groupOf(data []byte) (g group, err error) {
	err = jsonscan.Walk("a group", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "metadata":
			err = jsonscan.Walk("metadata", value, func(key, value []byte) (err error) {
				if string(key) == "name" {
					g.name, err = tenantclient.ObjectName("metadata.name", value)
				}
				return err
			})
		case "versions":
			var versions tenantclient.Array[served]
			versions, err = tenantclient.ArrayOf("versions", value, maxVersions, versionOf)
			if versions.Over {
				err = fmt.Errorf("a group lists more than %d versions", maxVersions)
			}
			g.versions = versions.Items
		}
		return err
	})
	return g, err
}

func versionOf(data []byte) (v served, err error) {
	err = jsonscan.Walk("a version", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "version":
			v.version, err = tenantclient.ObjectName("version", value)
		case "freshness":
			var freshness string
			freshness, err = jsonscan.Word("freshness", value, tenantclient.MaxWordRaw)
			v.stale = freshness == "Stale"
		case "resources":
			var resources tenantclient.Array[string]
			resources, err = tenantclient.ArrayOf("resources", value, maxResources, listedResource)
			if resources.Over {
				err = fmt.Errorf("a version lists more than %d resources", maxResources)
			}
			for _, r := range resources.Items {
				if r != "" {
					v.resources = append(v.resources, r)
				}
			}
		}
		return err
	})
	return v, err
}

// listedResource is a resource's name when its verbs hold list, and empty
// otherwise.
func listedResource(data []byte) (string, error) {
	var (
		name  string
		lists bool
	)
	err := jsonscan.Walk("a resource", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "resource":
			name, err = tenantclient.ObjectName("resource", value)
		case "verbs":
			var verbs tenantclient.Array[string]
			verbs, err = tenantclient.ArrayOf("verbs", value, maxVerbs, func(v []byte) (string, error) { return jsonscan.Word("verb", v, tenantclient.MaxWordRaw) })
			lists = slices.Contains(verbs.Items, "list")
		}
		return err
	})
	if !lists {
		return "", err
	}
	return name, err
}

type apiService struct {
	group, version string
	service        bool
}

type page[T any] struct {
	items []T
	next  string
}

func apiServicesOf(data []byte) (*page[apiService], error) {
	return pageOf(data, listPage, func(item []byte) (a apiService, err error) {
		err = jsonscan.Walk("an APIService", item, func(key, value []byte) (err error) {
			if string(key) != "spec" {
				return nil
			}
			return jsonscan.Walk("spec", value, func(key, value []byte) (err error) {
				switch string(key) {
				case "group":
					a.group, err = tenantclient.ObjectName("spec.group", value)
				case "version":
					a.version, err = tenantclient.ObjectName("spec.version", value)
				case "service":
					a.service = !jsonscan.Null(value)
				}
				return err
			})
		})
		return a, err
	})
}

type crd struct {
	group, plural string
	webhook       bool
	served        []string
}

func crdsOf(data []byte) (*page[crd], error) {
	return pageOf(data, crdPage, func(item []byte) (c crd, err error) {
		err = jsonscan.Walk("a CustomResourceDefinition", item, func(key, value []byte) (err error) {
			if string(key) != "spec" {
				return nil
			}
			return jsonscan.Walk("spec", value, func(key, value []byte) (err error) {
				switch string(key) {
				case "group":
					c.group, err = tenantclient.ObjectName("spec.group", value)
				case "names":
					err = jsonscan.Walk("spec.names", value, func(key, value []byte) (err error) {
						if string(key) == "plural" {
							c.plural, err = tenantclient.ObjectName("spec.names.plural", value)
						}
						return err
					})
				case "conversion":
					err = jsonscan.Walk("spec.conversion", value, func(key, value []byte) (err error) {
						if string(key) == "strategy" {
							var strategy string
							strategy, err = jsonscan.Word("spec.conversion.strategy", value, tenantclient.MaxWordRaw)
							c.webhook = strategy == "Webhook"
						}
						return err
					})
				case "versions":
					var versions tenantclient.Array[string]
					versions, err = tenantclient.ArrayOf("spec.versions", value, maxVersions, servedVersion)
					if versions.Over {
						err = fmt.Errorf("a CustomResourceDefinition lists more than %d versions", maxVersions)
					}
					for _, v := range versions.Items {
						if v != "" {
							c.served = append(c.served, v)
						}
					}
				}
				return err
			})
		})
		return c, err
	})
}

// servedVersion is a CustomResourceDefinition version's name while it is
// served, and empty otherwise.
func servedVersion(data []byte) (string, error) {
	var (
		name    string
		serving bool
	)
	err := jsonscan.Walk("a version", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "name":
			name, err = tenantclient.ObjectName("name", value)
		case "served":
			var b *bool
			b, err = jsonscan.Boolean("served", value)
			serving = b != nil && *b
		}
		return err
	})
	if !serving {
		return "", err
	}
	return name, err
}

// pageOf reads a list of at most most items, each with item, and its
// continue token.
func pageOf[T any](data []byte, most int, item func([]byte) (T, error)) (*page[T], error) {
	p := &page[T]{}
	err := jsonscan.Walk("the list", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "metadata":
			p.next, err = continueOf(value)
		case "items":
			var items tenantclient.Array[T]
			items, err = tenantclient.ArrayOf("items", value, most, item)
			if items.Over {
				err = fmt.Errorf("a page lists more than the %d items it asked for", most)
			}
			p.items = items.Items
		}
		return err
	})
	return p, err
}
