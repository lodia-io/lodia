// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc/connectivity"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/operator"
	"go.lodia.io/lodia/internal/testenv"
)

var sampledAt = time.Date(2026, 11, 11, 8, 0, 0, 0, time.UTC)

func sampledControlPlane(name string, uid types.UID) (*lodiav1alpha1.ControlPlane, *druidv1alpha1.Etcd) {
	cp := controlPlane("tenants", name, "")
	cp.UID = uid
	etcd := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{
		Name: name + "-etcd", Namespace: cp.Namespace, UID: types.UID("etcd-of-" + string(uid)),
		Labels:          map[string]string{lodiav1alpha1.ControlPlaneLabel: name},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, lodiav1alpha1.GroupVersion.WithKind("ControlPlane"))},
	}}
	etcd.Annotations = map[string]string{lodiav1alpha1.EtcdFormedAnnotation: string(etcd.UID)}
	return cp, etcd
}

func fakeCache(objs ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().WithScheme(k8s.Scheme()).WithObjects(objs...).Build()
}

func dialURL(endpoint string) func(context.Context, *druidv1alpha1.Etcd) (*clientv3.Client, error) {
	return func(context.Context, *druidv1alpha1.Etcd) (*clientv3.Client, error) {
		return clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	}
}

func samplePass(t *testing.T, r *datastore.SampleReconciler, key types.NamespacedName) ctrl.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatalf("the pass returned %v, want no error on any path", err)
	}
	return result
}

func servedSample(t *testing.T, r *datastore.SampleReconciler, cp *lodiav1alpha1.ControlPlane) datastore.EtcdSample {
	t.Helper()
	sample, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID)
	if !ok {
		t.Fatalf("no sample of %s served", cp.Name)
	}
	return sample
}

func clientURL(cfg *embed.Config) string { return cfg.ListenClientUrls[0].String() }

func closedURL(t *testing.T) url.URL {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return url.URL{Scheme: "http", Host: l.Addr().String()}
}

func etcdMember(t *testing.T, name string) *embed.Config {
	t.Helper()
	cfg := testserver.NewTestConfig(t)
	cfg.Name = name
	cfg.InitialClusterToken = t.Name()
	return cfg
}

func startMembers(t *testing.T, cfgs ...*embed.Config) []func() {
	t.Helper()
	stops := make([]func(), len(cfgs))
	var ready []<-chan struct{}
	for i, cfg := range cfgs {
		e, err := embed.StartEtcd(cfg)
		if err != nil {
			t.Fatal(err)
		}
		stops[i] = sync.OnceFunc(e.Close)
		t.Cleanup(stops[i])
		ready = append(ready, e.Server.ReadyNotify())
	}
	for _, r := range ready {
		select {
		case <-r:
		case <-time.After(60 * time.Second):
			t.Fatal("an etcd member took too long to start")
		}
	}
	return stops
}

func etcdCluster(t *testing.T, n int, configure func(i int, cfg *embed.Config)) ([]func(), []*embed.Config) {
	t.Helper()
	cfgs := make([]*embed.Config, n)
	var initial []string
	for i := range cfgs {
		cfgs[i] = etcdMember(t, "m"+strconv.Itoa(i))
		initial = append(initial, cfgs[i].Name+"="+cfgs[i].AdvertisePeerUrls[0].String())
	}
	for i, cfg := range cfgs {
		cfg.InitialCluster = strings.Join(initial, ",")
		if configure != nil {
			configure(i, cfg)
		}
	}
	return startMembers(t, cfgs...), cfgs
}

func etcdClient(t *testing.T, endpoint string) *clientv3.Client {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func dbSize(t *testing.T, c *clientv3.Client, endpoint string) int64 {
	t.Helper()
	status, err := c.Status(t.Context(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return status.DbSize
}

func sampler(objs ...client.Object) (*datastore.SampleReconciler, *clocktesting.FakeClock) {
	clk := clocktesting.NewFakeClock(sampledAt)
	return &datastore.SampleReconciler{Cache: fakeCache(objs...), Clock: clk}, clk
}

func TestOneMemberGivesItsLeaderAndItsSize(t *testing.T) {
	_, cfgs := etcdCluster(t, 1, nil)
	cp, etcd := sampledControlPlane("one", "uid-one")
	r, clk := sampler(cp, etcd)
	r.EtcdClient = dialURL(clientURL(cfgs[0]))
	samplePass(t, r, client.ObjectKeyFromObject(cp))
	clk.Step(5 * time.Second)
	got := servedSample(t, r, cp)
	want := dbSize(t, etcdClient(t, clientURL(cfgs[0])), clientURL(cfgs[0]))
	if !got.HasLeader || got.DBSize == nil || *got.DBSize != want || len(got.Alarms) > 0 {
		t.Errorf("the sample of one member is %+v with size %v, want its leader, its size %d and no alarm", got, got.DBSize, want)
	}
}

func TestThreeMembersGiveTheirLeaderWhileAQuorumAnswersAndTheLargestSize(t *testing.T) {
	members, cfgs := etcdCluster(t, 3, nil)
	c := etcdClient(t, clientURL(cfgs[0]))
	value := strings.Repeat("v", 10<<10)
	for i := range 300 {
		if _, err := c.Put(t.Context(), fmt.Sprintf("/bulk/%03d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	deleted, err := c.Delete(t.Context(), "/bulk/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Compact(t.Context(), deleted.Header.GetRevision(), clientv3.WithCompactPhysical()); err != nil {
		t.Fatal(err)
	}
	list, err := c.MemberList(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	urlOf, stop := map[string]string{}, map[string]func(){}
	for i, cfg := range cfgs {
		urlOf[cfg.Name], stop[cfg.Name] = clientURL(cfg), members[i]
	}
	var order []string
	idOf := map[string]uint64{}
	for _, m := range list.Members {
		order = append(order, m.GetName())
		idOf[m.GetName()] = m.GetID()
	}
	for _, name := range order[:2] {
		if _, err := c.Defragment(t.Context(), urlOf[name]); err != nil {
			t.Fatal(err)
		}
	}
	large := dbSize(t, c, urlOf[order[2]])
	for _, name := range order[:2] {
		if small := dbSize(t, c, urlOf[name]); small >= large {
			t.Fatalf("member %s is %d bytes after its defragmentation, not below %s's %d", name, small, order[2], large)
		}
	}
	status, err := c.Status(t.Context(), urlOf[order[2]])
	if err != nil {
		t.Fatal(err)
	}
	stops := order[:2]
	if status.Leader == idOf[order[0]] {
		stops = []string{order[1], order[0]}
	}

	cp, etcd := sampledControlPlane("three", "uid-three")
	r, clk := sampler(cp, etcd)
	r.EtcdClient = dialURL(urlOf[order[2]])
	check := func(stopped string, wantLeader bool) {
		t.Helper()
		samplePass(t, r, client.ObjectKeyFromObject(cp))
		clk.Step(time.Minute)
		got := servedSample(t, r, cp)
		if got.HasLeader != wantLeader || got.DBSize == nil || *got.DBSize != large {
			t.Errorf("with %s stopped the sample is %+v with size %v, want leader %t and member %s's %d bytes, the largest", stopped, got, got.DBSize, wantLeader, order[2], large)
		}
	}
	check("no member", true)
	stop[stops[0]]()
	check(stops[0], true)
	stop[stops[1]]()
	check(stops[0]+" and "+stops[1], false)
}

func TestALearnerIsNotCountedInTheQuorum(t *testing.T) {
	unreachable := closedURL(t)
	_, cfgs := etcdCluster(t, 2, func(i int, cfg *embed.Config) {
		cfg.StrictReconfigCheck = false
		if i == 1 {
			cfg.AdvertiseClientUrls = []url.URL{unreachable}
		}
	})
	c := etcdClient(t, clientURL(cfgs[0]))
	cp, etcd := sampledControlPlane("learning", "uid-learning")
	r, clk := sampler(cp, etcd)
	r.EtcdClient = dialURL(clientURL(cfgs[0]))
	learner := etcdMember(t, "learner")
	learner.StrictReconfigCheck = false
	added, err := c.MemberAddAsLearner(t.Context(), []string{learner.AdvertisePeerUrls[0].String()})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := c.MemberList(t.Context(), clientv3.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.IndexFunc(listed.Members, func(m *etcdserverpb.Member) bool { return m.GetID() == added.Member.GetID() }); i < 0 || len(listed.Members[i].GetClientURLs()) > 0 {
		t.Fatalf("the learner not started yet is not listed without a client URL: %v", listed.Members)
	}
	samplePass(t, r, client.ObjectKeyFromObject(cp))
	clk.Step(time.Minute)
	if got := servedSample(t, r, cp); got.HasLeader || got.DBSize == nil {
		t.Errorf("one voting member of two answering beside a learner not started yet gives %+v with size %v, want no leader and the member's size", got, got.DBSize)
	}

	learner.InitialCluster = strings.Join([]string{
		cfgs[0].Name + "=" + cfgs[0].AdvertisePeerUrls[0].String(),
		cfgs[1].Name + "=" + cfgs[1].AdvertisePeerUrls[0].String(),
		learner.Name + "=" + learner.AdvertisePeerUrls[0].String(),
	}, ",")
	learner.ClusterState = embed.ClusterStateFlagExisting
	startMembers(t, learner)
	testenv.Eventually(t, func() error {
		list, err := c.MemberList(t.Context())
		if err != nil {
			return err
		}
		for _, m := range list.Members {
			if m.GetID() == added.Member.GetID() && len(m.GetClientURLs()) == 0 {
				return errors.New("the learner has published no client URL yet")
			}
		}
		return nil
	})

	testenv.Eventually(t, func() error {
		voter, err := c.Status(t.Context(), clientURL(cfgs[0]))
		if err != nil {
			return err
		}
		learning, err := c.Status(t.Context(), clientURL(learner))
		if err != nil {
			return err
		}
		if voter.Leader == 0 || learning.Leader != voter.Leader || !learning.IsLearner {
			return fmt.Errorf("the voting member names leader %x, the learner %x, learner %t", voter.Leader, learning.Leader, learning.IsLearner)
		}
		return nil
	})

	samplePass(t, r, client.ObjectKeyFromObject(cp))
	clk.Step(time.Minute)
	if got := servedSample(t, r, cp); got.HasLeader {
		t.Errorf("one voting member of two and a learner naming the leader give %+v, want no leader", got)
	}

	list, err := c.MemberList(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range list.Members {
		if m.GetName() == cfgs[1].Name {
			if _, err := c.MemberRemove(t.Context(), m.GetID()); err != nil {
				t.Fatal(err)
			}
		}
	}
	testenv.Eventually(t, func() error {
		samplePass(t, r, client.ObjectKeyFromObject(cp))
		clk.Step(time.Minute)
		if got := servedSample(t, r, cp); !got.HasLeader {
			return fmt.Errorf("the one voting member and a learner naming the leader give %+v, want the leader", got)
		}
		return nil
	})
}

func TestAMemberListThatFailsGivesNoLeaderAndNoSize(t *testing.T) {
	cp, etcd := sampledControlPlane("unlisted", "uid-unlisted")
	r, clk := sampler(cp, etcd)
	unreachable := closedURL(t)
	r.EtcdClient = dialURL(unreachable.String())
	samplePass(t, r, client.ObjectKeyFromObject(cp))
	clk.Step(5 * time.Second)
	if got := servedSample(t, r, cp); got.HasLeader || got.DBSize != nil || len(got.Alarms) > 0 {
		t.Errorf("an etcd whose member list fails gives %+v with size %v, want no leader, no size and no alarm", got, got.DBSize)
	}
}

func TestAClientThatLeavesThePassTooLittleOfItsDeadlineKeepsNoSample(t *testing.T) {
	_, cfgs := etcdCluster(t, 1, nil)
	for _, c := range []struct {
		left   time.Duration
		sample bool
	}{
		{2 * time.Millisecond, false},
		{1250 * time.Millisecond, true},
	} {
		t.Run(c.left.String()+" left", func(t *testing.T) {
			cp, etcd := sampledControlPlane("slow", "uid-slow")
			r, clk := sampler(cp, etcd)
			r.EtcdClient = func(ctx context.Context, e *druidv1alpha1.Etcd) (*clientv3.Client, error) {
				deadline, _ := ctx.Deadline()
				time.Sleep(time.Until(deadline) - c.left)
				return dialURL(clientURL(cfgs[0]))(ctx, e)
			}
			samplePass(t, r, client.ObjectKeyFromObject(cp))
			clk.Step(5 * time.Second)
			got, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID)
			if ok != c.sample || ok && !got.HasLeader {
				t.Errorf("a healthy etcd whose client comes %s before the pass's deadline is served %+v, %t, want %t and any sample with its leader", c.left, got, ok, c.sample)
			}
		})
	}
}

func TestEveryPassClosesItsClient(t *testing.T) {
	_, cfgs := etcdCluster(t, 1, nil)
	for _, c := range []struct {
		name    string
		timeout time.Duration
		sample  bool
	}{
		{"that samples", 5 * time.Second, true},
		{"left too little time for the etcd's calls", 500 * time.Millisecond, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp, etcd := sampledControlPlane("closing", "uid-closing")
			r, clk := sampler(cp, etcd)
			var dialled []*clientv3.Client
			r.EtcdClient = func(ctx context.Context, e *druidv1alpha1.Etcd) (*clientv3.Client, error) {
				c, err := dialURL(clientURL(cfgs[0]))(ctx, e)
				dialled = append(dialled, c)
				return c, err
			}
			ctx, cancel := context.WithTimeout(t.Context(), c.timeout)
			defer cancel()
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
				t.Fatal(err)
			}
			if len(dialled) != 1 {
				t.Fatalf("the pass dialled %d clients, want one", len(dialled))
			}
			clk.Step(5 * time.Second)
			if _, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID); ok != c.sample {
				t.Fatalf("the pass kept a sample: %t, want %t", ok, c.sample)
			}
			if state := dialled[0].ActiveConnection().GetState(); state != connectivity.Shutdown {
				t.Errorf("after the pass its client's connection is %s, want %s", state, connectivity.Shutdown)
			}
		})
	}
}

func TestAMemberWrittenPastItsQuotaReportsNoSpace(t *testing.T) {
	_, cfgs := etcdCluster(t, 1, func(_ int, cfg *embed.Config) { cfg.QuotaBackendBytes = 1 << 20 })
	c := etcdClient(t, clientURL(cfgs[0]))
	value := strings.Repeat("v", 64<<10)
	for i := 0; ; i++ {
		_, err := c.Put(t.Context(), fmt.Sprintf("/full/%d", i), value)
		if errors.Is(err, rpctypes.ErrNoSpace) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if i > 1000 {
			t.Fatal("a quota of 1 MiB took 1000 writes of 64 KiB")
		}
	}
	cp, etcd := sampledControlPlane("full", "uid-full")
	r, clk := sampler(cp, etcd)
	r.EtcdClient = dialURL(clientURL(cfgs[0]))
	testenv.Eventually(t, func() error {
		samplePass(t, r, client.ObjectKeyFromObject(cp))
		clk.Step(time.Minute)
		got := servedSample(t, r, cp)
		if !reflect.DeepEqual(got.Alarms, []string{lodiav1alpha1.MetricAlarmNoSpace}) || !got.HasLeader {
			return fmt.Errorf("a member past its quota gives %+v, want its leader and %s", got, lodiav1alpha1.MetricAlarmNoSpace)
		}
		return nil
	})

	status, err := c.Status(t.Context(), clientURL(cfgs[0]))
	if err != nil {
		t.Fatal(err)
	}
	corrupt := &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: status.Header.GetMemberId(), Alarm: etcdserverpb.AlarmType_CORRUPT}
	if _, err := etcdserverpb.NewMaintenanceClient(c.ActiveConnection()).Alarm(t.Context(), corrupt); err != nil {
		t.Fatal(err)
	}
	samplePass(t, r, client.ObjectKeyFromObject(cp))
	clk.Step(time.Minute)
	if got := servedSample(t, r, cp); !reflect.DeepEqual(got.Alarms, []string{lodiav1alpha1.MetricAlarmCorrupt, lodiav1alpha1.MetricAlarmNoSpace}) {
		t.Errorf("a member with both alarms armed gives %+v, want both", got)
	}
}

func TestWhatIsNotSampledIsForgotten(t *testing.T) {
	_, cfgs := etcdCluster(t, 1, nil)
	for _, c := range []struct {
		name         string
		change       func(cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) []client.Object
		requeueAfter time.Duration
	}{
		{"deleted", func(*lodiav1alpha1.ControlPlane, *druidv1alpha1.Etcd) []client.Object { return nil }, 0},
		{"being deleted", func(cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) []client.Object {
			cp.Finalizers, cp.DeletionTimestamp = []string{lodiav1alpha1.DatastoreFinalizer}, &metav1.Time{Time: sampledAt}
			return []client.Object{cp, etcd}
		}, time.Minute},
		{"paused", func(cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) []client.Object {
			cp.Spec.Paused = true
			return []client.Object{cp, etcd}
		}, time.Minute},
		{"shared", func(cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) []client.Object {
			cp.Spec.Datastore = lodiav1alpha1.DatastoreChoice{Shared: &lodiav1alpha1.SharedDatastore{Name: "pool"}}
			return []client.Object{cp, etcd}
		}, time.Minute},
		{"without its Etcd", func(cp *lodiav1alpha1.ControlPlane, _ *druidv1alpha1.Etcd) []client.Object {
			return []client.Object{cp}
		}, time.Minute},
		{"with an Etcd not formed", func(cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) []client.Object {
			delete(etcd.Annotations, lodiav1alpha1.EtcdFormedAnnotation)
			return []client.Object{cp, etcd}
		}, time.Minute},
		{"with an Etcd marked formed with another UID", func(cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) []client.Object {
			etcd.Annotations[lodiav1alpha1.EtcdFormedAnnotation] = "a-predecessor"
			return []client.Object{cp, etcd}
		}, time.Minute},
		{"with an Etcd another ControlPlane controls", func(cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) []client.Object {
			etcd.OwnerReferences[0].UID = "a-predecessor"
			return []client.Object{cp, etcd}
		}, time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp, etcd := sampledControlPlane("forgotten", "uid-forgotten")
			r, clk := sampler(cp, etcd)
			r.EtcdClient = dialURL(clientURL(cfgs[0]))
			samplePass(t, r, client.ObjectKeyFromObject(cp))
			clk.Step(5 * time.Second)
			servedSample(t, r, cp)
			r.Cache = fakeCache(c.change(cp.DeepCopy(), etcd.DeepCopy())...)
			if got := samplePass(t, r, client.ObjectKeyFromObject(cp)); got.RequeueAfter != c.requeueAfter {
				t.Errorf("the pass requeues after %s, want %s", got.RequeueAfter, c.requeueAfter)
			}
			if got, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID); ok {
				t.Errorf("a ControlPlane %s still has the sample %+v", c.name, got)
			}
		})
	}
}

func TestAReadOfTheCacheThatFailsForgetsTheSampleUntilTheNextMinute(t *testing.T) {
	_, cfgs := etcdCluster(t, 1, nil)
	for _, failing := range []client.Object{&lodiav1alpha1.ControlPlane{}, &druidv1alpha1.Etcd{}} {
		t.Run(fmt.Sprintf("%T", failing), func(t *testing.T) {
			cp, etcd := sampledControlPlane("unread", "uid-unread")
			r, clk := sampler(cp, etcd)
			r.EtcdClient = dialURL(clientURL(cfgs[0]))
			samplePass(t, r, client.ObjectKeyFromObject(cp))
			clk.Step(5 * time.Second)
			servedSample(t, r, cp)
			r.Cache = interceptor.NewClient(fakeCache(cp, etcd), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if reflect.TypeOf(obj) == reflect.TypeOf(failing) {
						return errors.New("injected: the cache did not answer")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			if got := samplePass(t, r, client.ObjectKeyFromObject(cp)); got.RequeueAfter != time.Minute {
				t.Errorf("a pass whose read fails requeues after %s, want a minute", got.RequeueAfter)
			}
			if got, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID); ok {
				t.Errorf("a pass whose read fails leaves the sample %+v", got)
			}
		})
	}
}

func TestARecreatedControlPlaneIsSampledUnderItsNewUIDAlone(t *testing.T) {
	_, cfgs := etcdCluster(t, 1, nil)
	first, firstEtcd := sampledControlPlane("again", "uid-first")
	r, clk := sampler(first, firstEtcd)
	r.EtcdClient = dialURL(clientURL(cfgs[0]))
	key := client.ObjectKeyFromObject(first)
	samplePass(t, r, key)
	clk.Step(5 * time.Second)
	servedSample(t, r, first)

	successor, successorEtcd := sampledControlPlane("again", "uid-successor")
	r.Cache = fakeCache(successor, successorEtcd)
	if got, ok := r.Sample(key, successor.UID); ok {
		t.Errorf("the successor is served its predecessor's sample %+v", got)
	}
	samplePass(t, r, key)
	clk.Step(4 * time.Second)
	if got, ok := r.Sample(key, successor.UID); ok {
		t.Errorf("before its own sample is served, the successor is served %+v", got)
	}
	if got, ok := r.Sample(key, first.UID); ok {
		t.Errorf("the predecessor is still served %+v", got)
	}
	clk.Step(time.Second)
	servedSample(t, r, successor)
}

func TestEachSampleStandsAMinuteWhateverItsPassTook(t *testing.T) {
	_, cfgs := etcdCluster(t, 1, nil)
	cp, etcd := sampledControlPlane("timed", "uid-timed")
	r, clk := sampler(cp, etcd)
	unreachable := closedURL(t)
	took := 100 * time.Millisecond
	endpoint := clientURL(cfgs[0])
	r.EtcdClient = func(ctx context.Context, etcd *druidv1alpha1.Etcd) (*clientv3.Client, error) {
		clk.Step(took)
		return dialURL(endpoint)(ctx, etcd)
	}
	key := client.ObjectKeyFromObject(cp)
	at := func(offset time.Duration) (datastore.EtcdSample, bool) {
		t.Helper()
		clk.SetTime(sampledAt.Add(offset))
		return r.Sample(key, cp.UID)
	}

	if got := samplePass(t, r, key); got.RequeueAfter != 59900*time.Millisecond {
		t.Errorf("a pass of 0.1 s requeues after %s, want 59.9s, a minute after its start", got.RequeueAfter)
	}
	if got, ok := at(4999 * time.Millisecond); ok {
		t.Errorf("a sample is served %+v before 5 s after its pass's start", got)
	}
	if got, ok := at(5 * time.Second); !ok || !got.HasLeader {
		t.Errorf("5 s after its pass's start the first sample is %+v, %t, want it served with its leader", got, ok)
	}

	clk.SetTime(sampledAt.Add(time.Minute))
	took, endpoint = 4900*time.Millisecond, unreachable.String()
	if got := samplePass(t, r, key); got.RequeueAfter != 55100*time.Millisecond {
		t.Errorf("a pass of 4.9 s requeues after %s, want 55.1s, a minute after its start", got.RequeueAfter)
	}
	if got, ok := at(64999 * time.Millisecond); !ok || !got.HasLeader {
		t.Errorf("after the second pass ends and before 5 s after its start the sample is %+v, %t, want the first, with its leader", got, ok)
	}
	if got, ok := at(65 * time.Second); !ok || got.HasLeader {
		t.Errorf("5 s after the second pass's start the sample is %+v, %t, want the second, without a leader", got, ok)
	}
}

type blackHole struct{ accepted atomic.Pointer[time.Time] }

func listenBlackHole(t *testing.T) (*blackHole, url.URL) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &blackHole{}
	var mu sync.Mutex
	var open []net.Conn
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			now := time.Now()
			h.accepted.CompareAndSwap(nil, &now)
			mu.Lock()
			open = append(open, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range open {
			_ = conn.Close()
		}
	})
	return h, url.URL{Scheme: "http", Host: l.Addr().String()}
}

func formedControlPlane(t *testing.T, ns, name string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	createClass(t, newClass(ns, "Delete"))
	create(t, controlPlane(ns, name, ns))
	writeGeneration(t, ns, name, 1)
	waitEtcd(t, ns, name, namesGeneration(1))
	readyOn(t, ns, name, 1)
	waitEtcd(t, ns, name, func(etcd *druidv1alpha1.Etcd) error {
		if mark := etcd.Annotations[lodiav1alpha1.EtcdFormedAnnotation]; mark != string(etcd.UID) {
			return fmt.Errorf("Etcd %s is marked formed with %q", etcd.Name, mark)
		}
		return nil
	})
	return getControlPlane(t, ns, name)
}

func runSampler(t *testing.T, ns string, dial func(context.Context, *druidv1alpha1.Etcd) (*clientv3.Client, error)) *datastore.SampleReconciler {
	t.Helper()
	options, err := operator.Cache(false)
	if err != nil {
		t.Fatal(err)
	}
	options.DefaultNamespaces = map[string]cache.Config{ns: {}}
	mgr, err := ctrl.NewManager(asDatastore, ctrl.Options{
		Scheme:     k8s.Scheme(),
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Cache:      options,
		Controller: config.Controller{SkipNameValidation: new(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &datastore.SampleReconciler{Cache: mgr.GetCache(), EtcdClient: dial, Clock: clock.RealClock{}}
	if err := r.SetupWithManager(mgr, 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
	})
	return r
}

func TestAMemberThatHangsEndsThePassWithinItsDeadline(t *testing.T) {
	hole, holeURL := listenBlackHole(t)
	_, cfgs := etcdCluster(t, 1, func(_ int, cfg *embed.Config) { cfg.AdvertiseClientUrls = []url.URL{holeURL} })
	ns := namespace(t)
	cp := formedControlPlane(t, ns, "hanging")
	r := runSampler(t, ns, dialURL(clientURL(cfgs[0])))
	testenv.EventuallyWithin(t, 15*time.Second, func() error {
		got, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID)
		if !ok {
			return errors.New("no sample yet")
		}
		if got.HasLeader || got.DBSize != nil {
			return fmt.Errorf("a member that does not answer gives %+v, want no leader and no size", got)
		}
		return nil
	})
	accepted := hole.accepted.Load()
	if accepted == nil {
		t.Fatal("the sampler never dialled the member that hangs")
	}
	if took := time.Since(*accepted); took > 6*time.Second {
		t.Errorf("the pass ended %s after it dialled the member that hangs, want at most the 5 s deadline", took)
	}
}

func TestASecretReadThatHangsEndsThePassWithinItsDeadline(t *testing.T) {
	ns := namespace(t)
	cp := formedControlPlane(t, ns, "unread")
	type read struct{ took time.Duration }
	reads := make(chan read, 10)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	cfg := rest.CopyConfig(asDatastore)
	cfg.WrapTransport = func(next http.RoundTripper) http.RoundTripper {
		return roundTripper(func(req *http.Request) (*http.Response, error) {
			if !strings.Contains(req.URL.Path, "/secrets/") {
				return next.RoundTrip(req)
			}
			start := time.Now()
			select {
			case <-req.Context().Done():
				reads <- read{took: time.Since(start)}
				return nil, req.Context().Err()
			case <-release:
				return nil, errors.New("released")
			}
		})
	}
	hanging, err := client.New(cfg, client.Options{Scheme: k8s.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	r := runSampler(t, ns, func(ctx context.Context, etcd *druidv1alpha1.Etcd) (*clientv3.Client, error) {
		return datastore.DialEtcd(ctx, hanging, etcd)
	})
	select {
	case got := <-reads:
		if got.took > 6*time.Second {
			t.Errorf("the Secret read that hangs ended after %s, want at most the 5 s deadline", got.took)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the Secret read that hangs did not end in 15 s")
	}
	consistently(t, func() error {
		if got, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID); ok {
			return fmt.Errorf("an etcd whose client certificate cannot be read is served %+v", got)
		}
		return nil
	})
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestOnlyACreationOrADeletionBringsAPassBeforeItsMinute(t *testing.T) {
	_, cfgs := etcdCluster(t, 1, nil)
	ns := namespace(t)
	cp := formedControlPlane(t, ns, "steady")
	var dials atomic.Int32
	r := runSampler(t, ns, func(ctx context.Context, etcd *druidv1alpha1.Etcd) (*clientv3.Client, error) {
		dials.Add(1)
		return dialURL(clientURL(cfgs[0]))(ctx, etcd)
	})
	testenv.EventuallyWithin(t, 15*time.Second, func() error {
		if got, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID); !ok || !got.HasLeader {
			return fmt.Errorf("the sample is %+v, %t, want one with its leader", got, ok)
		}
		return nil
	})
	for i := range 3 {
		patch := client.MergeFrom(cp.DeepCopy())
		metav1.SetMetaDataLabel(&cp.ObjectMeta, "example.com/edit", strconv.Itoa(i))
		if err := k8s.Patch(t.Context(), cp, patch); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(3 * time.Second)
	if n := dials.Load(); n != 1 {
		t.Errorf("the sampler dialled the etcd %d times within a minute of its first pass, want once", n)
	}
	if err := k8s.Delete(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		if got, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID); ok {
			return fmt.Errorf("a deleted ControlPlane is still served %+v", got)
		}
		return nil
	})
}
