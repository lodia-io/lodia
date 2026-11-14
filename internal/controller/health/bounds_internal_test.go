// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func readNamed(t *testing.T, name string) tenantRead {
	t.Helper()
	for _, read := range everyRead() {
		if read.name == name {
			return read
		}
	}
	t.Fatalf("no read is named %q", name)
	return tenantRead{}
}

func quotedOf(n int) string { return `"` + strings.Repeat("a", n) + `"` }

// timeOfLength is an RFC 3339 time of n bytes, a fraction of seconds as long as
// it takes.
func timeOfLength(n int) string {
	const start, end = "2026-11-13T10:00:00.", "Z"
	return `"` + start + strings.Repeat("1", n-len(start)-len(end)) + end + `"`
}

// integerOfLength is an int32 of n bytes when n is 11, and a number of n bytes
// past it.
func integerOfLength(n int) string {
	return "-2147483648" + strings.Repeat("0", n-len("-2147483648"))
}

func TestEveryKeptFieldIsReadAtItsBoundAndOneBytePastItStopsOrFailsTheRead(t *testing.T) {
	sizedForTheBounds(t)
	const (
		leases = "the Leases of kube-node-lease"
		nodes  = "the Nodes"
	)
	item := func(object string) func(string) string {
		return func(token string) string {
			return `{"metadata":{},"items":[` + strings.ReplaceAll(object, "TOKEN", token) + `]}`
		}
	}
	whole := func(object string) func(string) string {
		return func(token string) string { return strings.ReplaceAll(object, "TOKEN", token) }
	}
	for _, c := range []struct {
		name    string
		read    string
		field   string
		most    int
		token   func(int) string
		body    func(string) string
		stops   string
		atBound string
	}{
		{"a Lease's name", leases, "metadata.name", 2048, quotedOf, item(`{"metadata":{"name":TOKEN}}`), leases, ""},
		{"a Lease's resourceVersion", leases, "metadata.resourceVersion", 64, quotedOf, item(`{"metadata":{"name":"n","resourceVersion":TOKEN}}`), leases, ""},
		{"a Node's name", nodes, "metadata.name", 2048, quotedOf, item(`{"metadata":{"name":TOKEN}}`), nodes, ""},
		{"a Node's kubelet version", nodes, "status.nodeInfo.kubeletVersion", 384, quotedOf, item(`{"metadata":{"name":"n"},"status":{"nodeInfo":{"kubeletVersion":TOKEN}}}`), nodes, ""},
		{"a Ready condition's time", nodes, "status.conditions.lastTransitionTime", 64, timeOfLength, item(`{"metadata":{"name":"n"},"status":{"conditions":[{"type":"Ready","lastTransitionTime":TOKEN}]}}`), nodes, ""},
		{"a leader Lease's resourceVersion", "a leader Lease", "metadata.resourceVersion", 64, quotedOf, whole(`{"metadata":{"resourceVersion":TOKEN}}`), "", ""},
		{"a leader Lease's holder", "a leader Lease", "spec.holderIdentity", 512, quotedOf, whole(`{"spec":{"holderIdentity":TOKEN}}`), "", ""},
		{"a leader Lease's duration", "a leader Lease", "spec.leaseDurationSeconds", 11, integerOfLength, whole(`{"spec":{"leaseDurationSeconds":TOKEN}}`), "", ""},
		{"a leader Lease's transitions", "a leader Lease", "spec.leaseTransitions", 11, integerOfLength, whole(`{"spec":{"leaseTransitions":TOKEN}}`), "", ""},
		{"a leader Lease's acquire time", "a leader Lease", "spec.acquireTime", 64, timeOfLength, whole(`{"spec":{"acquireTime":TOKEN}}`), "", "spec.acquireTime is not a time"},
		{"a leader Lease's renew time", "a leader Lease", "spec.renewTime", 64, timeOfLength, whole(`{"spec":{"renewTime":TOKEN}}`), "", "spec.renewTime is not a time"},
		{"a Service's name", "Services", "metadata.name", 2048, quotedOf, item(`{"metadata":{"name":TOKEN}}`), "", ""},
		{"a Service's namespace", "Services", "metadata.namespace", 2048, quotedOf, item(`{"metadata":{"namespace":TOKEN,"name":"s"}}`), "", ""},
		{"a Service list's token", "Services", "metadata.continue", 4096, quotedOf, whole(`{"metadata":{"continue":TOKEN},"items":[]}`), "", ""},
		{"a PersistentVolume's name", "PersistentVolumes", "metadata.name", 2048, quotedOf, item(`{"metadata":{"name":TOKEN}}`), "", ""},
		{"a PersistentVolumeClaim's namespace", "PersistentVolumeClaims", "metadata.namespace", 2048, quotedOf, item(`{"metadata":{"namespace":TOKEN,"name":"c"}}`), "", ""},
		{"a PersistentVolumeClaim's name", "PersistentVolumeClaims", "metadata.name", 2048, quotedOf, item(`{"metadata":{"name":TOKEN}}`), "", ""},
		{"a webhook configuration's name", "ValidatingWebhookConfigurations", "metadata.name", 2048, quotedOf, item(`{"metadata":{"name":TOKEN},"webhooks":[{}]}`), "", ""},
		{"a webhook's name", "MutatingWebhookConfigurations", "webhooks.name", 2048, quotedOf, item(`{"metadata":{"name":"c"},"webhooks":[{"name":TOKEN}]}`), "", ""},
		{"an APIService's name", "APIServices", "metadata.name", 2048, quotedOf, item(`{"metadata":{"name":TOKEN}}`), "", ""},
		{"a CronJob's namespace", "CronJobs", "metadata.namespace", 2048, quotedOf, item(`{"metadata":{"namespace":TOKEN,"name":"j"}}`), "", ""},
		{"a CronJob's name", "CronJobs", "metadata.name", 2048, quotedOf, item(`{"metadata":{"name":TOKEN}}`), "", ""},
	} {
		want := func(most int) string {
			if c.stops != "" {
				return c.field + " in " + c.stops + " is longer than " + strconv.Itoa(most) + " bytes"
			}
			return "read the response: " + c.field + " is longer than " + strconv.Itoa(most) + " bytes"
		}
		for _, past := range []bool{false, true} {
			name := c.name + " at its bound"
			n := c.most
			if past {
				name, n = c.name+" one byte past its bound", c.most+1
			}
			t.Run(name, func(t *testing.T) {
				r, tenant := servingEveryPath(t, amplifier{status: http.StatusOK, body: []byte(c.body(c.token(n)))})
				stop, err := readNamed(t, c.read).do(t.Context(), tenant, r)
				switch got := stop; {
				case past && c.stops != "":
					if got != want(c.most) || err != nil {
						t.Errorf("stopped %q with error %v, want the stop %q", got, err, want(c.most))
					}
				case past:
					if err == nil || err.Error() != want(c.most) {
						t.Errorf("read returned %v, want the error %q", err, want(c.most))
					}
				case c.atBound != "":
					if err == nil || err.Error() != "read the response: "+c.atBound || got != "" {
						t.Errorf("read returned stop %q and error %v, want the error %q of a value past its kind", got, err, c.atBound)
					}
				default:
					if err != nil || got != "" {
						t.Errorf("read returned stop %q and error %v, want a read that goes on", got, err)
					}
				}
			})
		}
	}
}

func TestAStatusIsReadAtTheBoundsOfItsFieldsAndNeverPastThem(t *testing.T) {
	sizedForTheBounds(t)
	status := func(reason, message, code, cause string) string {
		return `{"kind":"Status","status":"Failure","reason":` + reason + `,"message":` + message + `,"code":` + code + `,"details":{"causes":[{"reason":` + cause + `}]}}`
	}
	for _, c := range []struct {
		name string
		body func(n int) string
		most int
		past string
	}{
		{"a reason", func(n int) string { return status(quotedOf(n), `"m"`, "500", `"c"`) }, maxReasonRaw, ""},
		{"a cause's reason", func(n int) string { return status(`"r"`, `"m"`, "500", quotedOf(n)) }, maxReasonRaw, ""},
		{"a message", func(n int) string { return status(`"r"`, quotedOf(n), "500", `"c"`) }, 1024, longMessage},
		{"a code", func(n int) string { return status(`"r"`, `"m"`, integerOfLength(n), `"c"`) }, 11, ""},
	} {
		for _, past := range []bool{false, true} {
			n, name := c.most, c.name+" at its bound"
			if past {
				n, name = c.most+1, c.name+" one byte past its bound"
			}
			t.Run(name, func(t *testing.T) {
				_, tenant := servingEveryPath(t, amplifier{status: http.StatusInternalServerError, body: []byte(c.body(n))})
				_, err := tenant.getLease(t.Context(), "x")
				if err == nil {
					t.Fatal("a failed request returned no error")
				}
				switch {
				case !past && c.name == "a code":
					if got := err.Error(); got != "m" && !apierrors.IsInternalError(err) {
						t.Errorf("the error says %q", got)
					}
				case !past && c.name == "a message":
					if got := err.Error(); got != strings.Repeat("a", 1024) {
						t.Errorf("the error says %d bytes, want the message of %d bytes", len(got), 1024)
					}
				case !past:
					if got := err.Error(); got != "m" {
						t.Errorf("the error says %q, want the Status's message m", got)
					}
				case c.past != "":
					if got := err.Error(); got != c.past {
						t.Errorf("the error says %q, want %q", got, c.past)
					}
				default:
					if got := err.Error(); got != noStatusMessage {
						t.Errorf("the error says %q, want Health's own, %q", got, noStatusMessage)
					}
				}
			})
		}
	}
}

func TestAKeyOfAnyKindIsSkippedAndKeepsNothingOfTheTenant(t *testing.T) {
	sizedForTheBounds(t)
	for _, c := range []struct {
		name string
		key  string
	}{
		{"letters", strings.Repeat("a", maxResponseBytes/4)},
		{"invalid UTF-8", strings.Repeat("\xff", maxResponseBytes/4)},
		{"escapes past the bound", strings.Repeat(`\n`, maxResponseBytes/8)},
		{"escapes at the bound", strings.Repeat(`\n`, 128)},
		{"escapes one byte past the bound", strings.Repeat(`\n`, 128) + `a`},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := `{"` + c.key + `":{},"metadata":{"continue":""},"items":[{"` + c.key + `":1,"metadata":{"` + c.key + `":1,"name":"n","resourceVersion":"1"}}]}`
			r, tenant := servingEveryPath(t, amplifier{status: http.StatusOK, body: []byte(body)})
			kept, stop, err := r.listLeases(t.Context(), tenant)
			if len(kept) != 1 || stop != "" || err != nil {
				t.Errorf("kept %d Leases, stop %q, error %v, want one Lease", len(kept), stop, err)
			}
		})
	}
}

func TestAKeyThatUnquotesToAFieldNameIsTheField(t *testing.T) {
	body := `{"metadata":{},"items":[{"metadata":{"n\u0061me":"n","resource\u0056ersion":"1"}}]}`
	r, tenant := servingEveryPath(t, amplifier{status: http.StatusOK, body: []byte(body)})
	kept, stop, err := r.listLeases(t.Context(), tenant)
	if err != nil || stop != "" || len(kept) != 1 || kept[0].name != "n" || kept[0].version != versionOf("1") {
		t.Errorf("kept %+v, stop %q, error %v, want the Lease n at version 1", kept, stop, err)
	}
}

func TestAKeyOfAnotherCaseIsNotTheField(t *testing.T) {
	body := `{"metadata":{},"items":[{"metadata":{"name":"n","NAME":"x","Name":"y","resourceversion":"2","resourceVersion":"1"},"Metadata":{"name":"z"}}]}`
	r, tenant := servingEveryPath(t, amplifier{status: http.StatusOK, body: []byte(body)})
	kept, stop, err := r.listLeases(t.Context(), tenant)
	if err != nil || stop != "" || len(kept) != 1 || kept[0].name != "n" || kept[0].version != versionOf("1") {
		t.Errorf("kept %+v, stop %q, error %v, want the Lease n at version 1", kept, stop, err)
	}
}

func TestAnErrorLoggedByTheOperatorsLoggerCarriesNothingOfTheValue(t *testing.T) {
	sizedForTheBounds(t)
	const marker = "MARKER"
	for _, c := range []struct {
		name string
		body string
		read func(context *testing.T, r *NodesReconciler, tenant *tenant) error
	}{
		{"a time that does not parse", `{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"` + strings.Repeat(marker, 10) + `"}]}}]}`, listNodesError},
		{"a time of 8 MiB", `{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"` + strings.Repeat(marker, maxResponseBytes/2/len(marker)) + `"}]}}]}`, listNodesError},
		{"a time of invalid UTF-8", `{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"` + strings.Repeat("\xff", maxResponseBytes/2) + `"}]}}]}`, listNodesError},
		{"a name past its bound", `{"metadata":{},"items":[{"metadata":{"name":"` + strings.Repeat(marker, 1000) + `"}}]}`, listLeasesStop},
		{"a number that is no integer", `{"metadata":{"resourceVersion":"1"},"spec":{"leaseTransitions":1.` + strings.Repeat("5", 8) + `}}`, leaderLeaseError},
		{"a renew time that does not parse", `{"metadata":{"resourceVersion":"1"},"spec":{"renewTime":"` + strings.Repeat(marker, 10) + `"}}`, leaderLeaseError},
		{"an object where a string is", `{"metadata":{"resourceVersion":{"` + marker + `":1}}}`, leaderLeaseError},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, tenant := servingEveryPath(t, amplifier{status: http.StatusOK, body: []byte(c.body)})
			var logged bytes.Buffer
			err := c.read(t, r, tenant)
			if err == nil {
				t.Fatal("the read did not fail")
			}
			logger := crzap.New(crzap.WriteTo(&logged))
			logger.Error(err, "the read failed")
			if strings.Contains(err.Error(), marker) || strings.Contains(logged.String(), marker) || strings.Contains(logged.String(), "�") {
				t.Errorf("the error or its log carries the value: %.200q", logged.String())
			}
			if logged.Len() > 4096 {
				t.Errorf("the error logged %d bytes", logged.Len())
			}
		})
	}
}

func listNodesError(t *testing.T, r *NodesReconciler, tenant *tenant) error {
	t.Helper()
	_, stop, err := r.listNodes(t.Context(), tenant)
	if err == nil && stop != "" {
		return stopError(stop)
	}
	return err
}

func listLeasesStop(t *testing.T, r *NodesReconciler, tenant *tenant) error {
	t.Helper()
	_, stop, err := r.listLeases(t.Context(), tenant)
	if err == nil && stop != "" {
		return stopError(stop)
	}
	return err
}

func leaderLeaseError(t *testing.T, _ *NodesReconciler, tenant *tenant) error {
	t.Helper()
	_, err := tenant.getLease(t.Context(), "kube-controller-manager")
	return err
}

type stopError string

func (e stopError) Error() string { return string(e) }

func TestAFieldThatIsNotOfItsKindIsAFailedReadThatNamesTheField(t *testing.T) {
	for _, c := range []struct {
		body string
		want string
	}{
		{`{"metadata":{"resourceVersion":1}}`, "read the response: metadata.resourceVersion is not a string"},
		{`{"metadata":[]}`, "read the response: metadata is not an object"},
		{`{"spec":{"holderIdentity":true}}`, "read the response: spec.holderIdentity is not a string"},
		{`{"spec":{"leaseDurationSeconds":"15"}}`, "read the response: spec.leaseDurationSeconds is not an integer"},
		{`{"spec":{"leaseDurationSeconds":1.5}}`, "read the response: spec.leaseDurationSeconds is not an integer"},
		{`{"spec":{"renewTime":5}}`, "read the response: spec.renewTime is not a time"},
		{`{"spec":{"renewTime":"2026-11-13T10:00:00Z"}}`, "read the response: spec.renewTime is not a time"},
		{`[]`, "read the response: the Lease is not an object"},
		{`not json`, "the response is not JSON"},
	} {
		_, tenant := servingEveryPath(t, amplifier{status: http.StatusOK, body: []byte(c.body)})
		if _, err := tenant.getLease(t.Context(), "x"); err == nil || err.Error() != c.want {
			t.Errorf("%s returned %v, want %q", c.body, err, c.want)
		}
	}
}

func readerOf[T any](read func([]byte) (T, error)) func([]byte) (any, error) {
	return func(data []byte) (any, error) { return read(data) }
}

func TestAKeptFieldThatIsNullIsAbsent(t *testing.T) {
	for _, c := range []struct {
		name   string
		read   func([]byte) (any, error)
		absent string
		nulls  []string
	}{
		{"a leader Lease", readerOf(leaderLeaseFrom), `{}`, []string{
			`{"metadata":{"resourceVersion":null},"spec":{"holderIdentity":null,"leaseDurationSeconds":null,"renewTime":null,"acquireTime":null,"leaseTransitions":null}}`,
			`{"metadata":null,"spec":null}`,
		}},
		{"a list of Leases", readerOf(func(data []byte) (*leaseList, error) { return listFrom(data, leasePage, leaseItemOf) }), `{}`, []string{
			`{"metadata":{"continue":null},"items":null}`,
			`{"metadata":null}`,
		}},
		{"a list of Nodes", readerOf(func(data []byte) (*nodeList, error) { return listFrom(data, nodePage, nodeOf) }), `{}`, []string{
			`{"metadata":{"continue":null},"items":null}`,
			`{"metadata":null}`,
		}},
		{"a Lease", readerOf(leaseItemOf), `{}`, []string{
			`{"metadata":{"name":null,"resourceVersion":null}}`,
			`{"metadata":null}`,
		}},
		{"a Node", readerOf(nodeOf), `{}`, []string{
			`{"metadata":{"name":null},"status":{"nodeInfo":{"kubeletVersion":null},"conditions":null}}`,
			`{"metadata":null,"status":null}`,
			`{"status":{"nodeInfo":null}}`,
		}},
		{"a Ready condition", readerOf(func(data []byte) (nodeCondition, error) {
			n, err := nodeOf([]byte(`{"status":{"conditions":[` + string(data) + `]}}`))
			return n.Status.Conditions.items[0], err
		}), `{"type":"Ready"}`, []string{`{"type":"Ready","status":null,"lastTransitionTime":null}`}},
		{"a condition", readerOf(nodeConditionOf), `{}`, []string{`{"type":null,"status":null,"lastTransitionTime":null}`}},
		{"a Service", readerOf(serviceOf), `{}`, []string{
			`{"metadata":{"namespace":null,"name":null},"spec":{"type":null}}`,
			`{"metadata":null,"spec":null}`,
		}},
		{"a PersistentVolume", readerOf(persistentVolumeOf), `{}`, []string{
			`{"metadata":{"name":null},"spec":{"persistentVolumeReclaimPolicy":null}}`,
			`{"metadata":null,"spec":null}`,
		}},
		{"a PersistentVolumeClaim", readerOf(persistentVolumeClaimOf), `{}`, []string{
			`{"metadata":{"namespace":null,"name":null},"status":{"phase":null}}`,
			`{"metadata":null,"status":null}`,
		}},
		{"a webhook configuration", readerOf(webhookConfigurationOf), `{}`, []string{
			`{"metadata":{"name":null},"webhooks":null}`,
			`{"metadata":null}`,
		}},
		{"a webhook", readerOf(webhookOf), `{}`, []string{
			`{"name":null,"clientConfig":{"url":null},"failurePolicy":null}`,
			`{"clientConfig":null}`,
		}},
		{"an APIService", readerOf(apiServiceOf), `{}`, []string{
			`{"metadata":{"name":null},"spec":{"service":null}}`,
			`{"metadata":null,"spec":null}`,
		}},
		{"a CronJob", readerOf(cronJobOf), `{}`, []string{
			`{"metadata":{"namespace":null,"name":null},"spec":{"suspend":null}}`,
			`{"metadata":null,"spec":null}`,
		}},
		{"a Status", readerOf(statusFrom), `{}`, []string{
			`{"status":null,"message":null,"reason":null,"code":null,"details":{"causes":null}}`,
			`{"details":null}`,
		}},
		{"a cause", readerOf(causeOf), `{}`, []string{`{"reason":null}`}},
	} {
		absent, err := c.read([]byte(c.absent))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		for _, null := range c.nulls {
			t.Run(c.name+", "+null, func(t *testing.T) {
				if got, err := c.read([]byte(null)); err != nil || !reflect.DeepEqual(got, absent) {
					t.Errorf("read %+v, error %v, want what %s reads, %+v", got, err, c.absent, absent)
				}
			})
		}
	}
}

func TestAWebhookWhoseFailurePolicyIsNullFailsClosedAsTheDefaultDoes(t *testing.T) {
	for _, body := range []string{`{}`, `{"failurePolicy":null}`, `{"clientConfig":{"url":null},"failurePolicy":null}`} {
		if w, err := webhookOf([]byte(body)); err != nil || !w.actsOutside() {
			t.Errorf("%s reads as %+v, error %v, want a webhook that fails closed", body, w, err)
		}
	}
	if w, err := webhookOf([]byte(`{"failurePolicy":"Ignore"}`)); err != nil || w.actsOutside() {
		t.Errorf("a webhook that ignores failures reads as %+v, error %v, want one that acts on nothing outside", w, err)
	}
}

func TestATimeIsReadAsTheAPIsOwnTypeReadsIt(t *testing.T) {
	body := `{"metadata":{"resourceVersion":"7"},"spec":{"holderIdentity":"h","leaseDurationSeconds":15,"leaseTransitions":3,"acquireTime":"2026-11-13T10:00:00.123456Z","renewTime":"2026-11-13T10:00:05.000001+02:00"}}`
	_, tenant := servingEveryPath(t, amplifier{status: http.StatusOK, body: []byte(body)})
	lease, err := tenant.getLease(t.Context(), "x")
	if err != nil {
		t.Fatal(err)
	}
	acquired := time.Date(2026, 11, 13, 10, 0, 0, 123456000, time.UTC)
	renewed := time.Date(2026, 11, 13, 8, 0, 5, 1000, time.UTC)
	if lease.Metadata.ResourceVersion != "7" || *lease.Spec.HolderIdentity != "h" || *lease.Spec.LeaseDurationSeconds != 15 || *lease.Spec.LeaseTransitions != 3 ||
		!lease.Spec.AcquireTime.Time.Equal(acquired) || !lease.Spec.RenewTime.Time.Equal(renewed) {
		t.Errorf("the Lease holds %+v", lease)
	}
}

func TestAStringPastItsKeptLengthIsCutToOneByteMoreAndNeverHeld(t *testing.T) {
	name, kubelet := strings.Repeat("a", 2048), strings.Repeat("v", 384)
	l, err := leaseItemOf([]byte(`{"metadata":{"name":"` + name + `"}}`))
	if err != nil || len(l.Metadata.Name) != 254 {
		t.Errorf("a Lease named with 2,048 bytes holds a name of %d bytes, error %v, want 254", len(l.Metadata.Name), err)
	}
	n, err := nodeOf([]byte(`{"metadata":{"name":"` + name + `"},"status":{"nodeInfo":{"kubeletVersion":"` + kubelet + `"}}}`))
	if err != nil || len(n.Metadata.Name) != 254 || len(n.Status.NodeInfo.KubeletVersion) != 65 {
		t.Errorf("a Node named with 2,048 bytes with a kubelet version of 384 holds %d and %d bytes, error %v, want 254 and 65", len(n.Metadata.Name), len(n.Status.NodeInfo.KubeletVersion), err)
	}
	s, err := serviceOf([]byte(`{"metadata":{"namespace":"` + name + `","name":"` + name + `"}}`))
	if err != nil || len(s.Metadata.Namespace) != 254 || len(s.Metadata.Name) != 254 {
		t.Errorf("a Service named with 2,048 bytes holds %d and %d bytes, error %v, want 254 each", len(s.Metadata.Namespace), len(s.Metadata.Name), err)
	}
	w, err := webhookOf([]byte(`{"name":"` + name + `"}`))
	if err != nil || len(w.Name) != 254 {
		t.Errorf("a webhook named with 2,048 bytes holds a name of %d bytes, error %v, want 254", len(w.Name), err)
	}
	exact, err := leaseItemOf([]byte(`{"metadata":{"name":"` + name[:253] + `"}}`))
	if err != nil || len(exact.Metadata.Name) != 253 {
		t.Errorf("a Lease named with 253 bytes holds a name of %d bytes, error %v, want 253", len(exact.Metadata.Name), err)
	}
}
