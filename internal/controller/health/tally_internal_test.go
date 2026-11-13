// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

const (
	servicesPath   = "/api/v1/services"
	volumesPath    = "/api/v1/persistentvolumes"
	claimsPath     = "/api/v1/persistentvolumeclaims"
	validatingPath = "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations"
	mutatingPath   = "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations"
	apiServicePath = "/apis/apiregistration.k8s.io/v1/apiservices"
	cronJobsPath   = "/apis/batch/v1/cronjobs"

	emptyList = `{"metadata":{},"items":[]}`

	maxMessage = 32768
)

func servingPaths(t *testing.T, answers map[string][]byte) *tenant {
	t.Helper()
	return serving(t, answers, func() {})
}

func serving(t *testing.T, answers map[string][]byte, beforeAnswering func()) *tenant {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, ok := answers[req.URL.Path]
		if !ok {
			body = []byte(emptyList)
		}
		beforeAnswering()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	tenant, err := (&NodesReconciler{}).tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
	if err != nil {
		t.Fatal(err)
	}
	return tenant
}

func nameOf(prefix string, i, length int) string {
	return padded(fmt.Sprintf("%s%03d", prefix, i), length)
}

func TestATallyCountsEveryObjectAndKeepsTheThreeThatSortFirst(t *testing.T) {
	for _, c := range []struct {
		name  string
		names []string
		want  string
	}{
		{"none", nil, "0 things"},
		{"one", []string{"b"}, "1 things (b)"},
		{"three, unsorted", []string{"c", "a", "b"}, "3 things (a, b, c)"},
		{"four, ascending", []string{"a", "b", "c", "d"}, "4 things (a, b, c…)"},
		{"four, descending", []string{"d", "c", "b", "a"}, "4 things (a, b, c…)"},
		{"many, descending", []string{"j", "i", "h", "g", "f", "e", "d", "c", "b", "a"}, "10 things (a, b, c…)"},
		{"the same name twice", []string{"a", "a", "a", "a"}, "4 things (a, a, a…)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var got tally
			for _, name := range c.names {
				got.add(name)
			}
			if message := counted("things", got); message != c.want {
				t.Errorf("counted %q, want %q", message, c.want)
			}
		})
	}
}

func TestATallyJoinsTheNamespaceAndTheNameAndKeepsNoPartOverMaxName(t *testing.T) {
	long := strings.Repeat("a", maxName+1)
	exact := strings.Repeat("b", maxName)
	for _, c := range []struct {
		name  string
		parts [][]string
		want  string
	}{
		{"a namespace and a name", [][]string{{"ns", "svc"}}, "1 things (ns/svc)"},
		{"parts of exactly maxName", [][]string{{exact, exact}}, "1 things (" + exact + "/" + exact + ")"},
		{"a name one byte over", [][]string{{"ns", long}}, "1 things"},
		{"a namespace one byte over", [][]string{{long, "svc"}}, "1 things"},
		{"a name over, among names", [][]string{{"ns", "b"}, {"ns", long}, {"ns", "a"}}, "3 things (ns/a, ns/b…)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var got tally
			for _, parts := range c.parts {
				got.add(parts...)
			}
			if message := counted("things", got); message != c.want {
				t.Errorf("counted %q, want %q", message, c.want)
			}
		})
	}
}

func TestTwoTalliesMergeIntoTheCountAndTheThreeThatSortFirstOfBoth(t *testing.T) {
	var validating, mutating tally
	for _, name := range []string{"x", "y", "z", "w"} {
		validating.add(name)
	}
	for _, name := range []string{"b", "a"} {
		mutating.add(name)
	}
	mutating.more = true
	var got tally
	got.merge(validating)
	got.merge(mutating)
	if message, want := counted("things", got), "6 or more things (a, b, w…)"; message != want {
		t.Errorf("counted %q, want %q", message, want)
	}
}

func TestATallyBuildsNoNameForAnObjectThatDoesNotQualify(t *testing.T) {
	const webhooks = 128_000
	config := strings.Repeat("c", maxName)
	names := make([]string, webhooks)
	for i := range names {
		names[i] = nameOf("w", i, 8)
	}
	for _, order := range []struct {
		name string
		at   func(i int) string
	}{
		{"ascending", func(i int) string { return names[i] }},
		{"descending", func(i int) string { return names[webhooks-1-i] }},
	} {
		t.Run(order.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(3, func() {
				var got tally
				for i := range webhooks {
					got.add(config, order.at(i))
				}
			})
			if allocs > 16 {
				t.Errorf("counting %d webhooks allocated %.0f times, want a count that does not grow with them", webhooks, allocs)
			}
		})
	}
}

func emptyWebhooks(configurations, nameLen int) []byte {
	var b strings.Builder
	b.WriteString(`{"metadata":{},"items":[`)
	hooks := strings.TrimSuffix(strings.Repeat(`{},`, maxWebhooks), ",")
	for i := range configurations {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"metadata":{"name":"` + nameOf("c", i, nameLen) + `"},"webhooks":[` + hooks + `]}`)
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

func TestAPageOf128000EmptyWebhooksIsACountAndThreeNamesInEachKindOfConfiguration(t *testing.T) {
	const configurations = listLimit
	for _, c := range []struct {
		name    string
		nameLen int
		kept    string
	}{
		{"configurations named with maxName bytes", maxName, "(" + nameOf("c", 0, maxName) + "/, " + nameOf("c", 0, maxName) + "/, " + nameOf("c", 0, maxName) + "/…)"},
		{"configurations named past maxName", maxName + 1, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			page := emptyWebhooks(configurations, c.nameLen)
			for kind, answers := range map[string]map[string][]byte{
				"validating": {validatingPath: page},
				"mutating":   {mutatingPath: page},
				"both":       {validatingPath: page, mutatingPath: page},
			} {
				t.Run(kind, func(t *testing.T) {
					tenant := servingPaths(t, answers)
					message := (&Reconciler{Timeout: time.Minute}).actedOn(t.Context(), tenant)
					webhooks := configurations * maxWebhooks * len(answers)
					want := strconv.Itoa(webhooks) + " webhooks that call a URL or fail closed"
					if c.kept != "" {
						want += " " + c.kept
					}
					want = "0 LoadBalancer Services; 0 PersistentVolumes with reclaimPolicy Delete; 0 pending PersistentVolumeClaims; " + want + "; 0 APIServices served by a Service; 0 CronJobs not suspended"
					if message != want {
						t.Errorf("the message says %q, want %q", message, want)
					}
					if len(message) >= maxMessage {
						t.Errorf("the message has %d bytes, want under %d", len(message), maxMessage)
					}
				})
			}
		})
	}
}

func objectsOf(order func(int) int, object func(i int) string) []byte {
	items := make([]string, listLimit)
	for i := range items {
		items[i] = object(order(i))
	}
	return []byte(`{"metadata":{},"items":[` + strings.Join(items, ",") + `]}`)
}

func TestTheMessageOfAHeldRestoreStaysUnderTheSchemasBoundWhateverTheTenantAnswers(t *testing.T) {
	descending := func(i int) int { return listLimit - 1 - i }
	for _, c := range []struct {
		name    string
		nameLen int
		quoted  bool
	}{
		{"names of maxName bytes", maxName, true},
		{"names of one byte more", maxName + 1, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			namespace := func(i int) string { return nameOf("n", i, c.nameLen) }
			name := func(i int) string { return nameOf("s", i, c.nameLen) }
			tenant := servingPaths(t, map[string][]byte{
				servicesPath: objectsOf(descending, func(i int) string {
					return `{"metadata":{"namespace":"` + namespace(i) + `","name":"` + name(i) + `"},"spec":{"type":"LoadBalancer"}}`
				}),
				volumesPath: objectsOf(descending, func(i int) string {
					return `{"metadata":{"name":"` + name(i) + `"},"spec":{"persistentVolumeReclaimPolicy":"Delete"}}`
				}),
				claimsPath: objectsOf(descending, func(i int) string {
					return `{"metadata":{"namespace":"` + namespace(i) + `","name":"` + name(i) + `"},"status":{"phase":"Pending"}}`
				}),
				validatingPath: objectsOf(descending, func(i int) string {
					return `{"metadata":{"name":"` + name(i) + `"},"webhooks":[{"name":"` + name(i) + `"}]}`
				}),
				mutatingPath: objectsOf(descending, func(i int) string {
					return `{"metadata":{"name":"` + name(i) + `"},"webhooks":[{"name":"` + name(i) + `"}]}`
				}),
				apiServicePath: objectsOf(descending, func(i int) string {
					return `{"metadata":{"name":"` + name(i) + `"},"spec":{"service":{}}}`
				}),
				cronJobsPath: objectsOf(descending, func(i int) string {
					return `{"metadata":{"namespace":"` + namespace(i) + `","name":"` + name(i) + `"},"spec":{}}`
				}),
			})
			message := (&Reconciler{Timeout: time.Minute}).actedOn(t.Context(), tenant)

			pair := func(i int) string { return namespace(i) + "/" + name(i) }
			quote := func(name func(int) string, first ...int) string {
				if !c.quoted {
					return ""
				}
				return " (" + name(first[0]) + ", " + name(first[1]) + ", " + name(first[2]) + "…)"
			}
			want := strings.Join([]string{
				strconv.Itoa(listLimit) + " LoadBalancer Services" + quote(pair, 0, 1, 2),
				strconv.Itoa(listLimit) + " PersistentVolumes with reclaimPolicy Delete" + quote(name, 0, 1, 2),
				strconv.Itoa(listLimit) + " pending PersistentVolumeClaims" + quote(pair, 0, 1, 2),
				strconv.Itoa(2*listLimit) + " webhooks that call a URL or fail closed" + quote(func(i int) string { return name(i) + "/" + name(i) }, 0, 0, 1),
				strconv.Itoa(listLimit) + " APIServices served by a Service" + quote(name, 0, 1, 2),
				strconv.Itoa(listLimit) + " CronJobs not suspended" + quote(pair, 0, 1, 2),
			}, "; ")
			if message != want {
				t.Errorf("the message says %q, want %q", message, want)
			}
			if len(message) >= maxMessage {
				t.Errorf("the message has %d bytes, want under %d", len(message), maxMessage)
			}
		})
	}
}

func TestMeasureTheLiveHeapOfAPassOverAPageOf128000EmptyWebhooks(t *testing.T) {
	if os.Getenv(measureHeap) == "" {
		t.Skipf("set %s to measure the heap of a held restore's pass over a page of empty webhooks", measureHeap)
	}
	const configurations = listLimit
	for _, c := range []struct {
		name    string
		answers func(page []byte) map[string][]byte
		kinds   int
	}{
		{"a validating configuration list", func(page []byte) map[string][]byte { return map[string][]byte{validatingPath: page} }, 1},
		{"both configuration lists", func(page []byte) map[string][]byte {
			return map[string][]byte{validatingPath: page, mutatingPath: page}
		}, 2},
	} {
		for _, nameLen := range []int{maxName, 15_900} {
			t.Run(fmt.Sprintf("%s named with %d bytes", c.name, nameLen), func(t *testing.T) {
				page := emptyWebhooks(configurations, nameLen)
				tenant := servingPaths(t, c.answers(page))
				var message string
				begun := time.Now()
				peaks := measureLiveHeap(func() { message = (&Reconciler{Timeout: 10 * time.Minute}).actedOn(t.Context(), tenant) })
				t.Logf("%d webhooks in a page of %d bytes, %d pages at once, took %v: live heap %.1f MiB, %.2f responses; heap in use %.1f MiB; a message of %d bytes",
					configurations*maxWebhooks, len(page), c.kinds, time.Since(begun).Round(time.Millisecond), mib(peaks.live), float64(peaks.live)/maxResponseBytes, mib(peaks.inUse), len(message))
			})
		}
	}
}
