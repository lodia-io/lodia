// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type wordField struct {
	name  string
	words []string
	read  func(token string) (string, error)
}

var wordFields = []wordField{
	{"a condition's type", []string{string(corev1.NodeReady)}, func(token string) (string, error) {
		c, err := nodeConditionOf([]byte(`{"type":` + token + `,"status":"True"}`))
		return string(c.Type) + string(c.Status), err
	}},
	{"a Ready condition's status", []string{string(corev1.ConditionTrue), string(corev1.ConditionFalse), string(corev1.ConditionUnknown)}, func(token string) (string, error) {
		c, err := nodeConditionOf([]byte(`{"type":"Ready","status":` + token + `}`))
		return string(c.Status), err
	}},
	{"a Status's status", []string{metav1.StatusFailure}, func(token string) (string, error) {
		s, err := statusFrom([]byte(`{"status":` + token + `}`))
		return s.Status, err
	}},
	{"a Service's type", []string{string(corev1.ServiceTypeLoadBalancer)}, func(token string) (string, error) {
		s, err := serviceOf([]byte(`{"metadata":{"name":"s"},"spec":{"type":` + token + `}}`))
		return string(s.Spec.Type), err
	}},
	{"a PersistentVolume's reclaim policy", []string{string(corev1.PersistentVolumeReclaimDelete)}, func(token string) (string, error) {
		v, err := persistentVolumeOf([]byte(`{"metadata":{"name":"v"},"spec":{"persistentVolumeReclaimPolicy":` + token + `}}`))
		return string(v.Spec.PersistentVolumeReclaimPolicy), err
	}},
	{"a PersistentVolumeClaim's phase", []string{string(corev1.ClaimPending)}, func(token string) (string, error) {
		c, err := persistentVolumeClaimOf([]byte(`{"metadata":{"name":"c"},"status":{"phase":` + token + `}}`))
		return string(c.Status.Phase), err
	}},
	{"a webhook's failure policy", []string{string(admissionregistrationv1.Fail)}, func(token string) (string, error) {
		w, err := webhookOf([]byte(`{"failurePolicy":` + token + `}`))
		if w.FailurePolicy == nil {
			return "<nil>", err
		}
		return string(*w.FailurePolicy), err
	}},
}

func TestAFieldOnlyComparedWithAWordIsNoneOfTheWordsPastItsBoundAndNeitherAStopNorAFailedRead(t *testing.T) {
	for _, c := range wordFields {
		for _, n := range []int{maxWordRaw, maxWordRaw + 1, 316, 1 << 20} {
			t.Run(c.name+", "+strconv.Itoa(n)+" bytes", func(t *testing.T) {
				want := ""
				if n <= maxWordRaw && c.name != "a condition's type" {
					want = strings.Repeat("a", n)
				}
				if got, err := c.read(quotedOf(n)); err != nil || got != want {
					t.Errorf("read %d bytes of the field as %d bytes, error %v, want %d bytes and none", n, len(got), err, len(want))
				}
			})
		}
	}
}

func escapedFully(word string) string {
	var out strings.Builder
	for _, r := range word {
		out.WriteString(`\u` + fmt.Sprintf("%04x", r))
	}
	return out.String()
}

func TestEveryWordHealthComparesIsReadAsItselfWithEveryCharacterEscapedAndMaxWordRawIsTheLongest(t *testing.T) {
	longest := 0
	for _, c := range wordFields {
		for _, word := range c.words {
			raw := escapedFully(word)
			longest = max(longest, len(raw))
			t.Run(c.name+", "+word, func(t *testing.T) {
				want := word
				if c.name == "a condition's type" {
					want += "True"
				}
				if got, err := c.read(`"` + raw + `"`); err != nil || got != want {
					t.Errorf("read %d raw bytes of %q as %q, error %v, want %q", len(raw), word, got, err, want)
				}
				if got, err := c.read(`"` + raw + `a"`); err != nil || got == want {
					t.Errorf("read %q followed by a letter as %q, error %v, want none of the words", word, got, err)
				}
			})
		}
	}
	if longest != maxWordRaw {
		t.Errorf("the longest word Health compares is %d bytes with every character escaped, and maxWordRaw is %d", longest, maxWordRaw)
	}
	for _, c := range wordFields {
		t.Run(c.name+", one byte past the bound", func(t *testing.T) {
			if got, err := c.read(quotedOf(maxWordRaw + 1)); err != nil || (got != "" && got != "<nil>") {
				t.Errorf("read %d bytes as %q, error %v, want none of the words", maxWordRaw+1, got, err)
			}
		})
	}
}

func TestAWordWithinItsBoundIsComparedAfterUnquoting(t *testing.T) {
	c, err := nodeConditionOf([]byte(`{"type":"R\u0065ady","status":"Tr\u0075e"}`))
	if err != nil || c.Type != corev1.NodeReady || c.Status != corev1.ConditionTrue {
		t.Errorf("read type %q and status %q, error %v, want Ready and True", c.Type, c.Status, err)
	}
	s, err := statusFrom([]byte(`{"status":"Fail\u0075re"}`))
	if err != nil || s.Status != metav1.StatusFailure {
		t.Errorf("read status %q, error %v, want Failure", s.Status, err)
	}
}

func TestAFieldThatIsNotAStringIsNotAWord(t *testing.T) {
	for _, body := range []string{`{"type":5}`, `{"type":{}}`, `{"type":[]}`, `{"type":true}`} {
		if _, err := nodeConditionOf([]byte(body)); err == nil || err.Error() != "status.conditions.type is not a string" {
			t.Errorf("%s returned %v, want a failed read that names the field", body, err)
		}
	}
	if c, err := nodeConditionOf([]byte(`{"type":null,"status":"True"}`)); err != nil || c.Type != "" {
		t.Errorf("a null type returned %+v, %v, want none", c, err)
	}
}

func allocated(run func()) uint64 {
	least := ^uint64(0)
	for range 3 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		run()
		runtime.ReadMemStats(&after)
		least = min(least, after.TotalAlloc-before.TotalAlloc)
	}
	return least
}

func TestAFieldComparedOrSkippedIsNeverDecoded(t *testing.T) {
	const size = 4 << 20
	huge := `"` + strings.Repeat("\xff", size) + `"`
	hugeKey := `"` + strings.Repeat(`\n`, size/2) + `"`
	node := func(condition string) ([]byte, func([]byte) error) {
		return []byte(`{"metadata":{"name":"n"},"status":{"conditions":[` + condition + `]}}`), func(body []byte) error {
			_, err := nodeOf(body)
			return err
		}
	}
	read := func(body string, read func([]byte) error) ([]byte, func([]byte) error) { return []byte(body), read }
	for _, c := range []struct {
		name string
		body func() ([]byte, func([]byte) error)
	}{
		{"a Status's status", func() ([]byte, func([]byte) error) {
			return read(`{"status":`+huge+`}`, func(body []byte) error {
				_, err := statusFrom(body)
				return err
			})
		}},
		{"a Status read for a failure", func() ([]byte, func([]byte) error) {
			return read(`{"kind":"Status","status":`+huge+`,"message":"m"}`, func(body []byte) error {
				if _, ok := failureOf(body); ok {
					t.Error("a Status whose status is no word was read as a failure")
				}
				return nil
			})
		}},
		{"a condition's type", func() ([]byte, func([]byte) error) { return node(`{"type":` + huge + `}`) }},
		{"a non-Ready condition's status", func() ([]byte, func([]byte) error) {
			return node(`{"type":"MemoryPressure","status":` + huge + `}`)
		}},
		{"a non-Ready condition's time", func() ([]byte, func([]byte) error) {
			return node(`{"type":"MemoryPressure","lastTransitionTime":` + huge + `}`)
		}},
		{"a Ready condition's status", func() ([]byte, func([]byte) error) { return node(`{"type":"Ready","status":` + huge + `}`) }},
		{"a skipped value of a Node", func() ([]byte, func([]byte) error) {
			return read(`{"metadata":{"name":"n","annotations":{"a":`+huge+`}}}`, func(body []byte) error {
				_, err := nodeOf(body)
				return err
			})
		}},
		{"a skipped key of a Node", func() ([]byte, func([]byte) error) {
			return read(`{"metadata":{"name":"n"},`+hugeKey+`:0}`, func(body []byte) error {
				_, err := nodeOf(body)
				return err
			})
		}},
		{"a Service's type", func() ([]byte, func([]byte) error) {
			return read(`{"metadata":{"name":"s"},"spec":{"type":`+huge+`}}`, func(body []byte) error {
				_, err := serviceOf(body)
				return err
			})
		}},
		{"a PersistentVolume's reclaim policy", func() ([]byte, func([]byte) error) {
			return read(`{"metadata":{"name":"v"},"spec":{"persistentVolumeReclaimPolicy":`+huge+`}}`, func(body []byte) error {
				_, err := persistentVolumeOf(body)
				return err
			})
		}},
		{"a PersistentVolumeClaim's phase", func() ([]byte, func([]byte) error) {
			return read(`{"metadata":{"name":"c"},"status":{"phase":`+huge+`}}`, func(body []byte) error {
				_, err := persistentVolumeClaimOf(body)
				return err
			})
		}},
		{"a webhook's failure policy", func() ([]byte, func([]byte) error) {
			return read(`{"failurePolicy":`+huge+`}`, func(body []byte) error {
				_, err := webhookOf(body)
				return err
			})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			body, read := c.body()
			var err error
			got := allocated(func() { err = read(body) })
			if err != nil {
				t.Fatal(err)
			}
			if limit := uint64(size / 16); got > limit {
				t.Errorf("the read allocated %d bytes over a field of %d, want at most %d", got, size, limit)
			}
		})
	}
}

func TestAStopInsideAPageKeepsTheItemsBeforeItAtMostMaxNodes(t *testing.T) {
	long := strings.Repeat("a", maxNameRaw+1)
	for _, c := range []struct {
		name     string
		maxNodes int
		body     string
		list     func(*NodesReconciler, *tenant) (int, string, error)
		kept     int
		stop     string
	}{
		{"Leases, two before the stop", 5000, `{"metadata":{},"items":[{"metadata":{"name":"a"}},{"metadata":{"name":"b"}},{"metadata":{"name":"` + long + `"}},{"metadata":{"name":"d"}}]}`,
			func(r *NodesReconciler, t *tenant) (int, string, error) {
				kept, stop, err := r.listLeases(context.Background(), t)
				return len(kept), stop, err
			}, 2, "metadata.name in the Leases of kube-node-lease is longer than 2048 bytes"},
		{"Nodes, one before the stop", 5000, `{"metadata":{},"items":[{"metadata":{"name":"a"}},{"metadata":{"name":"` + long + `"}}]}`,
			func(r *NodesReconciler, t *tenant) (int, string, error) {
				kept, stop, err := r.listNodes(context.Background(), t)
				return len(kept), stop, err
			}, 1, "metadata.name in the Nodes is longer than 2048 bytes"},
		{"Nodes, none before the stop", 5000, `{"metadata":{},"items":[{"metadata":{"name":"` + long + `"}}]}`,
			func(r *NodesReconciler, t *tenant) (int, string, error) {
				kept, stop, err := r.listNodes(context.Background(), t)
				return len(kept), stop, err
			}, 0, "metadata.name in the Nodes is longer than 2048 bytes"},
		{"Nodes, three before the stop and maxNodes 2", 2, `{"metadata":{},"items":[{"metadata":{"name":"a"}},{"metadata":{"name":"b"}},{"metadata":{"name":"c"}},{"metadata":{"name":"` + long + `"}}]}`,
			func(r *NodesReconciler, t *tenant) (int, string, error) {
				kept, stop, err := r.listNodes(context.Background(), t)
				return len(kept), stop, err
			}, 2, "metadata.name in the Nodes is longer than 2048 bytes"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, tenant := servingEveryPath(t, amplifier{status: http.StatusOK, body: []byte(c.body)})
			r.MaxNodes = c.maxNodes
			if kept, stop, err := c.list(r, tenant); kept != c.kept || stop != c.stop || err != nil {
				t.Errorf("kept %d, stopped %q, error %v, want %d and %q", kept, stop, err, c.kept, c.stop)
			}
		})
	}
}
