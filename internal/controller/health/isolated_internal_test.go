// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	WithoutEnvtestEnv = "LODIA_HEALTH_TEST_WITHOUT_ENVTEST"

	mostGoroutinesOfATestRunAlone = 8
	longestAnEarlierTestMayLinger = 10 * time.Second
)

type goroutinesBeside struct {
	sync.Mutex
	goroutines int
}

var stepGoroutines goroutinesBeside

type stepT interface {
	Helper()
	Skipf(format string, args ...any)
	Fatal(args ...any)
}

func inTheStepWithoutEnvtest(t *testing.T) {
	t.Helper()
	inTheStep(t, &stepGoroutines, longestAnEarlierTestMayLinger)
}

func inTheStep(t stepT, beside *goroutinesBeside, within time.Duration) {
	t.Helper()
	if os.Getenv(WithoutEnvtestEnv) == "" {
		t.Skipf("it counts allocations or reads the heap, or holds the quiet of the process those tests share, in a process with no envtest and no manager, which the step of make test with %s set runs it in", WithoutEnvtestEnv)
		return
	}
	if left := beside.leftover(within); left != "" {
		t.Fatal(left)
	}
}

func (b *goroutinesBeside) leftover(within time.Duration) string {
	got, beside, ok := b.settle(within)
	if ok {
		return ""
	}
	var stacks bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&stacks, 1)
	return fmt.Sprintf("%d goroutines when the body starts, want the %d of the first test of the process: an earlier test left some\n%s", got, beside, stacks.String())
}

func (b *goroutinesBeside) settle(within time.Duration) (got, beside int, ok bool) {
	deadline := time.Now().Add(within)
	for {
		got = runtime.NumGoroutine()
		b.Lock()
		if b.goroutines == 0 {
			b.goroutines = got
		}
		beside = b.goroutines
		b.Unlock()
		if got <= beside || time.Now().After(deadline) {
			return got, beside, got <= beside
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testsOfTheStepWithoutEnvtest(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no test files in the test binary's working directory: %v", err)
	}
	var names []string
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if callee, ok := call.Fun.(*ast.Ident); ok && callee.Name == "inTheStepWithoutEnvtest" {
						names = append(names, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	slices.Sort(names)
	return names
}

func TestTheStepWithoutEnvtestRunsExactlyTheTestsThatSkipOutsideItAndTheMainRunLeavesThemOut(t *testing.T) {
	makefile, err := os.ReadFile("../../../Makefile")
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	lines := strings.Split(string(makefile), "\n")
	for i, line := range lines {
		if line != "APART = \\" {
			continue
		}
		for _, next := range lines[i+1:] {
			listed = append(listed, strings.Fields(strings.TrimSuffix(next, "\\"))...)
			if !strings.HasSuffix(next, "\\") {
				break
			}
		}
	}
	slices.Sort(listed)
	if got := testsOfTheStepWithoutEnvtest(t); !slices.Equal(got, listed) {
		t.Errorf("the tests that skip outside the step are %v, and the Makefile's APART lists %v: a test is in one and not in the other", got, listed)
	}
	for _, want := range []string{
		"APART_TESTS = $(subst $(space),|,$(strip $(APART)))\n",
		"\tKUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT=60s KUBEBUILDER_ASSETS=\"$$(go tool setup-envtest use $(ENVTEST_K8S_VERSION) -p path)\" go test -race -skip '^($(APART_TESTS))$$' $(RACE_PACKAGES)\n",
		"{ " + WithoutEnvtestEnv + "=1 go test -race -count=1 -v -run '^($(APART_TESTS))$$' ./internal/controller/health; echo $$? > \"$$st\"; } 2>&1 | tee \"$$out\"; \\\n",
		"\tstatus=\"$$(cat \"$$st\")\"; [ -n \"$$status\" ] || status=1; \\\n",
		"\tfor t in $(APART); do grep -q \"^--- PASS: $$t (\" \"$$out\" || { echo \"$$t did not pass in the step without envtest\"; status=1; }; done; \\\n",
		"\trm -f \"$$out\" \"$$st\"; exit $$status\n",
	} {
		if strings.Count(string(makefile), want) != 1 {
			t.Errorf("the Makefile does not hold exactly one line %q", want)
		}
	}
	if len(listed) == 0 || len(slices.Compact(slices.Clone(listed))) != len(listed) {
		t.Errorf("the Makefile's APART lists %d tests, want each once", len(listed))
	}
	if _, err := regexp.Compile("^(" + strings.Join(listed, "|") + ")$"); err != nil {
		t.Error(err)
	}
}

func TestATestOfTheStepWithoutEnvtestRunsBesideNoGoroutineOfTheSuite(t *testing.T) {
	inTheStepWithoutEnvtest(t)
	got := runtime.NumGoroutine()
	t.Logf("the test runs beside %d goroutines in all", got-1)
	if got > mostGoroutinesOfATestRunAlone {
		t.Errorf("the test runs among %d goroutines, want at most %d: the test's own, the runtime's and the testing package's", got, mostGoroutinesOfATestRunAlone)
	}
}

func TestAStepStartsABodyOnlyWhenTheGoroutinesOfEarlierTestsHaveEnded(t *testing.T) {
	inTheStepWithoutEnvtest(t)
	var beside goroutinesBeside
	_, first, ok := beside.settle(time.Second)
	if !ok {
		t.Fatalf("a process that has never been settled did not settle at %d goroutines", first)
	}
	release := make(chan struct{})
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		<-release
	}()
	if got, now, ok := beside.settle(50 * time.Millisecond); ok || got != first+1 || now != first {
		t.Errorf("with a goroutine left over the step settled to %d goroutines of %d (ok %v), want %d of %d and not settled", got, now, ok, first+1, first)
	}
	close(release)
	<-ended
	if got, now, ok := beside.settle(time.Second); !ok || now != first || got > first {
		t.Errorf("after it ended the step settled to %d goroutines of %d (ok %v), want %d or fewer of %d and settled", got, now, ok, first, first)
	}
	endsSoon := make(chan struct{})
	go func() {
		defer close(endsSoon)
		time.Sleep(100 * time.Millisecond)
	}()
	if got, now, ok := beside.settle(10 * time.Second); !ok || now != first || got > first {
		t.Errorf("with a goroutine that ends in 100 ms the step settled to %d goroutines of %d (ok %v), want %d or fewer of %d and settled", got, now, ok, first, first)
	}
	<-endsSoon
}

type fakeStepT struct {
	skipped, failed bool
	said            string
}

func (*fakeStepT) Helper() {}

func (f *fakeStepT) Skipf(format string, args ...any) {
	f.skipped = true
	f.said = fmt.Sprintf(format, args...)
}

func (f *fakeStepT) Fatal(args ...any) {
	f.failed = true
	f.said = fmt.Sprint(args...)
}

func TestAGuardSkipsOutsideTheStepAndFailsInItWhenAnEarlierTestLeftGoroutinesBeside(t *testing.T) {
	inTheStepWithoutEnvtest(t)
	var beside goroutinesBeside
	_, first, _ := beside.settle(time.Second)
	release := make(chan struct{})
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		<-release
	}()
	defer func() {
		close(release)
		<-ended
	}()
	for _, c := range []struct {
		name            string
		env             string
		skipped, failed bool
		says            string
	}{
		{"outside the step", "", true, false, WithoutEnvtestEnv},
		{"in the step with a goroutine left beside", "1", false, true, "an earlier test left some"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(WithoutEnvtestEnv, c.env)
			var fake fakeStepT
			inTheStep(&fake, &beside, 50*time.Millisecond)
			if fake.skipped != c.skipped || fake.failed != c.failed || !strings.Contains(fake.said, c.says) {
				t.Errorf("the guard skipped %v and failed %v, saying %q, want %v, %v and %q: %d goroutines beside %d", fake.skipped, fake.failed, fake.said, c.skipped, c.failed, c.says, runtime.NumGoroutine(), first)
			}
		})
	}
	t.Run("in the step with nothing left beside", func(t *testing.T) {
		t.Setenv(WithoutEnvtestEnv, "1")
		var quiet goroutinesBeside
		var fake fakeStepT
		inTheStep(&fake, &quiet, time.Second)
		if fake.skipped || fake.failed {
			t.Errorf("the guard skipped %v and failed %v, saying %q, want neither", fake.skipped, fake.failed, fake.said)
		}
	})
}
