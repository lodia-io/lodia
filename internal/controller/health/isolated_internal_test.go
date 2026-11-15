// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
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
	OwnProcessEnv = "LODIA_HEALTH_TEST_IN_A_PROCESS_OF_ITS_OWN"

	runnerProbeEnv = "LODIA_HEALTH_RUNNER_PROBE"
	bodyRanMarker  = "THE BODY RAN TO ITS END: "

	mostGoroutinesOfATestRunAlone = 8
	longestAnEarlierTestMayLinger = 10 * time.Second
)

var apart struct {
	sync.Mutex
	output  string
	err     error
	checked map[string]bool
}

type goroutinesBeside struct {
	sync.Mutex
	goroutines int
}

var childGoroutines goroutinesBeside

func inAProcessOfItsOwn(t *testing.T) (ranApart bool) {
	t.Helper()
	if os.Getenv(OwnProcessEnv) != "" {
		settleBesideTheBody(t)
		return false
	}
	apart.Lock()
	if apart.checked == nil || apart.checked[t.Name()] {
		apart.output, apart.err = runTheTestsApart(testsInAProcessOfTheirOwn(t))
		apart.checked = map[string]bool{}
	}
	apart.checked[t.Name()] = true
	output, err := apart.output, apart.err
	apart.Unlock()
	if problem := verdictOfTheChild(t.Name(), output, err); problem != "" {
		t.Fatalf("%s in a process of its own: %s\n%s", t.Name(), problem, logOfTheChild(t.Name(), output))
	}
	if testing.Verbose() {
		t.Logf("%s", logOfTheChild(t.Name(), output))
	}
	return true
}

func theBodyRanToItsEnd(t *testing.T) {
	if !t.Failed() {
		fmt.Printf("%s%s\n", bodyRanMarker, t.Name())
	}
}

func settleBesideTheBody(t *testing.T) {
	t.Helper()
	if got, beside, ok := childGoroutines.settle(longestAnEarlierTestMayLinger); !ok {
		var stacks bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 1)
		t.Fatalf("%d goroutines when the body starts, want the %d of the first test of the process: an earlier test left some\n%s", got, beside, stacks.String())
	}
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

func runTheTestsApart(names []string, env ...string) (output string, err error) {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = regexp.QuoteMeta(name)
	}
	command := exec.Command(os.Args[0], "-test.run=^("+strings.Join(quoted, "|")+")$", "-test.count=1", "-test.v")
	command.Env = append(append(os.Environ(), OwnProcessEnv+"=1"), env...)
	out, err := command.CombinedOutput()
	return string(out), err
}

func verdictOfTheChild(name, output string, err error) string {
	switch {
	case !strings.Contains(output, "--- PASS: "+name+" "):
		return "the child did not pass it"
	case !strings.Contains(output, bodyRanMarker+name+"\n"):
		return "the child did not run its body to its end"
	case err != nil && !strings.Contains(output, "--- FAIL: "):
		return fmt.Sprintf("the child exited with %v with no test of it failed", err)
	}
	return ""
}

func logOfTheChild(name, output string) string {
	_, rest, found := strings.Cut(output, "=== RUN   "+name+"\n")
	if !found {
		return output
	}
	log, _, _ := strings.Cut(rest, "--- PASS: "+name+" ")
	return log
}

func testsInAProcessOfTheirOwn(t *testing.T) []string {
	t.Helper()
	var names []string
	for _, test := range guardedTests(t) {
		names = append(names, test.Name.Name)
	}
	if run := flag.Lookup("test.run"); run != nil {
		return selectedBy(run.Value.String(), names)
	}
	return names
}

func selectedBy(run string, names []string) []string {
	pattern, _, _ := strings.Cut(run, "/")
	selected := regexp.MustCompile(pattern)
	return slices.DeleteFunc(slices.Clone(names), func(name string) bool { return !selected.MatchString(name) })
}

func guardedTests(t *testing.T) []*ast.FuncDecl {
	t.Helper()
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no test files beside the test binary's working directory: %v", err)
	}
	var tests []*ast.FuncDecl
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && strings.HasPrefix(fn.Name.Name, "Test") && fn.Body != nil && callsBy(fn.Body, "inAProcessOfItsOwn", false) {
				tests = append(tests, fn)
			}
		}
	}
	return tests
}

func callsBy(body *ast.BlockStmt, name string, deferred bool) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		var call *ast.CallExpr
		switch n := node.(type) {
		case *ast.DeferStmt:
			if deferred {
				call = n.Call
			}
		case *ast.CallExpr:
			if !deferred {
				call = n
			}
		}
		if call != nil {
			if fn, ok := call.Fun.(*ast.Ident); ok && fn.Name == name {
				found = true
			}
		}
		return !found
	})
	return found
}

func TestATestInAProcessOfItsOwnRunsBesideNoGoroutineOfTheSuite(t *testing.T) {
	if inAProcessOfItsOwn(t) {
		return
	}
	defer theBodyRanToItsEnd(t)
	got := runtime.NumGoroutine()
	t.Logf("the test runs beside %d goroutines in all", got-1)
	if got > mostGoroutinesOfATestRunAlone {
		t.Errorf("the test runs among %d goroutines, want at most %d: the test's own, the runtime's and the testing package's", got, mostGoroutinesOfATestRunAlone)
	}
}

func TestTheTestsThatCountAllocationsOrReadTheHeapAreTheOnesThatRunApartEachRunningToItsEnd(t *testing.T) {
	want := []string{
		"TestABlockOfTrailersIsMergedIntoAMapOfOneNameInAllocationsEqualToItsParts",
		"TestABlockOfTrailersOfNamesNoAllocatorSharesIsReadInAllocationsEqualToItsParts",
		"TestACutStringHoldsNoneOfTheStringItWasCutFrom",
		"TestAFieldComparedOrSkippedIsNeverDecoded",
		"TestAKeyRepeatedByLinesKeepsTheSliceAppendGrewToInTheBytesTheAllocatorTakesForIt",
		"TestAMapOfHeadersHoldsAtMostWhatItsTablesTakeAtEverySplitPointAndOneKeyOnEachSideOfIt",
		"TestAnHTTP1HeadOfFieldLinesEndingInALineFeedIsHeldAtMostHeadAndTrailersHTTP1",
		"TestAnHTTP1HeadOfNamesAndFieldLinesTogetherWithTheTrailerValueIsHeldAtMostHeadAndTrailersHTTP1",
		"TestAnHTTP1HeadThatAnnouncesTheNamesOfTheDearestCountWithAFullBlockOfTrailersNoneOfThemAnnouncedIsHeldAtMostHeadAndTrailersHTTP1",
		"TestAnHTTP1HeadWhoseBlockOfTrailersRepeatsKeysIsHeldAtMostItsPartsAndHeadAndTrailersHTTP1",
		"TestAnHTTP1HeadWhoseTrailerValueIsOnSeveralLinesIsHeldAtMostItsPartsAndHeadAndTrailersHTTP1",
		"TestASliceOfPointersTakesTheSizeClassOfItsBytesAndTheAllocatorsHeaderOfEightBytes",
		"TestASliceOfStringsTakesTheSizeClassOfItsBytesAndTheAllocatorsHeaderPastFiveHundredAndTwelve",
		"TestAChildStartsABodyOnlyWhenTheGoroutinesOfEarlierTestsHaveEnded",
		"TestATallyBuildsNoNameForAnObjectThatDoesNotQualify",
		"TestATestInAProcessOfItsOwnRunsBesideNoGoroutineOfTheSuite",
		"TestATimeFieldHoldingAnObjectOrAnArrayIsNotATimeWhateverItsSize",
		"TestIoReadAllHoldsAtMostItsChunksAndTheSliceItReturnsAtTheCut",
		"TestTheHeadTheScanFindsMostWithItsTrailersIsHeldAtMostItsPartsAndTheTerm",
		"TestTheLeastAnHTTP1FieldLineAddsToAHeadIsItsCopiesAndAnElementOfTheSliceSinceTheMapGrowsInSteps",
		"TestTheMostAHeadOfDistinctNamesAndFieldLinesWithItsTrailersHoldsIsFoundByScanningItsAnnouncedNames",
		"TestTheSizeClassesTheMapsAreBuiltFromAreTheAllocators",
		"TestTheSizesTheTermsAreBuiltFromAreTheSourcesOnThisPlatform",
		"TestTheWorstHintIsTheDearestOfEveryHintATenantCanSetByTheBytesTheAllocatorTakesForTheSlice",
		"TestWhatAPassBuildsBesideTheSamplingIsUnderSummariesAtMost",
		"TestWhatASamplingKeepsRestsOnItsOwnBoundsAndNotOnWhatTheServerAnswers",
		"TestWhatTheTypesKeepAtTheRawBoundsIsUnderKeptAtMost",
	}
	var got []string
	for _, test := range guardedTests(t) {
		got = append(got, test.Name.Name)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("the tests that ask to run in a process of their own, apart from those listed: %v; listed and not asking: %v", missing(got, want), missing(want, got))
	}
	for _, test := range guardedTests(t) {
		if !callsBy(test.Body, "theBodyRanToItsEnd", true) {
			t.Errorf("%s runs in a process of its own and does not defer theBodyRanToItsEnd, so its parent cannot know that its body ran to its end", test.Name.Name)
		}
	}
}

func missing(have, want []string) []string {
	var out []string
	for _, name := range have {
		if !slices.Contains(want, name) {
			out = append(out, name)
		}
	}
	return out
}

func TestAProbeOfTheRunnerOfTestsInAProcessOfTheirOwn(t *testing.T) {
	mode := os.Getenv(runnerProbeEnv)
	if mode == "" {
		t.Skip("the tests of the runner run it in a child")
	}
	switch mode {
	case "skip":
		return
	case "exit":
		theBodyRanToItsEnd(t)
		fmt.Printf("--- PASS: %s (0.00s)\n", t.Name())
		os.Exit(66)
	case "fail":
		defer theBodyRanToItsEnd(t)
		t.Error("the probe fails")
	case "fails later":
		defer theBodyRanToItsEnd(t)
		t.Cleanup(func() { t.Error("the probe fails once its body is done") })
	default:
		defer theBodyRanToItsEnd(t)
	}
}

func TestTheRunnerFailsAChildWhoseBodyFailsOrIsSkippedOrWhoseProcessExitsWithAnErrorAfterItsPass(t *testing.T) {
	const probe = "TestAProbeOfTheRunnerOfTestsInAProcessOfTheirOwn"
	for _, c := range []struct {
		mode   string
		passes bool
	}{
		{"passes", true},
		{"fail", false},
		{"fails later", false},
		{"skip", false},
		{"exit", false},
	} {
		t.Run(c.mode, func(t *testing.T) {
			output, err := runTheTestsApart([]string{probe}, runnerProbeEnv+"="+c.mode)
			if problem := verdictOfTheChild(probe, output, err); (problem == "") != c.passes {
				t.Errorf("a child that %s: the parent says %q, want it to pass: %v\n%s", c.mode, problem, c.passes, output)
			}
		})
	}
}

func TestTheRunnerFailsAChildThatExitsWithAnErrorWithNoFailedTestAndNotOneWhoseOtherTestFailed(t *testing.T) {
	passed := "=== RUN   TestX\n" + bodyRanMarker + "TestX\n--- PASS: TestX (0.00s)\n"
	for _, c := range []struct {
		name, output string
		err          error
		passes       bool
	}{
		{"every test passed and the process exited well", passed, nil, true},
		{"every test passed and the process exited with an error", passed, os.ErrProcessDone, false},
		{"another test failed and the process exited with an error", passed + "--- FAIL: TestY (0.00s)\n", os.ErrProcessDone, true},
		{"the test passed and its body did not run", "--- PASS: TestX (0.00s)\n", nil, false},
		{"the test failed", "--- FAIL: TestX (0.00s)\n", os.ErrProcessDone, false},
		{"the test is a prefix of one that passed", "--- PASS: TestXY (0.00s)\n" + bodyRanMarker + "TestXY\n", nil, false},
		{"the test is a prefix of one that passed and its body ran", "--- PASS: TestXY (0.00s)\n" + bodyRanMarker + "TestX\n", nil, false},
		{"the test passed and it is a prefix of one whose body ran", "--- PASS: TestX (0.00s)\n" + bodyRanMarker + "TestXY\n", nil, false},
		{"the body ran and the test failed after it", bodyRanMarker + "TestX\n--- FAIL: TestX (0.00s)\n", os.ErrProcessDone, false},
	} {
		if problem := verdictOfTheChild("TestX", c.output, c.err); (problem == "") != c.passes {
			t.Errorf("%s: the parent says %q, want it to pass: %v", c.name, problem, c.passes)
		}
	}
}

func TestAChildStartsABodyOnlyWhenTheGoroutinesOfEarlierTestsHaveEnded(t *testing.T) {
	if inAProcessOfItsOwn(t) {
		return
	}
	defer theBodyRanToItsEnd(t)
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
		t.Errorf("with a goroutine left over the child settled to %d goroutines of %d (ok %v), want %d of %d and not settled", got, now, ok, first+1, first)
	}
	close(release)
	<-ended
	if got, now, ok := beside.settle(time.Second); !ok || now != first || got > first {
		t.Errorf("after it ended the child settled to %d goroutines of %d (ok %v), want %d or fewer of %d and settled", got, now, ok, first, first)
	}
	endsSoon := make(chan struct{})
	go func() {
		defer close(endsSoon)
		time.Sleep(100 * time.Millisecond)
	}()
	if got, now, ok := beside.settle(10 * time.Second); !ok || now != first || got > first {
		t.Errorf("with a goroutine that ends in 100 ms the child settled to %d goroutines of %d (ok %v), want %d or fewer of %d and settled", got, now, ok, first, first)
	}
	<-endsSoon
}

func TestTheRunnerAsksTheChildForTheTestsTheParentRuns(t *testing.T) {
	names := []string{"TestOne", "TestTwo", "TestOneAndAHalf"}
	for _, c := range []struct {
		run  string
		want []string
	}{
		{"", names},
		{"^TestOne$", []string{"TestOne"}},
		{"TestOne", []string{"TestOne", "TestOneAndAHalf"}},
		{"^(TestTwo|TestOne)$/sub", []string{"TestOne", "TestTwo"}},
		{"NoSuch", nil},
	} {
		if got := selectedBy(c.run, names); !slices.Equal(got, c.want) {
			t.Errorf("-run %q selects %v, want %v", c.run, got, c.want)
		}
	}
}
