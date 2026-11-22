// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"regexp"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"
)

const (
	ciWorkflow   = "ci.yml"
	workflowsDir = ".github/workflows"
	nightlyGlob  = "nightly*.yml"

	pushEvent     = "push"
	scheduleEvent = "schedule"
	dispatchEvent = "workflow_dispatch"
)

// The one expression a run-name may hold, which GitHub evaluates for a
// scheduled run, whose inputs are empty, to the text in quotes.
var inputOrDefault = regexp.MustCompile(`\$\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*\|\|\s*'([^']*)'\s*\}\}`)

// nightly is a workflow file at the commit, and the title its runs have at the
// default of every input: a run title is the workflow's run-name, else its name.
type nightly struct {
	file, title string
}

// ci.yml's run is the one that counts: on the commit of a tag, release.yml's
// own run is in progress.
func requireGreenCI(ctx context.Context, gh GitHub, sha string) error {
	all, err := gh.Runs(ctx, ciWorkflow, sha)
	if err != nil {
		return err
	}
	var runs []Run
	for _, run := range all {
		if run.Event == pushEvent {
			runs = append(runs, run)
		}
	}
	if len(runs) == 0 {
		return fmt.Errorf("%s has no run for the push of %s to main: wait for it to start, or tag a commit that was pushed to main", ciWorkflow, sha)
	}
	var unmet []string
	for _, run := range runs {
		switch {
		case run.Status != completed:
			unmet = append(unmet, fmt.Sprintf("%s is %s: wait for it", run.URL, run.Status))
		case run.Conclusion != success:
			unmet = append(unmet, fmt.Sprintf("%s ended as %s: fix it on main and tag the commit that fixes it, or re-run it if it flaked", run.URL, run.Conclusion))
		}
	}
	if len(unmet) > 0 {
		return fmt.Errorf("%s is not green on %s: %s", ciWorkflow, sha, strings.Join(unmet, "; "))
	}
	return nil
}

func requireGreenNightlies(ctx context.Context, dir string, gh GitHub, sha string) error {
	nightlies, err := nightliesAt(ctx, dir, sha)
	if err != nil {
		return err
	}
	var problems []error
	for _, n := range nightlies {
		problems = append(problems, requireGreenNightly(ctx, gh, sha, n))
	}
	return errors.Join(problems...)
}

// The newest run at the commit that is complete decides. A run in progress on
// its first attempt is not counted. A run in progress on a later attempt is
// counted as the attempt before it, which the run object no longer shows. A
// dispatched run counts only at the default of its inputs.
func requireGreenNightly(ctx context.Context, gh GitHub, sha string, n nightly) error {
	runs, err := gh.Runs(ctx, n.file, sha)
	if err != nil {
		return err
	}
	type verdict struct {
		run        Run
		conclusion string
	}
	var counted []verdict
	otherSize := 0
	for _, run := range runs {
		switch {
		case run.Event != scheduleEvent && run.Event != dispatchEvent:
			continue
		case run.Event == dispatchEvent && run.Title != n.title:
			otherSize++
			continue
		}
		conclusion := run.Conclusion
		if run.Status != completed {
			if run.Attempt <= 1 {
				continue
			}
			previous, err := gh.Attempt(ctx, run.ID, run.Attempt-1)
			if err != nil {
				return err
			}
			conclusion = previous.Conclusion
		}
		counted = append(counted, verdict{run, conclusion})
	}
	if len(counted) == 0 {
		missing := fmt.Sprintf("%s has no completed run on main at %s: dispatch it with `gh workflow run %s --ref main` while main is at %s, and wait for it", n.file, sha, n.file, sha)
		if otherSize > 0 {
			missing += fmt.Sprintf(" (%d dispatched with a title other than %q, which does not count)", otherSize, n.title)
		}
		return errors.New(missing)
	}
	newest := slices.MaxFunc(counted, func(a, b verdict) int {
		return cmp.Or(a.run.Started.Compare(b.run.Started), cmp.Compare(a.run.ID, b.run.ID))
	})
	if newest.conclusion != success {
		return fmt.Errorf("%s, the newest run of %s at %s, ended as %s: re-run it with `gh run rerun %d` if it flaked, else fix main and tag a later commit", newest.run.URL, n.file, sha, newest.conclusion, newest.run.ID)
	}
	return nil
}

// The list is the files at the commit, so a nightly added or removed later does
// not move it, and an empty list refuses.
func nightliesAt(ctx context.Context, dir, sha string) ([]nightly, error) {
	out, err := git(ctx, dir, "ls-tree", "-z", "--name-only", sha+":"+workflowsDir)
	if err != nil {
		return nil, err
	}
	var nightlies []nightly
	for name := range strings.SplitSeq(string(out), "\x00") {
		if matched, _ := path.Match(nightlyGlob, name); !matched {
			continue
		}
		raw, err := git(ctx, dir, "show", sha+":"+workflowsDir+"/"+name)
		if err != nil {
			return nil, err
		}
		n, err := parseNightly(name, raw)
		if err != nil {
			return nil, fmt.Errorf("%s at %s: %w", name, sha, err)
		}
		nightlies = append(nightlies, n)
	}
	if len(nightlies) == 0 {
		return nil, fmt.Errorf("no %s/%s at %s: a release needs every nightly", workflowsDir, nightlyGlob, sha)
	}
	return nightlies, nil
}

// An input of workflow_dispatch is part of a run's title at the defaults only
// if the run-name states it with its default, and a boolean or a number is not:
// || turns its false or 0 into the default.
func parseNightly(file string, raw []byte) (nightly, error) {
	var workflow struct {
		Name    string `json:"name"`
		RunName string `json:"run-name"`
		On      any    `json:"true"`
		Quoted  any    `json:"on"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		return nightly{}, err
	}
	on := workflow.On
	if on == nil {
		on = workflow.Quoted
	}
	stated := map[string]string{}
	for _, m := range inputOrDefault.FindAllStringSubmatch(workflow.RunName, -1) {
		stated[m[1]] = m[2]
	}
	inputs := dispatchInputs(on)
	var refused []error
	for _, name := range slices.Sorted(maps.Keys(inputs)) {
		switch got, ok := stated[name]; {
		case inputs[name].kind == "boolean" || inputs[name].kind == "number":
			refused = append(refused, fmt.Errorf("the input %s of workflow_dispatch is of type %s, whose false or 0 the || in the run-name turns into the default, so a run at it would count as a run at the defaults", name, inputs[name].kind))
		case !ok || got != inputs[name].def:
			refused = append(refused, fmt.Errorf("the input %s of workflow_dispatch is not stated in the run-name as `${{ inputs.%s || '%s' }}`, so the title of a run at the defaults is not known", name, name, inputs[name].def))
		}
	}
	if err := errors.Join(refused...); err != nil {
		return nightly{}, err
	}
	title := workflow.Name
	if workflow.RunName != "" {
		title = inputOrDefault.ReplaceAllString(workflow.RunName, "$2")
		if strings.Contains(title, "${{") {
			return nightly{}, fmt.Errorf("the run-name %q has an expression other than `${{ inputs.<name> || '<default>' }}`, so the title of a run at the defaults is not known", workflow.RunName)
		}
	}
	if title == "" {
		return nightly{}, errors.New("no name")
	}
	return nightly{file: file, title: title}, nil
}

type input struct {
	kind, def string
}

func dispatchInputs(on any) map[string]input {
	events, _ := on.(map[string]any)
	dispatch, _ := events["workflow_dispatch"].(map[string]any)
	declared, _ := dispatch["inputs"].(map[string]any)
	inputs := map[string]input{}
	for name, spec := range declared {
		fields, _ := spec.(map[string]any)
		var in input
		in.kind, _ = fields["type"].(string)
		if fields["default"] != nil {
			in.def = fmt.Sprint(fields["default"])
		}
		inputs[name] = in
	}
	return inputs
}
