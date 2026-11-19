// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package authnconfig_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"sigs.k8s.io/yaml"

	"go.lodia.io/lodia/internal/authnconfig"
	"go.lodia.io/lodia/internal/versions"
)

var reference = filepath.Join("..", "..", "test", "kubeauthn")

type difference struct{ minor, where, expression string }

var refusedOnlyByLodia = func() map[difference]string {
	claims := []string{"username", "claim rule", "extra"}
	known := map[difference]string{}
	for _, class := range []struct {
		name        string
		minors      []string
		expressions map[string][]string
	}{
		{"duration() of an int, which cel-go v0.28.1 removed (google/cel-go#1300)", []string{"1.35", "1.36"}, map[string][]string{
			"duration(int(claims.n))":                  {"username", "extra"},
			"duration(1 + 1)":                          {"username", "extra"},
			"duration(int(claims.n)) > duration('1s')": claims,
			"timestamp(claims.iat) + duration(int(claims.ttl)) > timestamp(claims.exp)": claims,
		}},
		{"a dot-prefixed identifier that names a comprehension variable around it, which cel-go v0.26.0 resolves to the variable (google/cel-go#1256)", []string{"1.35", "1.36"}, map[string][]string{
			"[[1]].exists(claims, .claims.all(c, c == 1))":                          claims,
			"[{'sub': 'a'}].exists(claims, .claims.sub == claims.sub)":              claims,
			"[1].exists(google, .google.protobuf.Duration == type(duration('1s')))": claims,
			"[1].exists(x, .x > 0)":                                                 claims,
			"{'a': 1}.all(k, v, .v > 0)":                                            claims,
			"string([{'a': 1}].map(claims, .claims.email)[0])":                      {"extra"},
			"[{'a': 1}].map(claims, string(.claims.email))[0]":                      {"extra"},
			"[{'email': 'a'}].map(claims, .claims.email)[0]":                        {"extra"},
			"[{'email': 'a'}].map(claims, string(.claims.?email.orValue('')))[0]":   {"extra"},
			"[{'username': 'a'}].exists(user, .user.username == user.username)":     {"user rule"},
			"[{'username': 1}].exists(user, .user.username == 1)":                   {"user rule"},
			"optional.of({'sub': 'a'}).optMap(claims, .claims.sub).orValue('')":     {"username", "extra"},
		}},
		{"a qualified name whose first part names a comprehension variable around it, which cel-go v0.29.2 resolves through the variable and v0.26.0 as the whole name (google/cel-go#1256)", []string{"1.35", "1.36"}, map[string][]string{
			"[1].exists(google, google.protobuf.Duration == type(duration('1s')))": claims,
		}},
	} {
		for _, minor := range class.minors {
			for expression, places := range class.expressions {
				for _, where := range places {
					known[difference{minor, where, expression}] = class.name
				}
			}
		}
	}
	return known
}()

type place struct {
	where string
	put   func(*apiserverv1.JWTAuthenticator, string)
}

var places = map[string][]place{
	"claims": {
		{"username", func(a *apiserverv1.JWTAuthenticator, e string) { a.ClaimMappings.Username.Expression = e }},
		{"claim rule", func(a *apiserverv1.JWTAuthenticator, e string) {
			a.ClaimValidationRules = []apiserverv1.ClaimValidationRule{{Expression: e}}
		}},
		{"extra", func(a *apiserverv1.JWTAuthenticator, e string) {
			a.ClaimMappings.Extra = []apiserverv1.ExtraMapping{{Key: "example.com/x", ValueExpression: e}}
		}},
	},
	"user": {
		{"user rule", func(a *apiserverv1.JWTAuthenticator, e string) {
			a.UserValidationRules = []apiserverv1.UserValidationRule{{Expression: e}}
		}},
	},
}

type check struct {
	where, expression string
	file              []byte
}

func corpus(t *testing.T) []check {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "corpus.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var checks []check
	for line := range strings.Lines(string(data)) {
		line = strings.TrimSuffix(line, "\n")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kind, expression, ok := strings.Cut(line, "\t")
		if !ok || places[kind] == nil {
			t.Fatalf("the corpus line %q is not claims or user, a tab and an expression", line)
		}
		for _, p := range places[kind] {
			a := apiserverv1.JWTAuthenticator{
				Issuer:        apiserverv1.Issuer{URL: "https://login.example.com", Audiences: []string{"lodia"}, EgressSelectorType: apiserverv1.EgressSelectorCluster},
				ClaimMappings: apiserverv1.ClaimMappings{Username: apiserverv1.PrefixedClaimOrExpression{Expression: "'oidc:' + claims.sub"}},
			}
			p.put(&a, expression)
			file, err := yaml.Marshal(apiserverv1.AuthenticationConfiguration{
				TypeMeta: metav1.TypeMeta{APIVersion: apiserverv1.SchemeGroupVersion.String(), Kind: "AuthenticationConfiguration"},
				JWT:      []apiserverv1.JWTAuthenticator{a},
			})
			if err != nil {
				t.Fatal(err)
			}
			checks = append(checks, check{p.where, expression, file})
		}
	}
	return checks
}

type kubeAPIServer struct {
	Versions map[string]string `json:"versions"`
	Errors   []string          `json:"errors"`
}

func kubeAPIServerOf(t *testing.T, minor string, files []string) kubeAPIServer {
	t.Helper()
	modfile, err := os.ReadFile(filepath.Join(reference, minor+".mod"))
	if err != nil {
		t.Fatalf("no reference for %s: %v", minor, err)
	}
	apiserver := "v0." + strings.TrimPrefix(minor, "1.") + "."
	if !bytes.Contains(modfile, []byte("\tk8s.io/apiserver "+apiserver)) {
		t.Fatalf("%s.mod requires no k8s.io/apiserver %s*", minor, apiserver)
	}
	binary := filepath.Join(t.TempDir(), "kubeauthn")
	build := exec.Command("go", "build", "-modfile="+minor+".mod", "-o", binary, ".")
	build.Dir = reference
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the reference of %s: %v\n%s", minor, err, out)
	}
	input, err := json.Marshal(files)
	if err != nil {
		t.Fatal(err)
	}
	run := exec.Command(binary)
	run.Stdin = bytes.NewReader(input)
	out, err := run.Output()
	if err != nil {
		t.Fatalf("run the reference of %s: %v", minor, err)
	}
	var k kubeAPIServer
	if err := json.Unmarshal(out, &k); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(k.Versions["k8s.io/apiserver"], apiserver) || len(k.Errors) != len(files) {
		t.Fatalf("the reference of %s links k8s.io/apiserver %s and answers %d of %d files", minor, k.Versions["k8s.io/apiserver"], len(k.Errors), len(files))
	}
	return k
}

func TestValidateAgreesWithTheKubeAPIServerOfEveryMinorOnTheCorpus(t *testing.T) {
	checks := corpus(t)
	files := make([]string, len(checks))
	for i, c := range checks {
		files[i] = string(c.file)
	}
	operator, err := exec.Command("go", "list", "-m", "-f", "{{.Path}} {{.Version}}", "k8s.io/apiserver", "github.com/google/cel-go").Output()
	if err != nil {
		t.Fatal(err)
	}
	mods, err := filepath.Glob(filepath.Join(reference, "*.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, mod := range mods {
		if minor := strings.TrimSuffix(filepath.Base(mod), ".mod"); minor != "go" && !slices.Contains(versions.Minors(), minor) {
			t.Errorf("%s is the reference of %s, a minor Lodia does not run", mod, minor)
		}
	}
	seen := map[difference]bool{}
	for _, minor := range versions.Minors() {
		k := kubeAPIServerOf(t, minor, files)
		t.Logf("kube-apiserver %s: k8s.io/apiserver %s, github.com/google/cel-go %s; Lodia: %s", minor, k.Versions["k8s.io/apiserver"], k.Versions["github.com/google/cel-go"], strings.Join(strings.Fields(string(operator)), " "))
		accepted := 0
		for i, c := range checks {
			d := difference{minor, c.where, c.expression}
			seen[d] = true
			lodia := authnconfig.Validate(c.file, []string{minor}, nil, nil)
			upstream := k.Errors[i]
			if upstream == "" {
				accepted++
			}
			switch {
			case lodia == nil && upstream != "":
				t.Errorf("Lodia accepts at %s the %s %q, which kube-apiserver %s refuses: %s", minor, c.where, c.expression, minor, upstream)
			case lodia != nil && upstream == "" && refusedOnlyByLodia[d] == "":
				t.Errorf("Lodia refuses at %s the %s %q, which kube-apiserver %s accepts, and it is no known difference: %v", minor, c.where, c.expression, minor, lodia)
			case (lodia == nil) == (upstream == "") && refusedOnlyByLodia[d] != "":
				t.Errorf("the known difference %+v, %s, is none: both %s it", d, refusedOnlyByLodia[d], map[bool]string{true: "accept", false: "refuse"}[lodia == nil])
			}
		}
		t.Logf("kube-apiserver %s accepts %d of %d", minor, accepted, len(files))
	}
	for d, class := range refusedOnlyByLodia {
		if !seen[d] {
			t.Errorf("the known difference %+v, %s, is not in the corpus", d, class)
		}
	}
}
