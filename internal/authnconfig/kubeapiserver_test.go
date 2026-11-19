// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package authnconfig_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"k8s.io/apiserver/pkg/features"
	"sigs.k8s.io/yaml"

	"go.lodia.io/lodia/internal/authnconfig"
	"go.lodia.io/lodia/internal/versions"
)

var reference = filepath.Join("..", "..", "test", "kubeauthn")

type difference struct{ minor, where, name string }

var refusedOnlyByLodia = func() map[difference]string {
	claims := []string{"username", "claim rule", "extra"}
	known := map[difference]string{}
	for _, class := range []struct {
		name   string
		minors []string
		names  map[string][]string
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
		{"an http or https URL whose host holds more than one colon, which Go 1.26's url.Parse refuses and kube-apiserver 1.35, under godebug default=go1.25, accepts (urlstrictcolons)", []string{"1.35"}, map[string][]string{
			"two colons in the issuer's host":        {"file"},
			"two colons in the discovery URL's host": {"file"},
		}},
	} {
		for _, minor := range class.minors {
			for name, places := range class.names {
				for _, where := range places {
					known[difference{minor, where, name}] = class.name
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
	where, name string
	file        []byte
	issuers     []string
	gates       map[string]bool
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
			checks = append(checks, check{where: p.where, name: expression, file: file})
		}
	}
	return checks
}

const file = `apiVersion: apiserver.config.k8s.io/v1
kind: AuthenticationConfiguration
jwt:
- issuer:
    url: https://login.example.com
    audiences: [lodia]
    egressSelectorType: cluster
  claimMappings:
    username: {expression: "'oidc:' + claims.sub"}
`

func files(t *testing.T) []check {
	t.Helper()
	with := func(old, replacement string) []byte {
		t.Helper()
		if !strings.Contains(file, old) {
			t.Fatalf("the file holds no %q", old)
		}
		return []byte(strings.Replace(file, old, replacement, 1))
	}
	url := func(url string) []byte { return with("url: https://login.example.com\n", "url: "+url+"\n") }
	discovery := func(url string) []byte {
		return with("    egressSelectorType: cluster\n", "    egressSelectorType: cluster\n    discoveryURL: "+url+"\n")
	}
	audiences := func(lines string) []byte { return with("    audiences: [lodia]\n", lines) }
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "issuer"}, NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca := "    certificateAuthority: |\n      " + strings.ReplaceAll(strings.TrimSpace(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))), "\n", "\n      ") + "\n"
	gateOff := map[string]bool{string(features.StructuredAuthenticationConfigurationEgressSelector): false}
	var checks []check
	for _, c := range []struct {
		name    string
		file    []byte
		issuers []string
		gates   map[string]bool
	}{
		{name: "the file", file: []byte(file)},
		{name: "an unknown field", file: with("jwt:\n", "notAField: 1\njwt:\n")},
		{name: "a mis-cased field", file: with("    audiences:", "    Audiences:")},
		{name: "a mis-cased kind", file: with("kind: AuthenticationConfiguration", "kind: authenticationConfiguration")},
		{name: "a duplicate key", file: audiences("    audiences: [lodia]\n    audiences: [lodia]\n")},
		{name: "an upper-case scheme in the issuer", file: url("HTTPS://login.example.com")},
		{name: "two colons in the issuer's host", file: url("https://login.example.com:1:443")},
		{name: "an IPv6 issuer", file: url("https://[2001:db8::1]:8443")},
		{name: "a user in the issuer", file: url("https://alice@login.example.com")},
		{name: "a query in the issuer", file: url("https://login.example.com/?realm=a")},
		{name: "an empty query in the issuer", file: url("https://login.example.com/?")},
		{name: "a fragment in the issuer", file: url("https://login.example.com/#a")},
		{name: "a long s in the issuer's scheme", file: url("httpſ://login.example.com")},
		{name: "an http issuer", file: url("http://login.example.com")},
		{name: "a discovery URL", file: discovery("https://10.0.0.1/.well-known/openid-configuration")},
		{name: "an upper-case scheme in the discovery URL", file: discovery("HTTPS://10.0.0.1/x")},
		{name: "two colons in the discovery URL's host", file: discovery("https://10.0.0.1:1:443/x")},
		{name: "a user in the discovery URL", file: discovery("https://alice@10.0.0.1/x")},
		{name: "a query in the discovery URL", file: discovery("https://10.0.0.1/x?realm=a")},
		{name: "a fragment in the discovery URL", file: discovery("https://10.0.0.1/x#a")},
		{name: "a long s in the discovery URL's scheme", file: discovery("httpſ://10.0.0.1/x")},
		{name: "the issuer with a slash as the discovery URL", file: discovery("https://login.example.com/")},
		{name: "two audiences", file: audiences("    audiences: [lodia, other]\n")},
		{name: "two audiences that match any", file: audiences("    audiences: [lodia, other]\n    audienceMatchPolicy: MatchAny\n")},
		{name: "one audience that matches any", file: audiences("    audiences: [lodia]\n    audienceMatchPolicy: MatchAny\n")},
		{name: "a policy that is not MatchAny", file: audiences("    audiences: [lodia, other]\n    audienceMatchPolicy: MatchAll\n")},
		{name: "an empty audience", file: audiences("    audiences: ['']\n")},
		{name: "an audience twice", file: audiences("    audiences: [lodia, lodia]\n    audienceMatchPolicy: MatchAny\n")},
		{name: "no audience", file: audiences("    audiences: []\n")},
		{name: "a CA", file: with("    egressSelectorType: cluster\n", "    egressSelectorType: cluster\n"+ca)},
		{name: "a CA that does not parse", file: with("    egressSelectorType: cluster\n", "    egressSelectorType: cluster\n    certificateAuthority: not a certificate\n")},
		{name: "the egress selector's gate off", file: []byte(file), gates: gateOff},
		{name: "the egress selector's gate off without an egress selector", file: with("    egressSelectorType: cluster\n", ""), gates: gateOff},
		{name: "the ServiceAccount issuer as the issuer", file: []byte(file), issuers: []string{"https://login.example.com"}},
		{name: "the ServiceAccount issuer with a slash more than the issuer", file: []byte(file), issuers: []string{"https://login.example.com/"}},
	} {
		checks = append(checks, check{where: "file", name: c.name, file: c.file, issuers: c.issuers, gates: c.gates})
	}
	return checks
}

type kubeAPIServer struct {
	Versions map[string]string `json:"versions"`
	Errors   []string          `json:"errors"`
}

func referenceOf(t *testing.T, minor string) string {
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
	graph := exec.Command("go", "mod", "graph", "-modfile="+minor+".mod")
	graph.Dir = reference
	edges, err := graph.Output()
	if err != nil {
		t.Fatalf("the module graph of %s.mod: %v", minor, err)
	}
	selected := exec.Command("go", "list", "-m", "-modfile="+minor+".mod", "-f", "{{.Path}}@{{.Version}}", "k8s.io/apiserver", "github.com/google/cel-go", "github.com/antlr4-go/antlr/v4", "cel.dev/expr")
	selected.Dir = reference
	out, err := selected.Output()
	if err != nil {
		t.Fatalf("the modules %s.mod selects: %v", minor, err)
	}
	modules := strings.Fields(string(out))
	for _, module := range modules[1:] {
		if !slices.Contains(strings.Split(string(edges), "\n"), modules[0]+" "+module) {
			t.Errorf("%s.mod selects %s, which %s does not require", minor, module, modules[0])
		}
	}
	return binary
}

type reported struct {
	FeatureGates map[string]bool `json:"featureGates"`
	Files        []reportedFile  `json:"files"`
}

type reportedFile struct {
	Data                  string   `json:"data"`
	ServiceAccountIssuers []string `json:"serviceAccountIssuers"`
}

func kubeAPIServerOf(t *testing.T, binary, minor string, checks []check) kubeAPIServer {
	t.Helper()
	k := kubeAPIServer{Errors: make([]string, len(checks))}
	runs := map[string][]int{}
	for i, c := range checks {
		runs[fmt.Sprint(c.gates)] = append(runs[fmt.Sprint(c.gates)], i)
	}
	for _, indices := range runs {
		in := reported{FeatureGates: checks[indices[0]].gates}
		for _, i := range indices {
			in.Files = append(in.Files, reportedFile{Data: string(checks[i].file), ServiceAccountIssuers: checks[i].issuers})
		}
		input, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		run := exec.Command(binary)
		run.Stdin = bytes.NewReader(input)
		out, err := run.Output()
		if err != nil {
			t.Fatalf("run the reference of %s: %v", minor, err)
		}
		var answered kubeAPIServer
		if err := json.Unmarshal(out, &answered); err != nil {
			t.Fatal(err)
		}
		if apiserver := "v0." + strings.TrimPrefix(minor, "1.") + "."; !strings.HasPrefix(answered.Versions["k8s.io/apiserver"], apiserver) || len(answered.Errors) != len(indices) {
			t.Fatalf("the reference of %s links k8s.io/apiserver %s and answers %d of %d files", minor, answered.Versions["k8s.io/apiserver"], len(answered.Errors), len(indices))
		}
		k.Versions = answered.Versions
		for n, i := range indices {
			k.Errors[i] = answered.Errors[n]
		}
	}
	return k
}

func TestValidateAgreesWithTheKubeAPIServerOfEveryMinorOnTheCorpus(t *testing.T) {
	checks := append(corpus(t), files(t)...)
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
		k := kubeAPIServerOf(t, referenceOf(t, minor), minor, checks)
		t.Logf("kube-apiserver %s: k8s.io/apiserver %s, github.com/google/cel-go %s; Lodia: %s", minor, k.Versions["k8s.io/apiserver"], k.Versions["github.com/google/cel-go"], strings.Join(strings.Fields(string(operator)), " "))
		accepted := 0
		for i, c := range checks {
			d := difference{minor, c.where, c.name}
			seen[d] = true
			lodia := authnconfig.Validate(c.file, []string{minor}, c.issuers, c.gates)
			upstream := k.Errors[i]
			if upstream == "" {
				accepted++
			}
			switch {
			case lodia == nil && upstream != "":
				t.Errorf("Lodia accepts at %s the %s %q, which kube-apiserver %s refuses: %s", minor, c.where, c.name, minor, upstream)
			case lodia != nil && upstream == "" && refusedOnlyByLodia[d] == "":
				t.Errorf("Lodia refuses at %s the %s %q, which kube-apiserver %s accepts, and it is no known difference: %v", minor, c.where, c.name, minor, lodia)
			case (lodia == nil) == (upstream == "") && refusedOnlyByLodia[d] != "":
				t.Errorf("the known difference %+v, %s, is none: both %s it", d, refusedOnlyByLodia[d], map[bool]string{true: "accept", false: "refuse"}[lodia == nil])
			}
		}
		t.Logf("kube-apiserver %s accepts %d of %d", minor, accepted, len(checks))
	}
	for d, class := range refusedOnlyByLodia {
		if !seen[d] {
			t.Errorf("the known difference %+v, %s, is not in the corpus", d, class)
		}
	}
}
