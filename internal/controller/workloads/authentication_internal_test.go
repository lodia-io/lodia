// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"bytes"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	mainsAPIServerHash         = "2d4f0bddc7d6802981b06b6e1e3df9f8d3d6f936d8827b9ae95b1ac3c0934e14"
	mainsControllerManagerHash = "bdb8f6f104f4b5818271c0ee79534da435cfcaef02014aa315c84852e998b182"
	mainsAuthentication        = "anonymous:\n  conditions:\n  - path: /healthz\n  - path: /livez\n  - path: /readyz\n  - path: /api/v1/namespaces/kube-public/configmaps/cluster-info\n  enabled: true\napiVersion: apiserver.config.k8s.io/v1\njwt: []\nkind: AuthenticationConfiguration\n"
)

func acceptance() []lodiav1alpha1.JWTAuthenticator {
	return []lodiav1alpha1.JWTAuthenticator{
		{
			Issuer: lodiav1alpha1.Issuer{URL: "https://login.example.com/realms/team-a", Audiences: []string{"team-a-cluster"}},
			ClaimValidationRules: []lodiav1alpha1.ClaimValidationRule{
				{Expression: "claims.tenant == 'team-a'", Message: "the token is not team-a's"},
				{Claim: "hd", RequiredValue: "example.com"},
			},
			ClaimMappings: lodiav1alpha1.ClaimMappings{
				Username: lodiav1alpha1.PrefixedClaimOrExpression{Expression: "'oidc:' + claims.sub"},
				Groups:   lodiav1alpha1.PrefixedClaimOrExpression{Expression: "dyn(claims.roles).map(r, 'oidc:' + r)"},
				UID:      lodiav1alpha1.ClaimOrExpression{Expression: "claims.sub"},
				Extra:    []lodiav1alpha1.ExtraMapping{{Key: "example.com/tenant", ValueExpression: "claims.tenant"}},
			},
			UserValidationRules: []lodiav1alpha1.UserValidationRule{{Expression: "!('oidc:suspended' in user.groups)", Message: "the user is suspended"}},
		},
		{
			Issuer:              lodiav1alpha1.Issuer{URL: "https://dex.dex.svc", Audiences: []string{"a", "b"}, AudienceMatchPolicy: "MatchAny", EgressSelectorType: "cluster"},
			ClaimMappings:       lodiav1alpha1.ClaimMappings{Username: lodiav1alpha1.PrefixedClaimOrExpression{Claim: "email", Prefix: new("")}},
			UserValidationRules: []lodiav1alpha1.UserValidationRule{{Expression: reservedNames.Expression, Message: "the tenant's own words"}},
		},
	}
}

func restartInputs(cp string) map[string]string {
	data := map[string]string{}
	for _, secrets := range restartOnly {
		for _, s := range secrets {
			data[s.Name(cp)] = "sha256:" + s.Name(cp)
		}
	}
	return data
}

func decodeAuthentication(t *testing.T, c *corev1.ConfigMap) apiserverv1.AuthenticationConfiguration {
	t.Helper()
	var out apiserverv1.AuthenticationConfiguration
	if err := yaml.UnmarshalStrict([]byte(c.Data[authenticationFile]), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestTheRestartHashIsMainsWhateverTheJWTAuthenticators(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a"}}
	for _, jwt := range [][]lodiav1alpha1.JWTAuthenticator{nil, {}, acceptance(), acceptance()[1:]} {
		cp.Spec.APIServer = &lodiav1alpha1.APIServer{Authentication: &lodiav1alpha1.Authentication{JWT: jwt}}
		written, hashed, err := configMaps(cp)
		if err != nil {
			t.Fatal(err)
		}
		got := restartHashes(cp.Name, restartInputs(cp.Name), hashed)
		if got[apiServer] != mainsAPIServerHash || got[controllerManager] != mainsControllerManagerHash {
			t.Errorf("with %d JWT authenticators the restart hashes are %v, want main's %s and %s", len(jwt), got, mainsAPIServerHash, mainsControllerManagerHash)
		}
		if authn := written[1].Data[authenticationFile]; (len(jwt) == 0) != (authn == mainsAuthentication) {
			t.Errorf("with %d JWT authenticators Workloads writes\n%s", len(jwt), authn)
		}
	}
	cp.Spec.APIServer = nil
	if written, _, err := configMaps(cp); err != nil || written[1].Data[authenticationFile] != mainsAuthentication {
		t.Errorf("without apiServer Workloads writes %v, %v", written, err)
	}
}

func TestAChangedAnonymousSectionRollsTheAPIServer(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a"}}
	cp.Spec.APIServer = &lodiav1alpha1.APIServer{Authentication: &lodiav1alpha1.Authentication{JWT: acceptance()}}
	_, hashed, err := configMaps(cp)
	if err != nil {
		t.Fatal(err)
	}
	before := restartHashes(cp.Name, nil, hashed)
	authn := decodeAuthentication(t, hashed[1])
	authn.Anonymous.Conditions = append(authn.Anonymous.Conditions, apiserverv1.AnonymousAuthCondition{Path: "/version"})
	data, err := yaml.Marshal(authn)
	if err != nil {
		t.Fatal(err)
	}
	changed := []*corev1.ConfigMap{hashed[0], configMap(cp, "authentication-config", authenticationFile, data)}
	if after := restartHashes(cp.Name, nil, changed); after[apiServer] == before[apiServer] || after[controllerManager] != before[controllerManager] {
		t.Errorf("an anonymous section that admits /version gives the restart hashes %v, then %v: want the API server's alone to change", before, after)
	}
}

func TestEveryJWTAuthenticatorGoesThroughKonnectivityAndKeepsTheReservedNames(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a"}}
	cp.Spec.APIServer = &lodiav1alpha1.APIServer{Authentication: &lodiav1alpha1.Authentication{JWT: acceptance()}}
	written, _, err := configMaps(cp)
	if err != nil {
		t.Fatal(err)
	}
	authn := decodeAuthentication(t, written[1])
	if len(authn.JWT) != 2 {
		t.Fatalf("Workloads renders %d JWT authenticators, want the spec's 2", len(authn.JWT))
	}
	for i, a := range authn.JWT {
		if a.Issuer.EgressSelectorType != apiserverv1.EgressSelectorCluster {
			t.Errorf("authenticator %d reaches its issuer through %q, want cluster", i, a.Issuer.EgressSelectorType)
		}
		if n := slices.IndexFunc(a.UserValidationRules, func(r apiserverv1.UserValidationRule) bool { return r.Expression == reservedNames.Expression }); n < 0 ||
			slices.ContainsFunc(a.UserValidationRules[n+1:], func(r apiserverv1.UserValidationRule) bool { return r.Expression == reservedNames.Expression }) {
			t.Errorf("authenticator %d has the user rules %+v, want the reserved names once", i, a.UserValidationRules)
		}
	}
	if got := authn.JWT[0].UserValidationRules; len(got) != 2 || got[0].Expression != "!('oidc:suspended' in user.groups)" || got[1] != reservedNames {
		t.Errorf("the first authenticator's user rules are %+v, want the spec's and then Lodia's", got)
	}
	if got := authn.JWT[1].UserValidationRules; len(got) != 1 || got[0].Message != "the tenant's own words" {
		t.Errorf("the second authenticator's user rules are %+v, want the spec's alone, which is Lodia's expression", got)
	}
	none := decodeAuthentication(t, configMap(cp, "authentication-config", authenticationFile, []byte(mainsAuthentication)))
	if !equalAnonymous(authn.Anonymous, none.Anonymous) {
		t.Errorf("the anonymous section is %+v beside JWT authenticators, %+v without", authn.Anonymous, none.Anonymous)
	}
	path := filepath.Join("testdata", "render", "authentication.yaml")
	doc, err := yaml.Marshal(written[1])
	if err != nil {
		t.Fatal(err)
	}
	if *updateGolden {
		if err := os.WriteFile(path, doc, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(doc, want) {
		t.Errorf("the authentication configuration renders other bytes than %s:\n%s", path, doc)
	}
}

func equalAnonymous(a, b *apiserverv1.AnonymousAuthConfig) bool {
	return a != nil && b != nil && a.Enabled == b.Enabled && slices.Equal(a.Conditions, b.Conditions)
}

func TestAnInvalidConfigurationComesBeforeEveryTrueReasonAndAfterTheFalseOnes(t *testing.T) {
	const refused = "the authentication configuration of spec.apiServer.authentication is not written: kube-apiserver 1.37 refuses it: jwt[0].userValidationRules[0].expression: Invalid value: \"'; the authentication configuration of spec.apiServer.authentication is not written: ' == user.username\""
	ready := func(d *appsv1.Deployment) { d.Status.ReadyReplicas = 1 }
	stalled := func(d *appsv1.Deployment) {
		d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: timedOutReason}}
	}
	for _, c := range []struct {
		name      string
		apiServer []func(*appsv1.Deployment)
		status    metav1.ConditionStatus
		reason    string
	}{
		{"every component ready", []func(*appsv1.Deployment){ready}, metav1.ConditionTrue, lodiav1alpha1.ReasonAuthenticationInvalid},
		{"a rollout past its deadline that still serves", []func(*appsv1.Deployment){ready, stalled}, metav1.ConditionTrue, lodiav1alpha1.ReasonAuthenticationInvalid},
		{"a rollout past its deadline with no ready replica", []func(*appsv1.Deployment){stalled}, metav1.ConditionFalse, lodiav1alpha1.ReasonProgressDeadlineExceeded},
		{"no ready replica", nil, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable},
	} {
		t.Run(c.name, func(t *testing.T) {
			var deployments []*appsv1.Deployment
			for _, component := range components {
				d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: component, Generation: 2, Labels: map[string]string{lodiav1alpha1.ComponentLabel: component}}}
				d.Status.ObservedGeneration = 2
				ready(d)
				if component == apiServer {
					d.Status.ReadyReplicas = 0
					for _, change := range c.apiServer {
						change(d)
					}
				}
				deployments = append(deployments, d)
			}
			count := availability(deployments, "", false)
			for _, refusal := range []string{refused, refused + servesNone, refused + servesLast} {
				for _, served := range []string{servesLast, servesNone} {
					got := authenticated(count, refusal+served)
					if got.Status != c.status || got.Reason != c.reason {
						t.Errorf("APIServerReady is %s %s %q, want %s %s", got.Status, got.Reason, got.Message, c.status, c.reason)
					}
					if got.Message != count.Message+"; "+refusal+served {
						t.Errorf("APIServerReady %s says %q, want its count's message and the refusal", got.Reason, got.Message)
					}
				}
			}
			valid := authenticated(count, "")
			if valid.Reason == lodiav1alpha1.ReasonAuthenticationInvalid || valid.Message != count.Message {
				t.Errorf("a valid configuration gives %s %q", valid.Reason, valid.Message)
			}
		})
	}
}

func TestAControlPlaneGoneTakesItsValidationAlong(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	gone := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a"}}
	kept := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-b"}}
	written, _, err := configMaps(gone)
	if err != nil {
		t.Fatal(err)
	}
	for _, cp := range []*lodiav1alpha1.ControlPlane{gone, kept} {
		if err := r.validate(cp, written[1].Data[authenticationFile], []string{"https://203.0.113.10"}, written[0].Data[egressFile]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(gone)}); err != nil {
		t.Fatal(err)
	}
	if _, found := r.validations[client.ObjectKeyFromObject(gone)]; found || len(r.validations) != 1 {
		t.Errorf("after a pass that finds no ControlPlane team-a/dev Workloads keeps the validations of %v", slices.Collect(maps.Keys(r.validations)))
	}
}

func TestARefusedPassKeepsOnlyThisControlPlanesFileThatLoadsAndSaysWhatTheAPIServerServes(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a", UID: "dev-uid"}}
	endpoint := &lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: lodiav1alpha1.APIServerServicePort}}
	jwt := acceptance()
	cp.Spec.APIServer = &lodiav1alpha1.APIServer{Authentication: &lodiav1alpha1.Authentication{JWT: jwt}}
	valid, _, err := configMaps(cp)
	if err != nil {
		t.Fatal(err)
	}
	jwt[0].UserValidationRules = []lodiav1alpha1.UserValidationRule{{Expression: "user.nope == 'x'"}}
	config, hashed, err := configMaps(cp)
	if err != nil {
		t.Fatal(err)
	}
	const refused = "the authentication configuration of spec.apiServer.authentication is not written: kube-apiserver 1.37 refuses it: jwt[0].userValidationRules[0].expression: Invalid value: \"user.nope == 'x'\": compilation failed: ERROR: <input>:1:5: undefined field 'nope'\n | user.nope == 'x'\n | ....^"
	stored := func(data string, owners ...metav1.OwnerReference) *corev1.ConfigMap {
		c := hashed[1].DeepCopy()
		c.OwnerReferences = owners
		c.Data[authenticationFile] = data
		return c
	}
	own := hashed[1].OwnerReferences[0]
	const withoutJWT = "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthenticationConfiguration\n"
	compact, err := yaml.YAMLToJSON([]byte(valid[1].Data[authenticationFile]))
	if err != nil {
		t.Fatal(err)
	}
	escaped := strings.Replace(strings.ReplaceAll(string(compact), "https://", `https:\/\/`), `"message":"`, `"message":"\ud83d\ude00 `, 1)
	for _, c := range []struct {
		name         string
		stored       []client.Object
		kept         bool
		served, held string
	}{
		{"none stored", nil, false, servesNone, servesNone},
		{"a predecessor's orphan", []client.Object{stored(valid[1].Data[authenticationFile])}, false, servesNone, ""},
		{"a predecessor's orphan without JWT authenticators", []client.Object{stored(withoutJWT)}, false, servesNone, ""},
		{"this ControlPlane's, refused", []client.Object{stored(config[1].Data[authenticationFile], own)}, false, servesNone, ""},
		{"this ControlPlane's, refused, without JWT authenticators", []client.Object{stored(withoutJWT+"unknown: 1\n", own)}, false, servesNone, ""},
		{"this ControlPlane's, undecodable", []client.Object{stored("jwt: [", own)}, false, servesNone, ""},
		{"this ControlPlane's, loaded", []client.Object{stored(valid[1].Data[authenticationFile], own)}, true, servesLast, servesLast},
		{"this ControlPlane's without JWT authenticators", []client.Object{stored(hashed[1].Data[authenticationFile], own)}, true, servesNone, servesNone},
		{"this ControlPlane's in JSON with escapes go-yaml lacks", []client.Object{stored(escaped, own)}, true, servesLast, servesLast},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, revision := range []string{"", "held"} {
				fake := fake.NewClientBuilder().WithScheme(scheme).WithObjects(c.stored...).Build()
				r := &Reconciler{Client: fake, Reader: fake, Revision: revision}
				refusal, err := r.refusal(cp, endpoint, config)
				if err != nil {
					t.Fatal(err)
				}
				written, served, err := r.validated(t.Context(), cp, endpoint, refusal, config, hashed)
				if err != nil {
					t.Fatal(err)
				}
				want := c.served
				if r.held(cp) {
					want = c.held
				}
				if refusal != refused || served != want {
					t.Errorf("a pass held %v says\n%s%s\nwant\n%s%s", r.held(cp), refusal, served, refused, want)
				}
				switch {
				case c.kept && len(written) != 1:
					t.Errorf("Workloads writes %d ConfigMaps, want the egress selector's alone", len(written))
				case !c.kept && (len(written) != 2 || written[1].Data[authenticationFile] != mainsAuthentication):
					t.Errorf("Workloads writes %v, want the authentication configuration without JWT authenticators", written)
				}
			}
		})
	}
}
