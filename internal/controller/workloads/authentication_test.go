// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/authnconfig"
	"go.lodia.io/lodia/internal/controller/lifecycle"
	"go.lodia.io/lodia/internal/testenv"
	"go.lodia.io/lodia/internal/versions"
)

const (
	notWritten  = "the authentication configuration of spec.apiServer.authentication is not written: "
	nopeRefusal = notWritten + "kube-apiserver 1.37 refuses it: jwt[0].userValidationRules[0].expression: Invalid value: \"user.nope == 'x'\": compilation failed: ERROR: <input>:1:5: undefined field 'nope'\n | user.nope == 'x'\n | ....^"
)

func oidc(rules ...string) []lodiav1alpha1.JWTAuthenticator {
	a := lodiav1alpha1.JWTAuthenticator{
		Issuer:               lodiav1alpha1.Issuer{URL: "https://login.example.com/realms/team-a", Audiences: []string{"team-a-cluster"}},
		ClaimValidationRules: []lodiav1alpha1.ClaimValidationRule{{Expression: "claims.tenant == 'team-a'", Message: "the token is not team-a's"}},
		ClaimMappings: lodiav1alpha1.ClaimMappings{
			Username: lodiav1alpha1.PrefixedClaimOrExpression{Expression: "'oidc:' + claims.sub"},
			Groups:   lodiav1alpha1.PrefixedClaimOrExpression{Expression: "dyn(claims.roles).map(r, 'oidc:' + r)"},
			UID:      lodiav1alpha1.ClaimOrExpression{Expression: "claims.sub"},
			Extra:    []lodiav1alpha1.ExtraMapping{{Key: "example.com/tenant", ValueExpression: "claims.tenant"}},
		},
	}
	for _, rule := range rules {
		a.UserValidationRules = append(a.UserValidationRules, lodiav1alpha1.UserValidationRule{Expression: rule})
	}
	return []lodiav1alpha1.JWTAuthenticator{a}
}

func withJWT(jwt []lodiav1alpha1.JWTAuthenticator) func(*lodiav1alpha1.ControlPlaneSpec) {
	return func(s *lodiav1alpha1.ControlPlaneSpec) {
		if s.APIServer == nil {
			s.APIServer = &lodiav1alpha1.APIServer{}
		}
		s.APIServer.Authentication = &lodiav1alpha1.Authentication{JWT: jwt}
	}
}

func authenticationConfig(t *testing.T, cp *lodiav1alpha1.ControlPlane) *corev1.ConfigMap {
	t.Helper()
	c := &corev1.ConfigMap{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-authentication-config"}, c); err != nil {
		t.Fatal(err)
	}
	return c
}

func egressData(t *testing.T, cp *lodiav1alpha1.ControlPlane) []byte {
	t.Helper()
	c := &corev1.ConfigMap{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-egress-selector"}, c); err != nil {
		t.Fatal(err)
	}
	return []byte(c.Data["egress-selector.yaml"])
}

func expectAuthenticationInvalid(cp *lodiav1alpha1.ControlPlane, message string) error {
	fresh := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		return err
	}
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonAuthenticationInvalid || !strings.Contains(c.Message, message) || c.ObservedGeneration != fresh.Generation {
		return fmt.Errorf("APIServerReady is %+v, want False AuthenticationInvalid with %q at generation %d", c, message, fresh.Generation)
	}
	return nil
}

func TestAJWTEditReloadsAndAnInvalidOneIsNeverWritten(t *testing.T) {
	cp := running(t)
	hashed, generations := hashes(t, cp), generationsOf(t, cp)
	unchanged := func(step string) {
		t.Helper()
		if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
			t.Errorf("%s: the restart hashes are %v, then %v", step, hashed, after)
		}
	}
	if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") {
		t.Fatalf("without JWT authenticators Workloads writes\n%s", authn)
	}

	update(t, cp, withJWT(oidc("!user.groups.includes('oidc:suspended')")))
	testenv.Eventually(t, func() error {
		authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]
		if !strings.Contains(authn, "url: https://login.example.com/realms/team-a") || !strings.Contains(authn, "egressSelectorType: cluster") || !strings.Contains(authn, "includes") {
			return errors.New("the authentication configuration is\n" + authn)
		}
		return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
	})
	unchanged("the JWT authenticators added")
	if after := generationsOf(t, cp); fmt.Sprint(after) != fmt.Sprint(generations) {
		t.Errorf("the JWT authenticators added rolled the Deployments: generations %v, then %v", generations, after)
	}
	valid := authenticationConfig(t, cp)

	update(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) {
		withJWT(oidc("user.nope == 'x'"))(s)
		s.APIServer.ExtraArgs = map[string]string{"event-ttl": "2h"}
	})
	testenv.Eventually(t, func() error {
		if err := expectAuthenticationInvalid(cp, "the authentication configuration of spec.apiServer.authentication is not written: kube-apiserver 1.37 refuses it: jwt[0].userValidationRules[0].expression: Invalid value: \"user.nope == 'x'\": compilation failed"); err != nil {
			return err
		}
		if !strings.Contains(strings.Join(deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Command, " "), "--event-ttl=2h") {
			return errors.New("the API server's Deployment does not run the edit beside the invalid configuration")
		}
		return nil
	})
	if refused := authenticationConfig(t, cp); refused.ResourceVersion != valid.ResourceVersion || refused.Data["authentication-config.yaml"] != valid.Data["authentication-config.yaml"] {
		t.Errorf("the ConfigMap was written after a configuration that does not compile:\n%s", refused.Data["authentication-config.yaml"])
	}
	unchanged("an invalid configuration")
	markAvailable(t, deployment(t, cp, "kube-apiserver"))

	update(t, cp, withJWT(oidc("!('oidc:suspended' in user.groups)")))
	testenv.Eventually(t, func() error {
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "in user.groups)") {
			return errors.New("the fixed configuration is not written:\n" + authn)
		}
		return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
	})
	unchanged("the configuration fixed")
}

func withoutRule(t *testing.T, crd *unstructured.Unstructured, containing string, path ...string) {
	t.Helper()
	versions, _, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
	if err != nil || len(versions) != 1 {
		t.Fatalf("the CRD has versions %v: %v", versions, err)
	}
	path = append(append([]string{"schema", "openAPIV3Schema", "properties", "spec"}, path...), "x-kubernetes-validations")
	version := versions[0].(map[string]any)
	rules, _, err := unstructured.NestedSlice(version, path...)
	if err != nil {
		t.Fatal(err)
	}
	kept := slices.DeleteFunc(rules, func(r any) bool {
		return strings.Contains(r.(map[string]any)["rule"].(string), containing)
	})
	if len(kept) != len(rules)-1 {
		t.Fatalf("the CRD has %d rules on %s, not one", len(rules)-len(kept), containing)
	}
	if err := unstructured.SetNestedSlice(version, kept, path...); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedSlice(crd.Object, versions, "spec", "versions"); err != nil {
		t.Fatal(err)
	}
}

func controlPlanesCRD(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: "controlplanes.lodia.io"}, crd); err != nil {
		t.Fatal(err)
	}
	return crd
}

func updateCRD(ctx context.Context, crd *unstructured.Unstructured) error {
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(crd.GroupVersionKind())
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(crd), current); err != nil {
		return err
	}
	crd.SetResourceVersion(current.GetResourceVersion())
	return k8s.Update(ctx, crd)
}

func serveCRD(t *testing.T, crd *unstructured.Unstructured, ns string, probe func(*lodiav1alpha1.ControlPlaneSpec), refused string) {
	t.Helper()
	if err := updateCRD(t.Context(), crd); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		cp := &lodiav1alpha1.ControlPlane{
			ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns},
			Spec: lodiav1alpha1.ControlPlaneSpec{
				Version:  version,
				Endpoint: loadBalancer(),
				Network:  lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
			},
		}
		probe(&cp.Spec)
		got := ""
		if err := k8s.Create(t.Context(), cp, client.DryRunAll); err != nil {
			got = err.Error()
		}
		if (got == "") != (refused == "") || !strings.Contains(got, refused) {
			return fmt.Errorf("the probe ControlPlane gets %q, want %q", got, refused)
		}
		return nil
	})
}

func reportedAPIServer(t *testing.T, cp *lodiav1alpha1.ControlPlane) string {
	t.Helper()
	if c := get(t, cp).Status.Components; c != nil && c.APIServer != nil {
		return c.APIServer.Version
	}
	return ""
}

func serviceAccountIssuer(command []string) string {
	for _, arg := range command {
		if value, found := strings.CutPrefix(arg, "--service-account-issuer="); found {
			return value
		}
	}
	return ""
}

func expectAReplicaLoads(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	container := deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0]
	minor, ok := versions.Minor(container.Image[strings.LastIndex(container.Image, ":")+1:])
	if !ok {
		t.Fatalf("the API server's image %s has no version", container.Image)
	}
	issuer := serviceAccountIssuer(container.Command)
	file := authenticationConfig(t, cp).Data["authentication-config.yaml"]
	egress := egressData(t, cp)
	if err := authnconfig.Validate([]byte(file), []string{issuer}, egress); err != nil {
		t.Errorf("with --service-account-issuer=%s Validate refuses the file Workloads keeps: %v\n%s", issuer, err, file)
	}
	if minor != envtestMinor(t) {
		return
	}
	if s, err := startTenantAPIServer(t, egress, []byte(file), issuer); err != nil {
		t.Errorf("kube-apiserver %s with --service-account-issuer=%s does not start on the file Workloads keeps: %v; %s", minor, issuer, err, s.failure())
		return
	}
	t.Logf("kube-apiserver %s with --service-account-issuer=%s starts on the file Workloads keeps", minor, issuer)
}

func overlapErrors(issuer string) string {
	return "kube-apiserver 1.37 refuses it: jwt[0].issuer.url: Invalid value: \"" + issuer + "\": URL must not overlap with disallowed issuers: [" + issuer + "]"
}

func conditionsWhile(t *testing.T, cp *lodiav1alpha1.ControlPlane, do func(), done func() error) []metav1.Condition {
	t.Helper()
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := k8s.List(t.Context(), list, client.InNamespace(cp.Namespace)); err != nil {
		t.Fatal(err)
	}
	events, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, &client.ListOptions{Namespace: cp.Namespace, Raw: &metav1.ListOptions{ResourceVersion: list.ResourceVersion}})
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	do()
	testenv.Eventually(t, done)
	current := get(t, cp).ResourceVersion
	var seen []metav1.Condition
	for last := ""; last != current; {
		select {
		case event, open := <-events.ResultChan():
			if !open {
				t.Fatal("the watch of the ControlPlanes closed")
			}
			fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane)
			if !ok {
				t.Fatalf("the watch of the ControlPlanes sent %s %+v", event.Type, event.Object)
			}
			if fresh.Name != cp.Name {
				continue
			}
			last = fresh.ResourceVersion
			if c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady); c != nil {
				seen = append(seen, *c)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("the watch did not reach resourceVersion %s", current)
		}
	}
	return seen
}

func expectSaid(t *testing.T, seen []metav1.Condition, refusal string) {
	t.Helper()
	if !slices.ContainsFunc(seen, func(c metav1.Condition) bool {
		return c.Status == metav1.ConditionFalse && strings.Contains(c.Message, refusal)
	}) {
		t.Errorf("no APIServerReady said %q; they were %+v", refusal, seen)
	}
}

func TestAServiceAccountIssuerMovedOntoAJWTIssuerRemovesTheJWTAuthenticators(t *testing.T) {
	const shared = "https://login.example.com/realms/team-a"
	cp := running(t, withJWT(oidc()))
	authenticationData(t, cp, "url: "+shared)
	hashed := hashes(t, cp)
	overlap := func(s *lodiav1alpha1.ControlPlaneSpec) {
		withJWT(oidc())(s)
		s.ServiceAccount = &lodiav1alpha1.ServiceAccount{Issuer: shared}
	}
	head := controlPlanesCRD(t)
	relaxed := head.DeepCopy()
	withoutRule(t, relaxed, "self.serviceAccount.issuer")
	t.Cleanup(func() {
		if err := updateCRD(context.Background(), head.DeepCopy()); err != nil {
			t.Error(err)
		}
	})
	serveCRD(t, relaxed, cp.Namespace, overlap, "")
	seen := conditionsWhile(t, cp, func() {
		update(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) {
			s.ServiceAccount = &lodiav1alpha1.ServiceAccount{Issuer: shared}
		})
	}, func() error {
		if issuer := serviceAccountIssuer(deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Command); issuer != shared {
			return errors.New("the API server's Deployment runs --service-account-issuer=" + issuer)
		}
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") {
			return errors.New("the authentication configuration is\n" + authn)
		}
		return expectAt(cp, get(t, cp).Generation, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, notWritten+overlapErrors(shared))
	})
	expectSaid(t, seen, notWritten+overlapErrors(shared)+"; the API server's JWT authenticators are removed, since the one stored is refused too: "+overlapErrors(shared))
	expectAReplicaLoads(t, cp)
	if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
		t.Errorf("the restart hashes are %v, then %v", hashed, after)
	}
	serveCRD(t, head.DeepCopy(), cp.Namespace, overlap, "kube-apiserver refuses a JWT issuer.url equal to serviceAccount.issuer")

	update(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) { s.ServiceAccount = nil })
	testenv.Eventually(t, func() error {
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "url: "+shared) {
			return errors.New("with the ServiceAccount issuer moved the authentication configuration is\n" + authn)
		}
		return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
	})
}

func TestALoadBalancerAddressMovedOntoAJWTIssuerRemovesTheJWTAuthenticators(t *testing.T) {
	const moved = "203.0.113.20"
	jwt := oidc()
	jwt[0].Issuer.URL = "https://" + moved
	cp := running(t, withJWT(jwt))
	authenticationData(t, cp, "url: https://"+moved)
	hashed := hashes(t, cp)

	seen := conditionsWhile(t, cp, func() { setIngress(t, cp, corev1.LoadBalancerIngress{IP: moved}) }, func() error {
		if issuer := serviceAccountIssuer(deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Command); issuer != "https://"+moved {
			return errors.New("the API server's Deployment runs --service-account-issuer=" + issuer)
		}
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") {
			return errors.New("the authentication configuration is\n" + authn)
		}
		return expectAt(cp, get(t, cp).Generation, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, notWritten+overlapErrors("https://"+moved))
	})
	expectSaid(t, seen, notWritten+overlapErrors("https://"+moved)+"; the API server's JWT authenticators are removed, since the one stored is refused too: "+overlapErrors("https://"+moved))
	expectAReplicaLoads(t, cp)
	if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
		t.Errorf("the restart hashes are %v, then %v", hashed, after)
	}
}

func TestAPredecessorsOrphanedFileIsReplacedForATenantWhoseAPIServerRefusesIt(t *testing.T) {
	const address = "203.0.113.20"
	jwt := oidc()
	jwt[0].Issuer.URL = "https://" + address
	predecessor := running(t, withJWT(jwt))
	written := string(authenticationData(t, predecessor, "url: https://"+address))

	ns := namespace(t)
	orphan := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-authentication-config", Namespace: ns, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev"}},
		Data:       map[string]string{"authentication-config.yaml": written},
	}
	if err := k8s.Create(t.Context(), orphan); err != nil {
		t.Fatal(err)
	}
	cp := create(t, ns, loadBalancer(), func(s *lodiav1alpha1.ControlPlaneSpec) {
		s.Version = "v1.35.0"
		withJWT(jwt)(s)
	})
	seen := conditionsWhile(t, cp, func() {
		setIngress(t, cp, corev1.LoadBalancerIngress{IP: address})
		setTargetAt(t, cp, "v1.35.0", revision)
		inputsReadyBesidesTheTarget(t, cp)
		for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
			markAvailable(t, eventuallyDeployment(t, cp, component))
		}
	}, func() error {
		if c := authenticationConfig(t, cp); !strings.Contains(c.Data["authentication-config.yaml"], "jwt: []") || !metav1.IsControlledBy(c, get(t, cp)) {
			return fmt.Errorf("the authentication configuration, controlled by %v, is\n%s", metav1.GetControllerOf(c), c.Data["authentication-config.yaml"])
		}
		return expectAt(cp, get(t, cp).Generation, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, notWritten+overlapErrors("https://"+address))
	})
	expectSaid(t, seen, notWritten+overlapErrors("https://"+address)+"; the API server's JWT authenticators are removed, since the one stored is not this ControlPlane's")
	expectAReplicaLoads(t, cp)
	hashed := hashes(t, cp)

	update(t, cp, withJWT(oidc("!('oidc:suspended' in user.groups)")))
	testenv.Eventually(t, func() error {
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "in user.groups)") {
			return errors.New("the fixed configuration is not written:\n" + authn)
		}
		return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
	})
	expectAReplicaLoads(t, cp)
	if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
		t.Errorf("the restart hashes are %v, then %v", hashed, after)
	}
}

func TestAStoredFileEditedByHandIntoOneTheMinorRefusesIsReplaced(t *testing.T) {
	for _, c := range []struct {
		name, from, to, refused string
	}{
		{"a claim rule that does not compile", "claims.tenant == 'team-a'", "claims.tenant ==", "jwt[0].claimValidationRules[0].expression: Invalid value: \"claims.tenant ==\""},
		{"an issuer reached through an egress Workloads does not configure", "egressSelectorType: cluster", "egressSelectorType: controlplane", "jwt[0].issuer.egressSelectorType: Invalid value: \"controlplane\": egress lookup for \"controlplane\" is not configured"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := running(t, withJWT(oidc()))
			authenticationData(t, cp, c.from)
			hashed := hashes(t, cp)
			update(t, cp, withJWT(oidc("user.nope == 'x'")))
			testenv.Eventually(t, func() error { return expectAuthenticationInvalid(cp, "jwt[0].userValidationRules[0].expression") })

			edited := authenticationConfig(t, cp)
			edited.Data["authentication-config.yaml"] = strings.Replace(edited.Data["authentication-config.yaml"], c.from, c.to, 1)
			seen := conditionsWhile(t, cp, func() {
				if err := k8s.Update(t.Context(), edited); err != nil {
					t.Fatal(err)
				}
			}, func() error {
				if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") {
					return errors.New("the authentication configuration is\n" + authn)
				}
				return expectAt(cp, get(t, cp).Generation, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, nopeRefusal)
			})
			expectSaid(t, seen, nopeRefusal+"; the API server's JWT authenticators are removed, since the one stored is refused too: kube-apiserver 1.37 refuses it: "+c.refused)
			expectAReplicaLoads(t, cp)
			if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
				t.Errorf("the restart hashes are %v, then %v", hashed, after)
			}
		})
	}
}

func TestATenantThatNeverHadAJWTAuthenticatorIsNotToldItsAreRemoved(t *testing.T) {
	cp := running(t)
	stored := authenticationConfig(t, cp)
	if authn := stored.Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") {
		t.Fatalf("without JWT authenticators Workloads writes\n%s", authn)
	}
	seen := conditionsWhile(t, cp, func() { update(t, cp, withJWT(oidc("user.nope == 'x'"))) }, func() error {
		return expectAt(cp, get(t, cp).Generation, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, nopeRefusal)
	})
	reconcileNow(t, cp)
	seen = append(seen, *condition(t, cp))
	for _, c := range seen {
		if strings.Contains(c.Message, "JWT authenticators are removed") {
			t.Errorf("APIServerReady said %q", c.Message)
		}
	}
	if after := authenticationConfig(t, cp); after.ResourceVersion != stored.ResourceVersion {
		t.Errorf("the ConfigMap was written:\n%s", after.Data["authentication-config.yaml"])
	}
}

func TestThisControlPlanesOwnValidFileIsKeptWithoutARoll(t *testing.T) {
	cp := running(t, withJWT(oidc()))
	valid := authenticationConfig(t, cp)
	hashed, generations := hashes(t, cp), generationsOf(t, cp)

	update(t, cp, withJWT(oidc("user.nope == 'x'")))
	testenv.Eventually(t, func() error {
		return expectAuthenticationInvalid(cp, "the authentication configuration of spec.apiServer.authentication is not written: kube-apiserver 1.37 refuses it: jwt[0].userValidationRules[0].expression")
	})
	if c := condition(t, cp); strings.Contains(c.Message, "removed") {
		t.Errorf("APIServerReady says %q", c.Message)
	}
	if kept := authenticationConfig(t, cp); kept.ResourceVersion != valid.ResourceVersion {
		t.Errorf("the ConfigMap was written:\n%s", kept.Data["authentication-config.yaml"])
	}
	expectAReplicaLoads(t, cp)
	if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
		t.Errorf("the restart hashes are %v, then %v", hashed, after)
	}
	if after := generationsOf(t, cp); fmt.Sprint(after) != fmt.Sprint(generations) {
		t.Errorf("a refused configuration rolled the Deployments: generations %v, then %v", generations, after)
	}
}

func TestNoHeldPassOrWaitCountsARefusedConfigurationAsAvailable(t *testing.T) {
	for name, trigger := range map[string]func(*testing.T, *lodiav1alpha1.ControlPlane){
		"a held pass": func(t *testing.T, cp *lodiav1alpha1.ControlPlane) { setTargetAt(t, cp, version, next) },
		"a held pass whose target the table does not hold": func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			setTargetAt(t, cp, "v1.34.1", next)
		},
		"a held pass that counts a report outside the table": func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			setTargetAt(t, cp, version, next)
			testenv.Eventually(t, func() error {
				d := deployment(t, cp, "kube-apiserver")
				d.Spec.Template.Spec.Containers[0].Image = "registry.k8s.io/kube-apiserver:v1.33.2"
				return k8s.Update(t.Context(), d)
			})
			markAvailable(t, deployment(t, cp, "kube-apiserver"))
			testenv.Eventually(t, func() error {
				if got := reportedAPIServer(t, cp); got != "v1.33.2" {
					return fmt.Errorf("the API server reports %q", got)
				}
				return nil
			})
		},
		"a wait on a Secret's data hash": func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			setSecret(t, cp, lodiav1alpha1.FrontProxyClientSecret, "")
		},
		"a wait on the datastore": func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionDatastoreReady, map[string]any{"conditions": []any{map[string]any{
				"type": lodiav1alpha1.ConditionDatastoreReady, "status": "False", "reason": "Unavailable", "message": "etcd has no quorum",
				"observedGeneration": get(t, cp).Generation, "lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
			}}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			cp := running(t)
			valid := authenticationConfig(t, cp).Data["authentication-config.yaml"]
			setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 0, 0 })
			testenv.Eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable) })
			update(t, cp, withJWT(oidc("user.nope == 'x'")))
			g := get(t, cp).Generation
			testenv.Eventually(t, func() error {
				return expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready; "+nopeRefusal)
			})

			trigger(t, cp)
			markAvailable(t, deployment(t, cp, "kube-apiserver"))
			invalid := func() error {
				if c := condition(t, cp); c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonAuthenticationInvalid || c.ObservedGeneration != g ||
					!strings.Contains(c.Message, "jwt[0].userValidationRules[0].expression: Invalid value: \"user.nope == 'x'\"") {
					return fmt.Errorf("APIServerReady is %+v, want False AuthenticationInvalid at generation %d", c, g)
				}
				return nil
			}
			testenv.Eventually(t, invalid)
			reconcileNow(t, cp)
			if err := invalid(); err != nil {
				t.Errorf("the pass after: %v", err)
			}
			if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; authn != valid {
				t.Errorf("the ConfigMap changed:\n%s", authn)
			}
		})
	}
}

func lifecyclePass(t *testing.T, cp *lodiav1alpha1.ControlPlane, running string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	generation := get(t, cp).Generation
	condition := func(conditionType, status, reason string) map[string]any {
		return map[string]any{
			"type": conditionType, "status": status, "reason": reason, "message": "set by the test",
			"observedGeneration": generation, "lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}
	}
	applyStatus(t, cp, lifecycle.FieldManager, map[string]any{"conditions": []any{condition(lodiav1alpha1.ConditionProgressing, "False", lodiav1alpha1.ReasonIdle)}})
	var others []any
	for _, conditionType := range []string{lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionReachable, lodiav1alpha1.ConditionControllersLeading} {
		others = append(others, condition(conditionType, "True", "Fine"))
	}
	applyStatus(t, cp, "test/others", map[string]any{"conditions": others})
	r := &lifecycle.Reconciler{Client: k8s, Reader: k8s, Clock: clock.RealClock{}, Revision: running, MaxConcurrentRollouts: 1000, Settling: time.Minute}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	return get(t, cp)
}

func expectLifecycle(t *testing.T, cp *lodiav1alpha1.ControlPlane, generation int64, status metav1.ConditionStatus, reason, message, revision string) {
	t.Helper()
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Status != status || c.Reason != reason || c.Message != message || c.ObservedGeneration != generation {
		t.Errorf("Ready is %+v, want %s %s %q at generation %d", c, status, reason, message, generation)
	}
	if got := cp.Status.Target.OperatorRevision; got != revision {
		t.Errorf("status.target.operatorRevision is %s after Lifecycle's pass, want %s; Progressing is %+v", got, revision, meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionProgressing))
	}
}

func TestARefusalBehindAFalseCountIsNotCountedReadyAtALaterGeneration(t *testing.T) {
	const unavailable = "kube-apiserver has 0 of 1 replicas ready"
	refusedWhileUnavailable := func(t *testing.T, cp *lodiav1alpha1.ControlPlane) int64 {
		setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 0, 0 })
		testenv.Eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable) })
		update(t, cp, withJWT(oidc("user.nope == 'x'")))
		g := get(t, cp).Generation
		testenv.Eventually(t, func() error {
			return expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, unavailable+"; "+nopeRefusal)
		})
		return g
	}
	for name, c := range map[string]struct {
		refuse  func(*testing.T, *lodiav1alpha1.ControlPlane) int64
		later   func(*testing.T, *lodiav1alpha1.ControlPlane)
		running string
	}{
		"AuthenticationInvalid, then Unavailable in a wait at the next generation": {
			refuse: func(t *testing.T, cp *lodiav1alpha1.ControlPlane) int64 {
				update(t, cp, withJWT(oidc("user.nope == 'x'")))
				g := get(t, cp).Generation
				testenv.Eventually(t, func() error {
					return expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, nopeRefusal)
				})
				return g
			},
			later: func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
				changeSpec(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) { s.APIServer.ExtraArgs = map[string]string{"event-ttl": "2h"} })
				setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 0, 0 })
			},
			running: next,
		},
		"refused while Unavailable, then a wait at the next generation": {
			refuse: refusedWhileUnavailable,
			later: func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
				changeSpec(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) { s.APIServer.ExtraArgs = map[string]string{"event-ttl": "2h"} })
			},
			running: next,
		},
		"refused while Unavailable, then a held pass at the next generation": {
			refuse: refusedWhileUnavailable,
			later: func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
				setTargetAt(t, cp, version, next)
				update(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) { s.APIServer.ExtraArgs = map[string]string{"event-ttl": "2h"} })
			},
			running: revision,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cp := running(t, withJWT(oidc()))
			valid := authenticationConfig(t, cp)
			g := c.refuse(t, cp)
			c.later(t, cp)
			testenv.Eventually(t, func() error {
				return expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, unavailable+"; "+nopeRefusal)
			})
			granted := get(t, cp).Status.Target.OperatorRevision

			markAvailable(t, deployment(t, cp, "kube-apiserver"))
			testenv.Eventually(t, func() error {
				return expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, nopeRefusal)
			})
			reconcileNow(t, cp)
			if err := expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, nopeRefusal); err != nil {
				t.Errorf("the pass after, at generation %d: %v", get(t, cp).Generation, err)
			}
			if kept := authenticationConfig(t, cp); kept.ResourceVersion != valid.ResourceVersion {
				t.Errorf("the ConfigMap was written:\n%s", kept.Data["authentication-config.yaml"])
			}
			expectLifecycle(t, lifecyclePass(t, cp, c.running), g, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, "APIServerReady: "+nopeRefusal, granted)
		})
	}
}

func TestAHeldPassOrAWaitCountsTheServedFileOnceTheIssuerMoves(t *testing.T) {
	const moved = "203.0.113.20"
	for name, c := range map[string]struct {
		stop    func(*testing.T, *lodiav1alpha1.ControlPlane)
		running string
	}{
		"a held pass": {func(t *testing.T, cp *lodiav1alpha1.ControlPlane) { setTargetAt(t, cp, version, next) }, revision},
		"a wait on a Secret's data hash": {
			func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
				setSecret(t, cp, lodiav1alpha1.FrontProxyClientSecret, "")
			}, next,
		},
	} {
		t.Run(name, func(t *testing.T) {
			jwt := oidc()
			jwt[0].Issuer.URL = "https://" + moved
			cp := running(t, withJWT(jwt))
			ready := *condition(t, cp)
			stored := authenticationConfig(t, cp)
			c.stop(t, cp)
			reconcileNow(t, cp)

			setIngress(t, cp, corev1.LoadBalancerIngress{IP: moved})
			testenv.Eventually(t, func() error {
				if e := get(t, cp).Status.Endpoint; e == nil || e.APIServer.Host != moved {
					return fmt.Errorf("status.endpoint is %+v", e)
				}
				return nil
			})
			reconcileNow(t, cp)
			if err := expectAt(cp, ready.ObservedGeneration, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, ready.Message); err != nil {
				t.Error(err)
			}
			if after := authenticationConfig(t, cp); after.ResourceVersion != stored.ResourceVersion {
				t.Errorf("the ConfigMap was written:\n%s", after.Data["authentication-config.yaml"])
			}
			expectAReplicaLoads(t, cp)
			expectLifecycle(t, lifecyclePass(t, cp, c.running), ready.ObservedGeneration, metav1.ConditionTrue, lodiav1alpha1.ReasonReady,
				"CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True", c.running)
			if c.running != revision {
				return
			}

			testenv.Eventually(t, func() error {
				if issuer := serviceAccountIssuer(deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Command); issuer != "https://"+moved {
					return errors.New("the API server's Deployment runs --service-account-issuer=" + issuer)
				}
				if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") {
					return errors.New("the authentication configuration is\n" + authn)
				}
				return expectAuthenticationInvalid(cp, "URL must not overlap with disallowed issuers: [https://"+moved+"]")
			})
			expectAReplicaLoads(t, cp)
		})
	}
}

func TestAWaitAtALaterGenerationCountsUnderTheRefusalOfTheGenerationItNames(t *testing.T) {
	cp := running(t)
	update(t, cp, withJWT(oidc("user.nope == 'x'")))
	g := get(t, cp).Generation
	testenv.Eventually(t, func() error { return expectAuthenticationInvalid(cp, "Invalid value: \"user.nope == 'x'\"") })
	refused := *condition(t, cp)

	changeSpec(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) { s.APIServer.ExtraArgs = map[string]string{"event-ttl": "2h"} })
	reconcileNow(t, cp)
	if err := expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, refused.Message); err != nil {
		t.Errorf("a wait at generation %d: %v", g+1, err)
	}
	setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 0, 0 })
	reconcileNow(t, cp)
	if err := expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready; "+refused.Message); err != nil {
		t.Errorf("a wait at generation %d once the API server has no ready replica: %v", g+1, err)
	}
}

func TestAWaitAtALaterGenerationThatFixesTheFieldKeepsTheRefusal(t *testing.T) {
	cp := running(t, withJWT(oidc()))
	update(t, cp, withJWT(oidc("user.nope == 'x'")))
	g := get(t, cp).Generation
	testenv.Eventually(t, func() error {
		return expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, nopeRefusal)
	})

	changeSpec(t, cp, withJWT(oidc("user.username != 'x'")))
	reconcileNow(t, cp)
	if err := expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, nopeRefusal); err != nil {
		t.Errorf("a wait at generation %d, whose spec fixes the field: %v", g+1, err)
	}
}

func TestAWaitAtALaterRefusedGenerationCountsTheValidOneItNamesAsItWas(t *testing.T) {
	cp := running(t, withJWT(oidc()))
	ready := *condition(t, cp)

	changeSpec(t, cp, withJWT(oidc("user.nope == 'x'")))
	reconcileNow(t, cp)
	if err := expectAt(cp, ready.ObservedGeneration, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, ready.Message); err != nil {
		t.Errorf("a wait at generation %d, whose spec is refused, over generation %d, whose file is written: %v", get(t, cp).Generation, ready.ObservedGeneration, err)
	}
}
