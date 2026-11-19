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
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/authnconfig"
	"go.lodia.io/lodia/internal/testenv"
	"go.lodia.io/lodia/internal/versions"
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

func TestATenantOfAnOlderMinorGetsNoExpressionOfANewerOne(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), func(s *lodiav1alpha1.ControlPlaneSpec) {
		s.Version = "v1.35.0"
		withJWT(oidc("!user.groups.includes('oidc:suspended')"))(s)
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	setTargetAt(t, cp, "v1.35.0", revision)
	inputsReadyBesidesTheTarget(t, cp)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	testenv.Eventually(t, func() error {
		return expectAuthenticationInvalid(cp, "kube-apiserver 1.35 refuses it: jwt[0].userValidationRules[0].expression: Invalid value: \"!user.groups.includes('oidc:suspended')\": compilation failed: ERROR: <input>:1:22: undeclared reference to 'includes'")
	})
	if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") || strings.Contains(authn, "includes") {
		t.Errorf("a 1.35 tenant's API server gets\n%s", authn)
	}
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

func TestAControlPlaneStoredWithTheEgressGateOffGetsNoJWTFile(t *testing.T) {
	head := controlPlanesCRD(t)
	const refused = "kube-apiserver refuses the egressSelectorType Lodia renders for every JWT authenticator without StructuredAuthenticationConfigurationEgressSelector"
	gateOff := func(s *lodiav1alpha1.ControlPlaneSpec) {
		s.APIServer = &lodiav1alpha1.APIServer{FeatureGates: map[string]bool{"StructuredAuthenticationConfigurationEgressSelector": false}}
	}
	relaxed := head.DeepCopy()
	withoutRule(t, relaxed, "StructuredAuthenticationConfigurationEgressSelector", "properties", "apiServer", "properties", "featureGates")
	ns := namespace(t)
	t.Cleanup(func() {
		if err := updateCRD(context.Background(), head.DeepCopy()); err != nil {
			t.Error(err)
		}
	})
	serveCRD(t, relaxed, ns, gateOff, "")
	cp := create(t, ns, loadBalancer(), func(s *lodiav1alpha1.ControlPlaneSpec) {
		withJWT(oidc())(s)
		s.APIServer.FeatureGates = map[string]bool{"StructuredAuthenticationConfigurationEgressSelector": false}
	})
	serveCRD(t, head.DeepCopy(), ns, gateOff, refused)

	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	testenv.Eventually(t, func() error {
		return expectAuthenticationInvalid(cp, "kube-apiserver 1.37 refuses it: jwt[0].issuer.egressSelectorType: Invalid value: \"cluster\": egress selector is not supported when StructuredAuthenticationConfigurationEgressSelector feature gate is disabled")
	})
	if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") || strings.Contains(authn, "login.example.com") {
		t.Errorf("a ControlPlane whose API server runs without the egress selector gets\n%s", authn)
	}

	update(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) { s.APIServer.FeatureGates = nil })
	testenv.Eventually(t, func() error {
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "url: https://login.example.com/realms/team-a") {
			return errors.New("with the gate back the authentication configuration is\n" + authn)
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

func TestAMinorStepGetsAnExpressionOfTheNewerMinorOnceTheAPIServerReportsIt(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), func(s *lodiav1alpha1.ControlPlaneSpec) {
		s.Version = "v1.36.6"
		withJWT(oidc())(s)
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	setTargetAt(t, cp, "v1.36.6", revision)
	inputsReadyBesidesTheTarget(t, cp)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	testenv.Eventually(t, func() error { return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady) })
	if got := reportedAPIServer(t, cp); got != "v1.36.6" {
		t.Fatalf("the API server reports %q, want v1.36.6", got)
	}

	update(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) {
		s.Version = "v1.37.1"
		withJWT(oidc("!user.groups.includes('oidc:suspended')"))(s)
	})
	setTargetAt(t, cp, "v1.37.1", revision)
	testenv.Eventually(t, func() error {
		if image := deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Image; !strings.HasSuffix(image, ":v1.37.1") {
			return fmt.Errorf("the API server's image is %s", image)
		}
		return expectAuthenticationInvalid(cp, "kube-apiserver 1.36 refuses it: jwt[0].userValidationRules[0].expression: Invalid value: \"!user.groups.includes('oidc:suspended')\"")
	})
	if got := reportedAPIServer(t, cp); got != "v1.36.6" {
		t.Errorf("mid-step the API server reports %q, want v1.36.6", got)
	}
	if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; strings.Contains(authn, "includes") {
		t.Errorf("mid-step the 1.36 API server gets\n%s", authn)
	}
	expectAReplicaLoads(t, cp)

	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	testenv.Eventually(t, func() error {
		if got := reportedAPIServer(t, cp); got != "v1.37.1" {
			return fmt.Errorf("the API server reports %q", got)
		}
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "includes") {
			return errors.New("once the API server reports 1.37 the authentication configuration is\n" + authn)
		}
		return nil
	})
}

func flagValue(command []string, name string) string {
	for _, arg := range command {
		if value, found := strings.CutPrefix(arg, "--"+name+"="); found {
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
	issuer := flagValue(container.Command, "service-account-issuer")
	gates := map[string]bool{}
	if pairs := flagValue(container.Command, "feature-gates"); pairs != "" {
		for pair := range strings.SplitSeq(pairs, ",") {
			name, value, _ := strings.Cut(pair, "=")
			gates[name] = value == "true"
		}
	}
	file := authenticationConfig(t, cp).Data["authentication-config.yaml"]
	if err := authnconfig.Validate([]byte(file), []string{minor}, []string{issuer}, gates); err != nil {
		t.Errorf("a new replica of kube-apiserver %s with --service-account-issuer=%s refuses the file Workloads keeps: %v\n%s", minor, issuer, err, file)
	}
	if minor != envtestMinor(t) || len(gates) > 0 {
		return
	}
	if s, err := startTenantAPIServer(t, []byte(file), issuer); err != nil {
		t.Errorf("kube-apiserver %s with --service-account-issuer=%s does not start on the file Workloads keeps: %v; %s", minor, issuer, err, s.failure())
		return
	}
	t.Logf("kube-apiserver %s with --service-account-issuer=%s starts on the file Workloads keeps", minor, issuer)
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
	update(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) {
		s.ServiceAccount = &lodiav1alpha1.ServiceAccount{Issuer: shared}
	})
	serveCRD(t, head.DeepCopy(), cp.Namespace, overlap, "kube-apiserver refuses a JWT issuer.url equal to serviceAccount.issuer")

	testenv.Eventually(t, func() error {
		if issuer := flagValue(deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Command, "service-account-issuer"); issuer != shared {
			return errors.New("the API server's Deployment runs --service-account-issuer=" + issuer)
		}
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") {
			return errors.New("the authentication configuration is\n" + authn)
		}
		return expectAuthenticationInvalid(cp, "kube-apiserver 1.37 refuses it: jwt[0].issuer.url: Invalid value: \""+shared+"\": URL must not overlap with disallowed issuers: ["+shared+"]; the API server's JWT authenticators are removed")
	})
	expectAReplicaLoads(t, cp)
	if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
		t.Errorf("the restart hashes are %v, then %v", hashed, after)
	}

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

	setIngress(t, cp, corev1.LoadBalancerIngress{IP: moved})
	testenv.Eventually(t, func() error {
		if issuer := flagValue(deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Command, "service-account-issuer"); issuer != "https://"+moved {
			return errors.New("the API server's Deployment runs --service-account-issuer=" + issuer)
		}
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") {
			return errors.New("the authentication configuration is\n" + authn)
		}
		return expectAuthenticationInvalid(cp, "URL must not overlap with disallowed issuers: [https://"+moved+"]; the API server's JWT authenticators are removed")
	})
	expectAReplicaLoads(t, cp)
	if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
		t.Errorf("the restart hashes are %v, then %v", hashed, after)
	}
}

func TestAPredecessorsOrphanedFileIsReplacedForATenantWhoseAPIServerRefusesIt(t *testing.T) {
	const includes = "!user.groups.includes('oidc:suspended')"
	predecessor := running(t, withJWT(oidc(includes)))
	written := string(authenticationData(t, predecessor, "includes"))

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
		withJWT(oidc(includes))(s)
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	setTargetAt(t, cp, "v1.35.0", revision)
	inputsReadyBesidesTheTarget(t, cp)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	testenv.Eventually(t, func() error {
		if c := authenticationConfig(t, cp); !strings.Contains(c.Data["authentication-config.yaml"], "jwt: []") || !metav1.IsControlledBy(c, get(t, cp)) {
			return fmt.Errorf("the authentication configuration, controlled by %v, is\n%s", metav1.GetControllerOf(c), c.Data["authentication-config.yaml"])
		}
		if err := expectAuthenticationInvalid(cp, "kube-apiserver 1.35 refuses it: jwt[0].userValidationRules[0].expression: Invalid value: \""+includes+"\": compilation failed: ERROR: <input>:1:22: undeclared reference to 'includes'"); err != nil {
			return err
		}
		return expectAuthenticationInvalid(cp, "; the API server's JWT authenticators are removed")
	})
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
	cp := running(t, withJWT(oidc()))
	authenticationData(t, cp, "claims.tenant == 'team-a'")
	hashed := hashes(t, cp)
	update(t, cp, withJWT(oidc("user.nope == 'x'")))
	testenv.Eventually(t, func() error { return expectAuthenticationInvalid(cp, "jwt[0].userValidationRules[0].expression") })

	edited := authenticationConfig(t, cp)
	edited.Data["authentication-config.yaml"] = strings.Replace(edited.Data["authentication-config.yaml"], "claims.tenant == 'team-a'", "claims.tenant ==", 1)
	if err := k8s.Update(t.Context(), edited); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		if authn := authenticationConfig(t, cp).Data["authentication-config.yaml"]; !strings.Contains(authn, "jwt: []") {
			return errors.New("the authentication configuration is\n" + authn)
		}
		return expectAuthenticationInvalid(cp, "undefined field 'nope'\n | user.nope == 'x'\n | ....^; the API server's JWT authenticators are removed")
	})
	expectAReplicaLoads(t, cp)
	if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
		t.Errorf("the restart hashes are %v, then %v", hashed, after)
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
				return expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready")
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
	if err := expectAt(cp, g, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready"); err != nil {
		t.Errorf("a wait at generation %d once the API server has no ready replica: %v", g+1, err)
	}
}
