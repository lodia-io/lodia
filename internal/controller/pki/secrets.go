// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/certs"
)

// The CAs, by the suffix of their <cp>-cakey-<ca> Secret.
const (
	clusterCA    = "cluster"
	clientCA     = "client"
	frontProxyCA = "front-proxy"
	etcdCA       = "etcd"
)

const (
	// signingDuration is how long the controller manager's kubelet client
	// certificates live, and so how long a replaced kubelet signer stays in
	// the client CA bundle.
	signingDuration = 30 * 24 * time.Hour
	// serviceAccountKeyBits is RSA 2048: OIDC makes RS256 mandatory for every
	// provider, so it is what a federation partner accepts.
	serviceAccountKeyBits = 2048
	encryptionKeyBytes    = 32
	caKey                 = "ca.crt"
	kubeconfigKey         = "kubeconfig"
	signingKey            = "signing.key"
	encryptionConfigKey   = "encryption-config.yaml"
)

var (
	serverAuth       = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	clientAuth       = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	serverClientAuth = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	etcdRoles        = []string{"ca", "server", "client", "peer", "backup-restore"}
)

// tenant is one ControlPlane's PKI during a pass: what exists, what was
// decided, and when the earliest renewal is due.
type tenant struct {
	cp       *bryggav1alpha1.ControlPlane
	existing map[string]*corev1.Secret
	now      time.Time
	renewal  time.Time
	cas      map[string]*certs.Certificate
	endpoint *endpoint
}

type endpoint struct {
	host string
	port int32
}

func (e *endpoint) url() string {
	return "https://" + net.JoinHostPort(e.host, strconv.Itoa(int(e.port)))
}

func newTenant(cp *bryggav1alpha1.ControlPlane, secrets []corev1.Secret) *tenant {
	t := &tenant{cp: cp, existing: make(map[string]*corev1.Secret, len(secrets)), now: time.Now(), cas: map[string]*certs.Certificate{}}
	for i := range secrets {
		t.existing[secrets[i].Name] = &secrets[i]
	}
	if e := cp.Status.Endpoint; e != nil && e.Host != "" {
		t.endpoint = &endpoint{host: e.Host, port: e.Port}
		if e.Port == 0 {
			t.endpoint.port = 443
		}
	} else if gw := cp.Spec.Endpoint.Gateway; gw != nil {
		t.endpoint = &endpoint{host: string(gw.Hostnames.APIServer), port: 443}
	}
	return t
}

func (t *tenant) name(suffix string) string {
	return t.cp.Name + "-" + suffix
}

func (t *tenant) schedule(cert *x509.Certificate) {
	if due := certs.RenewalTime(cert); t.renewal.IsZero() || due.Before(t.renewal) {
		t.renewal = due
	}
}

func (t *tenant) requeueAfter() time.Duration {
	after := bindingsPeriod
	if !t.renewal.IsZero() {
		after = min(after, t.renewal.Sub(t.now))
	}
	return max(after, time.Second)
}

func data(s *corev1.Secret, key string) []byte {
	if s == nil {
		return nil
	}
	return s.Data[key]
}

// leaf keeps the certificate in certPEM and keyPEM when it is p's, chains to
// ca and is not due for renewal; otherwise it issues one.
func (t *tenant) leaf(certPEM, keyPEM []byte, ca *certs.Certificate, p certs.Profile, lifetime time.Duration) (*certs.Certificate, error) {
	c, err := certs.Parse(certPEM, keyPEM)
	if err != nil || certs.Verify(c.Cert, ca.Cert) != nil || !certs.Matches(c.Cert, p) || !t.now.Before(certs.RenewalTime(c.Cert)) {
		if c, err = ca.Issue(p, lifetime); err != nil {
			return nil, err
		}
	}
	t.schedule(c.Cert)
	return c, nil
}

// issue brings every Secret of the tenant namespace contract up to date.
func (r *Reconciler) issue(ctx context.Context, t *tenant) error {
	for _, name := range []string{clusterCA, clientCA, frontProxyCA, etcdCA} {
		if err := r.ca(ctx, t, name); err != nil {
			return err
		}
	}
	cluster, cl, frontProxy, etcd := t.cas[clusterCA], t.cas[clientCA], t.cas[frontProxyCA], t.cas[etcdCA]

	signer, err := r.tls(ctx, t, t.name("kubelet-signer"), cl, certs.Profile{CommonName: t.name("kubelet-signer"), CA: true}, nil)
	if err != nil {
		return err
	}
	for name, bundle := range map[string][]*x509.Certificate{
		t.name("ca-bundle"):        {cluster.Cert},
		t.name("client-ca-bundle"): t.clientBundle(signer),
		t.name("front-proxy-ca"):   {frontProxy.Cert},
		t.name("etcd-ca"):          {etcd.Cert},
	} {
		if err := r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{caKey: certs.CertPEM(bundle...)}, nil); err != nil {
			return err
		}
	}

	for _, l := range []struct {
		suffix  string
		ca      *certs.Certificate
		profile certs.Profile
	}{
		{"apiserver-kubelet-client", cluster, certs.Profile{CommonName: "kube-apiserver-kubelet-client", Organizations: []string{"brygga:kubelet-api-admins"}, Usages: clientAuth}},
		{"front-proxy-client", frontProxy, certs.Profile{CommonName: "front-proxy-client", Usages: clientAuth}},
		{"apiserver-etcd-client", etcd, certs.Profile{CommonName: "kube-apiserver-etcd-client", Usages: clientAuth}},
	} {
		if _, err := r.tls(ctx, t, t.name(l.suffix), l.ca, l.profile, nil); err != nil {
			return err
		}
	}
	if t.endpoint != nil {
		if _, err := r.tls(ctx, t, t.name("apiserver-serving"), cluster, t.servingProfile(), nil); err != nil {
			return err
		}
		if _, err := r.tls(ctx, t, t.name("konnectivity-server"), cluster, t.konnectivityProfile(), nil); err != nil {
			return err
		}
	}

	internal := "https://" + t.name("kube-apiserver") + "." + t.cp.Namespace + ".svc:443"
	for _, k := range []kubeconfig{
		{"konnectivity-kubeconfig", "system:konnectivity-server", nil, "https://127.0.0.1:6443", false},
		{"controller-manager-kubeconfig", "system:kube-controller-manager", nil, internal, false},
		{"scheduler-kubeconfig", "system:kube-scheduler", nil, internal, false},
		{"brygga-kubeconfig", "brygga:operator", nil, internal, true},
	} {
		if err := r.kubeconfig(ctx, t, k); err != nil {
			return err
		}
	}
	if t.endpoint != nil {
		if err := r.kubeconfig(ctx, t, kubeconfig{"admin-kubeconfig", "kubernetes-admin", []string{"brygga:cluster-admins"}, t.endpoint.url(), true}); err != nil {
			return err
		}
	}

	if err := r.serviceAccountKey(ctx, t); err != nil {
		return err
	}
	if err := r.encryptionConfig(ctx, t); err != nil {
		return err
	}
	return r.etcd(ctx, t)
}

// ca loads the CA's key pair from <cp>-cakey-<name>, or creates it.
func (r *Reconciler) ca(ctx context.Context, t *tenant, name string) error {
	secret := t.name("cakey-" + name)
	if s := t.existing[secret]; s != nil {
		if c, err := certs.Parse(s.Data[corev1.TLSCertKey], s.Data[corev1.TLSPrivateKeyKey]); err == nil && c.Cert.IsCA {
			t.cas[name] = c
			return nil
		}
	}
	lifetime := r.CALifetime
	if name == clusterCA {
		lifetime = r.TrustAnchorLifetime
	}
	c, err := certs.NewCA(t.name(name+"-ca"), lifetime)
	if err != nil {
		return err
	}
	t.cas[name] = c
	return r.applyTLS(ctx, t, secret, c, nil)
}

// tls keeps or issues the certificate of a kubernetes.io/tls Secret.
func (r *Reconciler) tls(ctx context.Context, t *tenant, name string, ca *certs.Certificate, p certs.Profile, labels map[string]string) (*certs.Certificate, error) {
	s := t.existing[name]
	c, err := t.leaf(data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey), ca, p, r.LeafLifetime)
	if err != nil {
		return nil, err
	}
	return c, r.applyTLS(ctx, t, name, c, labels)
}

func (r *Reconciler) applyTLS(ctx context.Context, t *tenant, name string, c *certs.Certificate, labels map[string]string) error {
	key, err := certs.KeyPEM(c.Key)
	if err != nil {
		return err
	}
	return r.applySecret(ctx, t, name, corev1.SecretTypeTLS, map[string][]byte{corev1.TLSCertKey: certs.CertPEM(c.Cert), corev1.TLSPrivateKeyKey: key}, labels)
}

// clientBundle is the client CA, the kubelet signer and every previous signer
// for one signing duration after its replacement, since kubelets present only
// their leaf.
func (t *tenant) clientBundle(signer *certs.Certificate) []*x509.Certificate {
	ca := t.cas[clientCA].Cert
	bundle := []*x509.Certificate{ca, signer.Cert}
	previous, _ := certs.ParseCerts(data(t.existing[t.name("client-ca-bundle")], caKey))
	replaced := signer.Cert.NotBefore
	for _, c := range previous {
		if c.IsCA && !c.Equal(ca) && !c.Equal(signer.Cert) && certs.Verify(c, ca) == nil && t.now.Before(replaced.Add(signingDuration)) {
			bundle = append(bundle, c)
		}
	}
	return bundle
}

// servingProfile is the API server's serving certificate: every name and
// address a client may dial.
func (t *tenant) servingProfile() certs.Profile {
	cp := t.cp
	domain := string(cp.Spec.Network.ClusterDomain)
	if domain == "" {
		domain = "cluster.local"
	}
	api := t.name("kube-apiserver")
	p := certs.Profile{CommonName: "kube-apiserver", Usages: serverAuth}
	for _, name := range []string{"kubernetes", "kubernetes.default", "kubernetes.default.svc", "kubernetes.default.svc." + domain} {
		p.SAN(name)
	}
	for _, cidr := range cp.Spec.Network.ServiceCIDRs {
		if prefix, err := netip.ParsePrefix(string(cidr)); err == nil {
			p.SAN(prefix.Masked().Addr().Next().String())
		}
	}
	for _, name := range []string{api, api + "." + cp.Namespace, api + "." + cp.Namespace + ".svc", "localhost", "127.0.0.1", t.endpoint.host} {
		p.SAN(name)
	}
	if gw := cp.Spec.Endpoint.Gateway; gw != nil {
		p.SAN(string(gw.Hostnames.APIServer))
	}
	for _, san := range cp.Spec.Endpoint.CertSANs {
		p.SAN(string(san))
	}
	return p
}

func (t *tenant) konnectivityProfile() certs.Profile {
	p := certs.Profile{CommonName: "konnectivity-server", Usages: serverAuth}
	for _, name := range []string{"localhost", "127.0.0.1", t.endpoint.host} {
		p.SAN(name)
	}
	if gw := t.cp.Spec.Endpoint.Gateway; gw != nil {
		p.SAN(string(gw.Hostnames.Konnectivity))
	}
	return p
}

// kubeconfig is a client identity under the client CA and the server it dials.
type kubeconfig struct {
	suffix string
	user   string
	groups []string
	server string
	// embed puts the certificates inline, for a kubeconfig that leaves the
	// pod world; otherwise they are files beside it, which client-go reloads.
	embed bool
}

func (r *Reconciler) kubeconfig(ctx context.Context, t *tenant, k kubeconfig) error {
	name := t.name(k.suffix)
	s := t.existing[name]
	certPEM, keyPEM := data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey)
	if k.embed {
		certPEM, keyPEM, _ = certs.Embedded(data(s, kubeconfigKey))
	}
	c, err := t.leaf(certPEM, keyPEM, t.cas[clientCA], certs.Profile{CommonName: k.user, Organizations: k.groups, Usages: clientAuth}, r.LeafLifetime)
	if err != nil {
		return err
	}
	key, err := certs.KeyPEM(c.Key)
	if err != nil {
		return err
	}
	ca, cert := certs.CertPEM(t.cas[clusterCA].Cert), certs.CertPEM(c.Cert)
	rendered, err := certs.Kubeconfig(t.cp.Name, k.server, k.user, ca, cert, key, k.embed)
	if err != nil {
		return err
	}
	content := map[string][]byte{kubeconfigKey: rendered}
	if !k.embed {
		content[caKey], content[corev1.TLSCertKey], content[corev1.TLSPrivateKeyKey] = ca, cert, key
	}
	return r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, content, nil)
}

// serviceAccountKey keeps the signing key of <cp>-sa-keys, brygga-signer's
// file contract, or creates it.
func (r *Reconciler) serviceAccountKey(ctx context.Context, t *tenant) error {
	name := t.name("sa-keys")
	content := map[string][]byte{}
	if s := t.existing[name]; s != nil {
		maps.Copy(content, s.Data)
	}
	if key, err := certs.ParseKey(content[signingKey]); err != nil || !isServiceAccountKey(key) {
		key, err := rsa.GenerateKey(rand.Reader, serviceAccountKeyBits)
		if err != nil {
			return err
		}
		if content[signingKey], err = certs.KeyPEM(key); err != nil {
			return err
		}
	}
	return r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, content, nil)
}

func isServiceAccountKey(key any) bool {
	rsaKey, ok := key.(*rsa.PrivateKey)
	return ok && rsaKey.N.BitLen() >= serviceAccountKeyBits
}

// encryptionConfig keeps <cp>-encryption-config while its first provider is
// the one in spec; a changed provider goes first with a new key and the
// previous providers stay readable.
func (r *Reconciler) encryptionConfig(ctx context.Context, t *tenant) error {
	name := t.name("encryption-config")
	existing := data(t.existing[name], encryptionConfigKey)
	want := bryggav1alpha1.EncryptionProvider("secretbox")
	if api := t.cp.Spec.APIServer; api != nil && api.Encryption != nil && api.Encryption.Provider != "" {
		want = api.Encryption.Provider
	}
	var cfg apiserverv1.EncryptionConfiguration
	var providers []apiserverv1.ProviderConfiguration
	if err := yaml.Unmarshal(existing, &cfg); err == nil && len(cfg.Resources) == 1 {
		providers = cfg.Resources[0].Providers
	}
	if len(providers) > 0 && providerOf(providers[0]) == want {
		return r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{encryptionConfigKey: existing}, nil)
	}
	first, err := newProvider(want)
	if err != nil {
		return err
	}
	cfg = apiserverv1.EncryptionConfiguration{
		TypeMeta:  metav1.TypeMeta{APIVersion: apiserverv1.SchemeGroupVersion.String(), Kind: "EncryptionConfiguration"},
		Resources: []apiserverv1.ResourceConfiguration{{Resources: []string{"secrets"}, Providers: append([]apiserverv1.ProviderConfiguration{first}, providers...)}},
	}
	rendered, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{encryptionConfigKey: rendered}, nil)
}

func providerOf(p apiserverv1.ProviderConfiguration) bryggav1alpha1.EncryptionProvider {
	switch {
	case p.Secretbox != nil:
		return "secretbox"
	case p.AESCBC != nil:
		return "aescbc"
	case p.Identity != nil:
		return "None"
	default:
		return ""
	}
}

func newProvider(provider bryggav1alpha1.EncryptionProvider) (apiserverv1.ProviderConfiguration, error) {
	if provider == "None" {
		return apiserverv1.ProviderConfiguration{Identity: &apiserverv1.IdentityConfiguration{}}, nil
	}
	secret := make([]byte, encryptionKeyBytes)
	if _, err := rand.Read(secret); err != nil {
		return apiserverv1.ProviderConfiguration{}, err
	}
	keys := []apiserverv1.Key{{Name: "key1", Secret: base64.StdEncoding.EncodeToString(secret)}}
	switch provider {
	case "secretbox":
		return apiserverv1.ProviderConfiguration{Secretbox: &apiserverv1.SecretboxConfiguration{Keys: keys}}, nil
	case "aescbc":
		return apiserverv1.ProviderConfiguration{AESCBC: &apiserverv1.AESConfiguration{Keys: keys}}, nil
	default:
		return apiserverv1.ProviderConfiguration{}, fmt.Errorf("encryption provider %q is not supported", provider)
	}
}

// etcd keeps the newest generation of the five etcd Secrets complete and
// valid, issues the next one when a leaf is due (etcd-druid rolls the members
// only for a new Secret name), keeps the previous generation and deletes the
// older ones.
func (r *Reconciler) etcd(ctx context.Context, t *tenant) error {
	ca := t.cas[etcdCA]
	profiles := t.etcdProfiles()
	var generations []int
	for _, s := range t.existing {
		if g, err := strconv.Atoi(s.Labels[bryggav1alpha1.EtcdCertificatesLabel]); err == nil && !slices.Contains(generations, g) {
			generations = append(generations, g)
		}
	}
	slices.Sort(generations)
	current := 0
	if len(generations) > 0 {
		current = generations[len(generations)-1]
	}
	due := current == 0
	for _, p := range profiles {
		s := t.existing[t.etcdName(p.role, current)]
		c, err := certs.Parse(data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey))
		if err != nil {
			continue
		}
		if certs.Verify(c.Cert, ca.Cert) != nil || !certs.Matches(c.Cert, p.profile) || !t.now.Before(certs.RenewalTime(c.Cert)) {
			due = true
		}
	}
	if due {
		current++
	}
	labels := map[string]string{bryggav1alpha1.EtcdCertificatesLabel: strconv.Itoa(current)}
	if err := r.applySecret(ctx, t, t.etcdName("ca", current), corev1.SecretTypeOpaque, map[string][]byte{caKey: certs.CertPEM(ca.Cert)}, labels); err != nil {
		return err
	}
	for _, p := range profiles {
		if _, err := r.tls(ctx, t, t.etcdName(p.role, current), ca, p.profile, labels); err != nil {
			return err
		}
	}
	for _, g := range generations {
		if g >= current-1 {
			continue
		}
		for _, role := range etcdRoles {
			name := t.etcdName(role, g)
			err := r.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: t.cp.Namespace}})
			if client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("delete Secret %s: %w", name, err)
			}
			delete(t.existing, name)
		}
	}
	return nil
}

func (t *tenant) etcdName(role string, generation int) string {
	return t.name("etcd-" + role + "-" + strconv.Itoa(generation))
}

type etcdProfile struct {
	role    string
	profile certs.Profile
}

// etcdProfiles are the certificates etcd-druid expects for the Etcd named
// <cp>-etcd, as hack/etcd-spike/pki.sh issued them.
func (t *tenant) etcdProfiles() []etcdProfile {
	n := t.name("etcd")
	forms := func(names ...string) []string {
		var out []string
		for _, s := range names {
			out = append(out, s, s+"."+t.cp.Namespace, s+"."+t.cp.Namespace+".svc", s+"."+t.cp.Namespace+".svc.cluster.local")
		}
		return out
	}
	local := []string{n + "-local"}
	loopback := []net.IP{net.ParseIP("127.0.0.1")}
	return []etcdProfile{
		{"server", certs.Profile{CommonName: n, Usages: serverClientAuth, DNSNames: slices.Concat(local, forms(n+"-client", "*."+n+"-peer"), []string{"localhost"}), IPAddresses: loopback}},
		{"peer", certs.Profile{CommonName: n + "-peer", Usages: serverClientAuth, DNSNames: forms(n+"-peer", "*."+n+"-peer")}},
		{"backup-restore", certs.Profile{CommonName: n + "-backup-restore", Usages: serverAuth, DNSNames: slices.Concat(local, forms(n+"-client"), []string{"localhost"}), IPAddresses: loopback}},
		{"client", certs.Profile{CommonName: n + "-client", Usages: clientAuth}},
	}
}

// applySecret writes the Secret with server-side apply, owned by the
// ControlPlane and labelled for the cache, unless it already is that.
func (r *Reconciler) applySecret(ctx context.Context, t *tenant, name string, typ corev1.SecretType, content map[string][]byte, labels map[string]string) error {
	cp := t.cp
	labels = maps.Clone(labels)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[bryggav1alpha1.ControlPlaneLabel] = cp.Name
	if s := t.existing[name]; s != nil && s.Type == typ && maps.EqualFunc(s.Data, content, bytes.Equal) && t.owns(s) && hasLabels(s, labels) {
		return nil
	}
	owner := metav1ac.OwnerReference().
		WithAPIVersion(bryggav1alpha1.GroupVersion.String()).
		WithKind(kind).
		WithName(cp.Name).
		WithUID(cp.UID).
		WithController(true).
		WithBlockOwnerDeletion(true)
	secret := corev1ac.Secret(name, cp.Namespace).WithLabels(labels).WithOwnerReferences(owner).WithType(typ).WithData(content)
	if err := r.Apply(ctx, secret, client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply Secret %s: %w", name, err)
	}
	t.existing[name] = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: *owner.APIVersion, Kind: kind, Name: cp.Name, UID: cp.UID, Controller: owner.Controller}}},
		Type: typ,
		Data: content,
	}
	return nil
}

func (t *tenant) owns(s *corev1.Secret) bool {
	return slices.ContainsFunc(s.OwnerReferences, func(o metav1.OwnerReference) bool {
		return o.UID == t.cp.UID && o.Controller != nil && *o.Controller
	})
}

func hasLabels(s *corev1.Secret, labels map[string]string) bool {
	for k, v := range labels {
		if s.Labels[k] != v {
			return false
		}
	}
	return true
}
