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

// A CA's bundle proves the tenant had that CA, so a CA is created only when
// neither the bundle nor its key Secret exists.
var caBundles = []struct {
	ca, bundle string
}{
	{clusterCA, "ca-bundle"},
	{clientCA, "client-ca-bundle"},
	{frontProxyCA, "front-proxy-ca"},
	{etcdCA, "etcd-ca"},
}

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

	providerNone      bryggav1alpha1.EncryptionProvider = "None"
	providerSecretbox bryggav1alpha1.EncryptionProvider = "secretbox"
	providerAESCBC    bryggav1alpha1.EncryptionProvider = "aescbc"
)

var (
	serverAuth       = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	clientAuth       = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	serverClientAuth = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	etcdRoles        = []string{"ca", "server", "client", "peer", "backup-restore"}
)

type tenant struct {
	cp       *bryggav1alpha1.ControlPlane
	existing map[string]*corev1.Secret
	now      time.Time
	// wake is the earliest renewal, CA notice or CA expiry still ahead.
	wake time.Time
	// cas sign; caCerts holds every CA read, expired ones included.
	cas                 map[string]*certs.Certificate
	caCerts             map[string]*x509.Certificate
	missingCAs          []string
	expiredCAs          []string
	unsupportedProvider bryggav1alpha1.EncryptionProvider
	endpoint            *endpoint
}

type endpoint struct {
	host string
	port int32
}

func (e *endpoint) url() string {
	return "https://" + net.JoinHostPort(e.host, strconv.Itoa(int(e.port)))
}

func newTenant(cp *bryggav1alpha1.ControlPlane, secrets []corev1.Secret, now time.Time) *tenant {
	t := &tenant{cp: cp, existing: make(map[string]*corev1.Secret, len(secrets)), now: now, cas: map[string]*certs.Certificate{}, caCerts: map[string]*x509.Certificate{}}
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

func (t *tenant) stamp(prefix string) string {
	return prefix + "@" + strconv.FormatInt(t.now.Unix(), 10)
}

func (t *tenant) wakeAt(at time.Time) {
	if at.After(t.now) && (t.wake.IsZero() || at.Before(t.wake)) {
		t.wake = at
	}
}

// renew schedules c's renewal, unless c already ends with its CA: a new
// certificate would not outlive it.
func (t *tenant) renew(c, ca *x509.Certificate) {
	if c.NotAfter.Before(ca.NotAfter) {
		t.wakeAt(certs.RenewalTime(c))
	}
}

func (t *tenant) requeueAfter() time.Duration {
	after := bindingsPeriod
	if !t.wake.IsZero() {
		after = min(after, t.wake.Sub(t.now))
	}
	return max(after.Truncate(time.Second), time.Second)
}

func (t *tenant) credentials() map[string]any {
	out := map[string]any{}
	for field, secret := range map[string]string{"adminKubeconfigSecretRef": t.name("admin-kubeconfig"), "caBundleSecretRef": t.name("ca-bundle")} {
		if t.existing[secret] != nil {
			out[field] = map[string]any{"name": secret}
		}
	}
	return out
}

func data(s *corev1.Secret, key string) []byte {
	if s == nil {
		return nil
	}
	return s.Data[key]
}

func isImmutable(s *corev1.Secret) bool {
	return s.Immutable != nil && *s.Immutable
}

func (t *tenant) current(c, ca *certs.Certificate, p certs.Profile) bool {
	return certs.Verify(c.Cert, ca.Cert) == nil && certs.Matches(c.Cert, p) &&
		(t.now.Before(certs.RenewalTime(c.Cert)) || !c.Cert.NotAfter.Before(ca.Cert.NotAfter))
}

func (t *tenant) leaf(certPEM, keyPEM []byte, ca *certs.Certificate, p certs.Profile, lifetime time.Duration) (*certs.Certificate, error) {
	c, err := certs.Parse(certPEM, keyPEM)
	if err != nil || !t.current(c, ca, p) {
		if c, err = ca.Issue(p, lifetime); err != nil {
			return nil, err
		}
	}
	t.renew(c.Cert, ca.Cert)
	return c, nil
}

// issue writes the encryption configuration last, so that an unsupported
// provider blocks nothing else.
func (r *Reconciler) issue(ctx context.Context, t *tenant) error {
	for _, ca := range caBundles {
		if err := r.ca(ctx, t, ca.ca, ca.bundle); err != nil {
			return err
		}
	}
	cluster, cl, frontProxy, etcd := t.cas[clusterCA], t.cas[clientCA], t.cas[frontProxyCA], t.cas[etcdCA]

	if cluster != nil {
		if err := r.bundle(ctx, t, t.name("ca-bundle"), cluster.Cert); err != nil {
			return err
		}
		if err := r.tls(ctx, t, t.name("apiserver-kubelet-client"), cluster, certs.Profile{CommonName: "kube-apiserver-kubelet-client", Organizations: []string{"brygga:kubelet-api-admins"}, Usages: clientAuth}); err != nil {
			return err
		}
		if t.endpoint != nil {
			if err := r.tls(ctx, t, t.name("apiserver-serving"), cluster, t.servingProfile()); err != nil {
				return err
			}
			if err := r.tls(ctx, t, t.name("konnectivity-server"), cluster, t.konnectivityProfile()); err != nil {
				return err
			}
		}
	}

	if cl != nil {
		if err := r.kubeletSigner(ctx, t); err != nil {
			return err
		}
	}
	if cl != nil && cluster != nil {
		internal := "https://" + t.name("kube-apiserver") + "." + t.cp.Namespace + ".svc:443"
		kubeconfigs := []kubeconfig{
			{"konnectivity-kubeconfig", "system:konnectivity-server", nil, "https://127.0.0.1:6443", false},
			{"controller-manager-kubeconfig", "system:kube-controller-manager", nil, internal, false},
			{"scheduler-kubeconfig", "system:kube-scheduler", nil, internal, false},
			{"brygga-kubeconfig", "brygga:operator", nil, internal, true},
		}
		if t.endpoint != nil {
			kubeconfigs = append(kubeconfigs, kubeconfig{"admin-kubeconfig", "kubernetes-admin", []string{"brygga:cluster-admins"}, t.endpoint.url(), true})
		}
		for _, k := range kubeconfigs {
			if err := r.kubeconfig(ctx, t, k); err != nil {
				return err
			}
		}
	}

	if frontProxy != nil {
		if err := r.bundle(ctx, t, t.name("front-proxy-ca"), frontProxy.Cert); err != nil {
			return err
		}
		if err := r.tls(ctx, t, t.name("front-proxy-client"), frontProxy, certs.Profile{CommonName: "front-proxy-client", Usages: clientAuth}); err != nil {
			return err
		}
	}

	if etcd != nil {
		if err := r.bundle(ctx, t, t.name("etcd-ca"), etcd.Cert); err != nil {
			return err
		}
		if err := r.tls(ctx, t, t.name("apiserver-etcd-client"), etcd, certs.Profile{CommonName: "kube-apiserver-etcd-client", Usages: clientAuth}); err != nil {
			return err
		}
		if err := r.etcd(ctx, t); err != nil {
			return err
		}
	}

	if err := r.serviceAccountKey(ctx, t); err != nil {
		return err
	}
	return r.encryptionConfig(ctx, t)
}

// lookup reads a Secret the labelled list lacks by name, so one restored
// without its labels is used, not replaced.
func (r *Reconciler) lookup(ctx context.Context, t *tenant, name string) (*corev1.Secret, error) {
	if s := t.existing[name]; s != nil {
		return s, nil
	}
	s := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: t.cp.Namespace, Name: name}, s); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	t.existing[name] = s
	return s, nil
}

// ca creates a CA only for a tenant with neither its key Secret nor its
// bundle: a new CA in place of a lost or expired one would cut every client
// or node off at once.
func (r *Reconciler) ca(ctx context.Context, t *tenant, name, bundle string) error {
	secret := t.name("cakey-" + name)
	key, err := r.lookup(ctx, t, secret)
	if err != nil {
		return err
	}
	trusted, err := r.lookup(ctx, t, t.name(bundle))
	if err != nil {
		return err
	}
	if key == nil && trusted == nil {
		lifetime := r.CALifetime
		if name == clusterCA {
			lifetime = r.TrustAnchorLifetime
		}
		c, err := certs.NewCA(t.stamp(name+"-ca"), lifetime)
		if err != nil {
			return err
		}
		t.cas[name], t.caCerts[name] = c, c.Cert
		r.notice(t, c.Cert)
		return r.applyTLS(ctx, t, secret, c, nil, false)
	}
	c := readCA(key)
	if c == nil {
		t.missingCAs = append(t.missingCAs, name)
		return nil
	}
	t.caCerts[name] = c.Cert
	if !t.now.Before(c.Cert.NotAfter) {
		t.expiredCAs = append(t.expiredCAs, name)
		return nil
	}
	t.cas[name] = c
	r.notice(t, c.Cert)
	return r.applySecret(ctx, t, secret, corev1.SecretTypeTLS, map[string][]byte{corev1.TLSCertKey: key.Data[corev1.TLSCertKey], corev1.TLSPrivateKeyKey: key.Data[corev1.TLSPrivateKeyKey]}, nil, false)
}

func readCA(s *corev1.Secret) *certs.Certificate {
	c, err := certs.Parse(data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey))
	if err != nil || !c.Cert.IsCA || c.Cert.CheckSignatureFrom(c.Cert) != nil {
		return nil
	}
	return c
}

func (r *Reconciler) notice(t *tenant, ca *x509.Certificate) {
	t.wakeAt(ca.NotAfter.Add(-r.CAExpiryNotice))
	t.wakeAt(ca.NotAfter)
}

// kubeletSigner compares no subject, since every signer gets a new one, and
// writes a new signer only after the client CA bundle that trusts it.
func (r *Reconciler) kubeletSigner(ctx context.Context, t *tenant) error {
	name := t.name("kubelet-signer")
	ca := t.cas[clientCA]
	s := t.existing[name]
	c, err := certs.Parse(data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey))
	if err != nil || !c.Cert.MaxPathLenZero || !t.current(c, ca, certs.Profile{CommonName: c.Cert.Subject.CommonName, CA: true}) {
		if c, err = ca.Issue(certs.Profile{CommonName: t.stamp("kubelet-signer"), CA: true}, r.LeafLifetime); err != nil {
			return err
		}
	}
	t.renew(c.Cert, ca.Cert)
	if err := r.bundle(ctx, t, t.name("client-ca-bundle"), t.clientBundle(c)...); err != nil {
		return err
	}
	return r.applyTLS(ctx, t, name, c, nil, false)
}

func (r *Reconciler) tls(ctx context.Context, t *tenant, name string, ca *certs.Certificate, p certs.Profile) error {
	s := t.existing[name]
	c, err := t.leaf(data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey), ca, p, r.LeafLifetime)
	if err != nil {
		return err
	}
	return r.applyTLS(ctx, t, name, c, nil, false)
}

func (r *Reconciler) applyTLS(ctx context.Context, t *tenant, name string, c *certs.Certificate, labels map[string]string, immutable bool) error {
	cert, err := certs.CertPEM(c.Cert)
	if err != nil {
		return err
	}
	key, err := certs.KeyPEM(c.Key)
	if err != nil {
		return err
	}
	return r.applySecret(ctx, t, name, corev1.SecretTypeTLS, map[string][]byte{corev1.TLSCertKey: cert, corev1.TLSPrivateKeyKey: key}, labels, immutable)
}

func (r *Reconciler) bundle(ctx context.Context, t *tenant, name string, bundle ...*x509.Certificate) error {
	pem, err := certs.Bundle(bundle...)
	if err != nil {
		return err
	}
	return r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{caKey: pem}, nil, false)
}

// clientBundle keeps each replaced signer for one signing duration, since
// kubelets present only their leaf.
func (t *tenant) clientBundle(signer *certs.Certificate) []*x509.Certificate {
	ca := t.cas[clientCA].Cert
	bundle := []*x509.Certificate{ca, signer.Cert}
	previous, _ := certs.ParseCerts(data(t.existing[t.name("client-ca-bundle")], caKey))
	replaced := signer.Cert.NotBefore.Add(certs.Backdate)
	for _, c := range previous {
		if c.IsCA && !c.Equal(ca) && certs.Verify(c, ca) == nil && t.now.Before(replaced.Add(signingDuration)) {
			bundle = append(bundle, c)
		}
	}
	return bundle
}

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
	cert, err := certs.CertPEM(c.Cert)
	if err != nil {
		return err
	}
	key, err := certs.KeyPEM(c.Key)
	if err != nil {
		return err
	}
	ca, err := certs.CertPEM(t.cas[clusterCA].Cert)
	if err != nil {
		return err
	}
	rendered, err := certs.Kubeconfig(t.cp.Name, k.server, k.user, ca, cert, key, k.embed)
	if err != nil {
		return err
	}
	content := map[string][]byte{kubeconfigKey: rendered}
	if !k.embed {
		content[caKey], content[corev1.TLSCertKey], content[corev1.TLSPrivateKeyKey] = ca, cert, key
	}
	return r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, content, nil, false)
}

// serviceAccountKey keeps <cp>-sa-keys, brygga-signer's file contract.
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
	return r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, content, nil, false)
}

func isServiceAccountKey(key any) bool {
	rsaKey, ok := key.(*rsa.PrivateKey)
	return ok && rsaKey.N.BitLen() >= serviceAccountKeyBits
}

// encryptionConfig puts a changed provider first with a new key, and keeps
// the previous providers so that what they encrypted stays readable.
func (r *Reconciler) encryptionConfig(ctx context.Context, t *tenant) error {
	name := t.name("encryption-config")
	existing := data(t.existing[name], encryptionConfigKey)
	want := providerSecretbox
	if api := t.cp.Spec.APIServer; api != nil && api.Encryption != nil && api.Encryption.Provider != "" {
		want = api.Encryption.Provider
	}
	if !slices.Contains([]bryggav1alpha1.EncryptionProvider{providerNone, providerSecretbox, providerAESCBC}, want) {
		t.unsupportedProvider = want
		return nil
	}
	var cfg apiserverv1.EncryptionConfiguration
	var providers []apiserverv1.ProviderConfiguration
	if err := yaml.Unmarshal(existing, &cfg); err == nil && len(cfg.Resources) == 1 {
		providers = cfg.Resources[0].Providers
	}
	if len(providers) > 0 && providerOf(providers[0]) == want {
		return r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{encryptionConfigKey: existing}, nil, false)
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
	return r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{encryptionConfigKey: rendered}, nil, false)
}

func providerOf(p apiserverv1.ProviderConfiguration) bryggav1alpha1.EncryptionProvider {
	switch {
	case p.Secretbox != nil:
		return providerSecretbox
	case p.AESCBC != nil:
		return providerAESCBC
	case p.Identity != nil:
		return providerNone
	default:
		return ""
	}
}

func newProvider(provider bryggav1alpha1.EncryptionProvider) (apiserverv1.ProviderConfiguration, error) {
	if provider == providerNone {
		return apiserverv1.ProviderConfiguration{Identity: &apiserverv1.IdentityConfiguration{}}, nil
	}
	secret := make([]byte, encryptionKeyBytes)
	if _, err := rand.Read(secret); err != nil {
		return apiserverv1.ProviderConfiguration{}, err
	}
	keys := []apiserverv1.Key{{Name: "key1", Secret: base64.StdEncoding.EncodeToString(secret)}}
	switch provider {
	case providerSecretbox:
		return apiserverv1.ProviderConfiguration{Secretbox: &apiserverv1.SecretboxConfiguration{Keys: keys}}, nil
	case providerAESCBC:
		return apiserverv1.ProviderConfiguration{AESCBC: &apiserverv1.AESConfiguration{Keys: keys}}, nil
	default:
		return apiserverv1.ProviderConfiguration{}, fmt.Errorf("encryption provider %q is not supported", provider)
	}
}

// etcd keeps the current generation and the newest complete one below it,
// which the Etcd may still run on, and deletes every other.
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
	if kept := t.etcdGeneration(current, ca, profiles); kept != nil {
		for _, c := range kept {
			t.renew(c.Cert, ca.Cert)
		}
	} else {
		current++
		if err := r.issueEtcd(ctx, t, current, ca, profiles); err != nil {
			return err
		}
	}
	previous := 0
	for _, g := range generations {
		if g < current && t.etcdComplete(g) {
			previous = g
		}
	}
	for _, g := range generations {
		if g == current || g == previous {
			continue
		}
		for _, role := range etcdRoles {
			name := t.etcdName(role, g)
			if s := t.existing[name]; s == nil || s.DeletionTimestamp != nil {
				continue
			}
			err := r.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: t.cp.Namespace}})
			if client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("delete Secret %s: %w", name, err)
			}
		}
	}
	return nil
}

// issueEtcd writes generation g whole and immutable: etcd-druid rolls the
// members only for a new Secret name, never for one changed in place.
func (r *Reconciler) issueEtcd(ctx context.Context, t *tenant, g int, ca *certs.Certificate, profiles []etcdProfile) error {
	caPEM, err := certs.CertPEM(ca.Cert)
	if err != nil {
		return err
	}
	labels := map[string]string{bryggav1alpha1.EtcdCertificatesLabel: strconv.Itoa(g)}
	if err := r.applySecret(ctx, t, t.etcdName("ca", g), corev1.SecretTypeOpaque, map[string][]byte{caKey: caPEM}, labels, true); err != nil {
		return err
	}
	for _, p := range profiles {
		c, err := ca.Issue(p.profile, r.LeafLifetime)
		if err != nil {
			return err
		}
		t.renew(c.Cert, ca.Cert)
		if err := r.applyTLS(ctx, t, t.etcdName(p.role, g), c, labels, true); err != nil {
			return err
		}
	}
	return nil
}

func (t *tenant) etcdComplete(g int) bool {
	for _, role := range etcdRoles {
		if s := t.existing[t.etcdName(role, g)]; s == nil || s.DeletionTimestamp != nil {
			return false
		}
	}
	return true
}

func (t *tenant) etcdGeneration(g int, ca *certs.Certificate, profiles []etcdProfile) map[string]*certs.Certificate {
	if g == 0 || !t.etcdComplete(g) {
		return nil
	}
	s := t.existing[t.etcdName("ca", g)]
	if bundle, err := certs.ParseCerts(s.Data[caKey]); !isImmutable(s) || err != nil || len(bundle) != 1 || !bundle[0].Equal(ca.Cert) {
		return nil
	}
	kept := make(map[string]*certs.Certificate, len(profiles))
	for _, p := range profiles {
		s := t.existing[t.etcdName(p.role, g)]
		c, err := certs.Parse(s.Data[corev1.TLSCertKey], s.Data[corev1.TLSPrivateKeyKey])
		if !isImmutable(s) || err != nil || !t.current(c, ca, p.profile) {
			return nil
		}
		kept[p.role] = c
	}
	return kept
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

func (r *Reconciler) applySecret(ctx context.Context, t *tenant, name string, typ corev1.SecretType, content map[string][]byte, labels map[string]string, immutable bool) error {
	cp := t.cp
	labels = maps.Clone(labels)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[bryggav1alpha1.ControlPlaneLabel] = cp.Name
	if s := t.existing[name]; s != nil && (immutable && isImmutable(s) ||
		s.Type == typ && maps.EqualFunc(s.Data, content, bytes.Equal) && t.owns(s) && hasLabels(s, labels)) {
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
	if immutable {
		secret.WithImmutable(true)
	}
	if err := r.Apply(ctx, secret, client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply Secret %s: %w", name, err)
	}
	t.existing[name] = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: *owner.APIVersion, Kind: kind, Name: cp.Name, UID: cp.UID, Controller: owner.Controller}}},
		Type:      typ,
		Data:      content,
		Immutable: secret.Immutable,
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
