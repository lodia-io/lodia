# Credentials rotate in stages, and the routine ones never touch a node: the trust anchor is split from the client CA, and ServiceAccount tokens are signed by a sidecar

Decided 2026-10-25. `brygga-signer` is built; the PKI controller issues the
CA per purpose, the leaves, the kubeconfigs, the ServiceAccount key and the
encryption configuration, renews leaves at two thirds and the etcd set as
generations; the Workloads controller and the rotation operations are not
built.

## Decision

**A CA per purpose**, each a self-signed root: a CA signed by another would let
every bundle that trusts the other accept its certificates
([CA reuse](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/tasks/extend-kubernetes/configure-aggregation-layer.md#L300-L307)).
Each tenant has:

| CA | signs | who must trust it | lifetime |
|---|---|---|---|
| **cluster CA** (the trust anchor) | the API server's serving certificate, its kubelet client certificate, the tunnel and konnectivity serving certificates | every client and node: kubeconfigs, `cluster-info`, `kube-root-ca.crt`, the kubelet's `clientCAFile` | 10 years |
| **client CA** | every client identity: admin, controller manager, scheduler, Brygga, `KubeconfigRequest`s; and an intermediate *kubelet signer* | only the API server, whose `--client-ca-file` bundle holds the client CA and the current kubelet signer | 5 years |
| **front-proxy CA** | the API server's aggregation client certificate | the API server and aggregated API servers | 5 years |
| **etcd CA** | etcd server, peer and the API server's etcd client certificate | etcd and the API server | 5 years |

The controller manager signs kubelet client certificates with the kubelet
signer intermediate only, through the per-signer flags
`--cluster-signing-kubelet-client-cert-file` and `-key-file`; no root CA key
is ever mounted into a control plane pod. It does not sign other signers'
requests. kubelet client certificates live 30 days
(`--cluster-signing-duration=720h`), and kubelets rotate them on their own
([kube-controller-manager](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/)).

**ServiceAccount tokens** are signed by `brygga-signer`, a sidecar in the
API server pod that implements the external JWT signer API over a Unix
socket on an `emptyDir` (`--service-account-signing-endpoint`); it serves
the signing key and the verification keys from a Secret the PKI controller
writes. The external signer is beta and on by default in 1.35, and GA and
locked on since 1.36, so it is on in every minor Brygga supports
([KEP-740](https://github.com/kubernetes/enhancements/blob/master/keps/sig-auth/740-service-account-external-signing/README.md),
[`ExternalServiceAccountTokenSigner`](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/features/kube_features.go#L1612-L1616)).
The controller manager gets no ServiceAccount key at all: its
`--service-account-private-key-file` only enables legacy secret-based tokens.
Each tenant has its own issuer, `https://<public API host>`.

The Secret `<cp>-sa-keys` is the signer's whole input; a Secret key cannot
contain `/`, so its files are flat:

- `signing.key` — exactly one PEM private key (PKCS#8; PKCS#1 RSA and SEC1 EC
  are accepted): RSA of at least 2048 bits signs RS256, ECDSA P-256, P-384 and
  P-521 sign ES256, ES384 and ES512, the algorithms kube-apiserver accepts
  from a signer
  ([`validateJWTHeader`](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/serviceaccount/externaljwt/plugin/plugin.go#L148-L184));
- `verify-*.pub` — any number of PEM PKIX public keys, published for
  verification only;
- every other entry is ignored, including the kubelet's `..data` links, through
  which the signer reads the Secret so that one load never mixes two versions.

The published set is the signing key's public key and every `verify-*.pub`,
deduplicated and ordered by key ID; no key is excluded from OIDC discovery. A
key's ID is kube-apiserver's own: the unpadded base64url SHA-256 of its PKIX
DER
([`keyIDFromPublicKey`](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/serviceaccount/jwt.go#L99)),
so a cluster that moves from a static key file to the signer keeps its tokens
valid. The key set's timestamp changes only when the published set does, and
a Secret that does not load leaves the last good set in place: every replica
reads the same Secret, so the signer keeps serving and stays ready rather than
take every API server out at once. The three stages of the ServiceAccount key
rotation below are three states of this Secret: (1) `signing.key` K₁,
`verify-k2.pub` K₂; (2) `signing.key` K₂, `verify-k1.pub` K₁; (3)
`signing.key` K₂.

**Leaf certificates** renew at two thirds of their lifetime. The API server
reloads its serving certificate, client CA bundles, kubelet client
certificate and, per handshake, its etcd client certificate; components read
their kubeconfigs' certificates from mounted files, which client-go reloads;
etcd reloads its certificates per connection. A renewal therefore rolls
nothing except two, both in the maintenance window: the konnectivity server's
certificate, until a reload is proven, and the certificate of etcd's
backup-restore sidecar, which keeps serving its first one. etcd-druid rolls
the members only when the `Etcd` names a new Secret, so the PKI controller
renews etcd's certificates together, under new names: one roll, no API outage
with three members, about 20 s with one
([etcd spike](../etcd-spike-2026-10.md), finding 3). Each renewal is a
generation `g` of five Secrets, `<cp>-etcd-{ca,server,client,peer,backup-restore}-<g>`,
labelled `brygga.segla.io/etcd-certificates: "<g>"`; Datastore points the
`Etcd` at the newest complete generation, and PKI keeps the previous one and
deletes older ones, so neither controller reads the other's state.

**Rotations**, started with `brygga.segla.io/operation=<name>-start` and,
where a stage waits for people, completed with `<name>-complete`; phases in
`status.credentials.rotation`:

| rotation | Preparing | Prepared → Completing | touches nodes | completes |
|---|---|---|---|---|
| client CA | CA₂ joins the API server's client bundle (reloaded); Brygga re-issues every client certificate it holds and the admin kubeconfig from CA₂; the kubelet signer moves under CA₂ (the controller manager reloads signing files) | after one signing duration (30 days) every kubelet has renewed under CA₂; CA₁ leaves the bundle | no | on its own, or at once with `-complete` if the owner accepts losing stale kubeconfigs |
| ServiceAccount key | K₂ is published as a verification key; once every replica serves it, K₂ signs and K₁ only verifies | K₁ is dropped after the owner confirms that no client holds an old token | no | `-complete` |
| etcd encryption key | E₂ is added, then made first (the API server reloads the configuration every minute); the Tenant controller rewrites every encrypted resource | E₁ is removed | no | on its own |
| etcd CA, front-proxy CA | the new CA joins the bundles; certificates are re-issued; etcd and the API server roll where a CA file is read once | the old CA leaves the bundles | no | on its own |
| cluster CA (trust anchor) | CA₂ joins every bundle: `cluster-info`, `kube-root-ca.crt` (the controller manager restarts, its `--root-ca-file` is read once), the CA bundle Secret, kubeconfigs; node owners add CA₂ to `/etc/kubernetes/pki/ca.crt` and the kubelet's kubeconfig | serving, kubelet client, tunnel and konnectivity certificates move to CA₂; CA₁ leaves the bundles | **yes** | `-complete`, after the owner updated every node |

Only the last row needs people. It is meant for a compromised or expiring
trust anchor, once in a decade; every rotation done for hygiene is one of the
first four.

## Why

- **Every project we studied makes CA rotation a manual event, and most make
  it touch every node or break existing kubeconfigs**
  ([analysis](../analysis.md)). The reason is that one CA usually both
  anchors trust and signs identities, so rotating identities means
  re-anchoring trust everywhere. Splitting the two is what HyperShift's
  documented CA hierarchy does
  ([control plane certificates](https://github.com/openshift/hypershift/blob/main/docs/content/how-to/pki/control-plane-certificates.md)),
  and it means the routine rotation (client identities) never reaches a node.
- **Nodes Brygga does not manage cannot be re-anchored by Brygga.** A node's
  trust is in files kubeadm wrote at join; only its owner can change them.
  Keeping those files valid across routine rotations is the only rotation
  model that works for nodes anywhere.
- **The API server reloads almost everything now**: serving certificates and
  client CA bundles through dynamic file controllers, the encryption
  configuration with automatic reload, structured authentication and
  authorization configuration
  ([kube-apiserver](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-apiserver/),
  [encrypt data](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)).
  The ServiceAccount key was the exception — read once at startup, the reason
  KEP-740 exists — and the external signer removes it.
- **The CA key out of the controller manager** narrows what a compromised
  controller manager can sign to kubelet client certificates of a 30-day
  intermediate, not arbitrary identities under the root.
- **Stages with both credentials valid** are how Gardener rotates without
  outage
  ([credentials rotation](https://github.com/gardener/gardener/blob/master/docs/usage/shoot-operations/shoot_credentials_rotation.md));
  Brygga keeps the stages and their names.

## Rejected

- One cluster CA for everything (the kubeadm default) — every rotation
  becomes a node event.
- Mounting the root CA key into the controller manager — the default kubeadm
  layout; a compromised pod signs anything.
- The static multi-key ServiceAccount setup — works, but each step is a
  rolling restart of every API server.
- Automatic completion of the ServiceAccount key rotation — tokens extended
  for legacy clients can live a year; the owner decides when old ones may
  stop working.

## Consequences

- `brygga-signer` is a Brygga image in every API server pod; all replicas
  read the same Secret, and the PKI controller waits for every replica to
  report the new key set before switching the signing key.
- The phase 3 gate runs every rotation with two nodes `Ready` and a workload
  calling the API with a projected token throughout
  ([roadmap](../roadmap.md)).
- Kubelet serving certificates stay self-signed by default (no
  `serverTLSBootstrap`), so no CSR approver is needed in the tenant; turning
  it on is a later option with its own approver.
- An expiring trust anchor is announced a year ahead through
  `CertificatesReady` (`CAExpiring`) and an alert, because its rotation needs
  people.

## Code

- `cmd/brygga-signer` — the sidecar: the socket, the keys directory, the
  probes, `--max-token-expiration` (8760h, so the API server keeps upstream's
  one-year extension) and `--refresh-hint` (1m).
- `internal/signer` — the `ExternalJWTSigner` service of
  [`k8s.io/externaljwt/apis/v1`](https://github.com/kubernetes/externaljwt/blob/v0.37.1/apis/v1/api.proto),
  the file contract above, and the reload on a changed directory.
  `signer_test.go` holds each invariant for RSA and ECDSA keys;
  `TestRotationUnderARunningAPIServer` in `apiserver_test.go` runs the three
  stages against one envtest kube-apiserver whose only signer it is.
- [The hard way, spike d](../hard-way.md#d-spike-serviceaccount-tokens-from-an-external-signer)
  — the same rotation by hand in kind, with a pod calling the API throughout.
- `internal/certs` — ECDSA P-256 CAs and leaves from `crypto/x509`, valid
  from five minutes before issuance (kubeadm's backdate,
  [constants.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/cmd/kubeadm/app/constants/constants.go#L45-L46))
  and no longer than their CA, renewed two thirds of the way from issuance to
  expiry; bundles as sets in DER order, as library-go writes them
  ([cabundle.go](https://github.com/openshift/library-go/blob/9a2bc12675c2bb37d3799a30786ccee5fa545d57/pkg/operator/certrotation/cabundle.go#L134-L177));
  PEM through client-go's `util/cert` and `util/keyutil`; kubeconfigs with
  their certificates as files beside them or embedded. `certs_test.go` holds
  the validity and the bundle order.
- `internal/controller/pki` — the controller. `secrets.go` is the Secret
  table above: the four `<cp>-cakey-*` key pairs that no pod mounts, each a
  self-signed root with the second of its issuance in its subject; the
  kubelet signer, matched by its Secret rather than its subject, and the
  client CA bundle that keeps a replaced signer for one signing duration;
  the leaves and kubeconfigs; `<cp>-sa-keys` with its `signing.key`; the
  encryption configuration, written last so that a provider PKI cannot
  render (`kms`) blocks nothing else; and the immutable etcd generations, a
  generation with a member missing or being deleted replaced by the next and
  the previous one kept. A CA is created only for a tenant that has neither
  its key Secret nor its bundle; a lost or unreadable key is `CAMissing`, and
  nothing that CA signs is written. `reconciler_test.go` proves each of
  these against a running envtest API server: the subjects, usages, SANs and
  validity; that no Secret but `<cp>-cakey-*` and `<cp>-kubelet-signer`
  holds a CA's key; that a pass over an unchanged `ControlPlane` changes no
  `resourceVersion` and a moved `status.endpoint` re-issues exactly the
  serving certificate, the konnectivity server certificate and the admin
  kubeconfig; the renewal at two thirds with the CA key pairs unchanged and
  the old signer still in the bundle under its own subject; etcd generations
  1, 2 and 3 with 1 deleted after the third, and a generation replaced, not
  repaired, when a member is deleted or held by a finalizer; a lost CA left
  in place; `kms` reported with everything else issued.
