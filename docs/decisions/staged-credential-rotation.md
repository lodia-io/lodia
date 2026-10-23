# Credentials rotate in stages, and the routine ones never touch a node: the trust anchor is split from the client CA, and ServiceAccount tokens are signed by a sidecar

Proposed 2026-10-23; not decided yet. Not built.

## Decision

**A CA per purpose.** Each tenant has:

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
writes. The external signer is beta and on by default in 1.34 and 1.35 and
stable in 1.36, the minors Brygga supports
([KEP-740](https://github.com/kubernetes/enhancements/blob/master/keps/sig-auth/740-service-account-external-signing/README.md)).
The controller manager gets no ServiceAccount key at all: its
`--service-account-private-key-file` only enables legacy secret-based tokens.
Each tenant has its own issuer, `https://<public API host>`.

**Leaf certificates** renew at two thirds of their lifetime. The API server
reloads its serving certificate, client CA bundles, kubelet client
certificate and, per handshake, its etcd client certificate; components read
their kubeconfigs' certificates from mounted files, which client-go reloads;
etcd reloads its certificates per connection. A renewal therefore rolls
nothing, except the konnectivity server's certificate, which is renewed in
the maintenance window until a reload is proven.

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

none yet.
