# Nodes join with `kubeadm join`; Brygga writes the bootstrap objects itself and never imports k8s.io/kubernetes

Decided 2026-10-25. The bootstrap objects are built; the e2e join comes
with the phase 1 gate.

## Decision

- A worker joins a Brygga control plane with the standard
  `kubeadm join <endpoint> --token … --discovery-token-ca-cert-hash sha256:…`.
  Nothing Brygga-specific runs on the node; the node needs outbound TCP 443
  to the control plane endpoint and nothing inbound.
- The Tenant controller creates in the tenant cluster everything `kubeadm
  join` reads and every permission it needs: the `cluster-info` ConfigMap in
  `kube-public` with the CA bundle and endpoint, the `kubeadm-config` and
  `kubelet-config` ConfigMaps in `kube-system`, the RBAC for bootstrap tokens
  (node bootstrapper, CSR auto-approval for new nodes and for certificate
  rotation, `get nodes`), and kube-proxy's ConfigMap and RBAC. The exact list
  is in [`docs/README.md`](../README.md#inside-the-tenant).
- The controller manager runs with `bootstrap-signer-controller` and
  `token-cleaner-controller` enabled (both are off by default), so
  `cluster-info` carries a valid JWS signature for every bootstrap token and
  expired tokens disappear; the API server runs with
  `--enable-bootstrap-token-auth` and lets anonymous requests read
  `cluster-info` and nothing else but the health paths
  ([bootstrap tokens](https://kubernetes.io/docs/reference/access-authn-authz/bootstrap-tokens/),
  [kubeadm join implementation](https://kubernetes.io/docs/reference/setup-tools/kubeadm/implementation-details/)).
- Bootstrap tokens are created by the cluster's owner with an admin
  kubeconfig (`kubeadm token create --print-join-command`), by a platform on
  top, or by hand; Brygga does not mint them.
- These objects are rendered from Brygga's own templates of the
  `ClusterConfiguration` and `KubeletConfiguration` documents. Brygga does not
  import `k8s.io/kubernetes` (where kubeadm's types live); the documents are
  small, versioned YAML.
- Brygga does not manage machines. A Cluster API provider that creates a
  `ControlPlane` and uses its kubeconfig is a separate project in the roadmap.

## Why

- `kubeadm join` is what every node tool, image and tutorial already speaks:
  cloud-init, Ansible, Cluster API's bootstrap provider, Segla's agent. Any
  Linux machine that can run a kubelet can join.
- Importing `k8s.io/kubernetes` pins the whole operator to one Kubernetes
  minor and drags in its dependency graph; the two documents kubeadm reads
  are stable, versioned (`kubeadm.k8s.io/v1beta4`,
  `kubelet.config.k8s.io/v1beta1`) and easy to template.
- Keeping machines out of scope keeps the operator about control planes; the
  hosted control plane projects that do both carry a provider per
  infrastructure.

## Rejected

- A Brygga node agent — one more thing to install and trust on a node; the
  kubelet's TLS bootstrap already covers identity.
- Ignition or image-based joins — tied to one OS family.

## Consequences

- The Kubernetes version skew policy sets the order of upgrades: the control
  plane first, then nodes, which may lag by up to three minors
  ([upgrades-and-maintenance-windows](upgrades-and-maintenance-windows.md)).
- Kubelet client certificates are signed by the controller manager with an
  intermediate kubelet signer, never with a root CA key
  ([staged-credential-rotation](staged-credential-rotation.md)); kubelet
  serving certificates stay self-signed unless `serverTLSBootstrap` is turned
  on later together with an approver.
- Brygga's e2e suite joins real kubeadm nodes, so a regression in the
  bootstrap objects fails the gate.

## Code

- `internal/controller/tenant/manifests/bootstrap.yaml` — `cluster-info`,
  `kubeadm-config`, `kubelet-config` and the bootstrap RBAC, named and
  shaped as kubeadm v1.35.0 writes them; `kube-proxy.yaml` and
  `coredns.yaml` are kubeadm's addons.
