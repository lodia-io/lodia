# Nodes join with `kubeadm join`; Lodia writes the bootstrap objects itself and never imports k8s.io/kubernetes

Decided 2026-10-25.
The bootstrap objects are built, and the e2e suite joins a node with them on every pull request.

## Decision

- A worker joins a Lodia control plane with the standard `kubeadm join <endpoint> --token … --discovery-token-ca-cert-hash sha256:…`.
  Nothing Lodia-specific runs on the node; the node needs outbound TCP 443 to the control plane endpoint and nothing inbound.
- The Tenant controller creates in the tenant cluster everything `kubeadm join` reads and every permission it needs ([the list](../README.md#inside-the-tenant)).
- The controller manager runs with `bootstrap-signer-controller` and `token-cleaner-controller` enabled (both are off by default), so `cluster-info` carries a valid JWS signature for every bootstrap token and expired tokens disappear; the API server runs with `--enable-bootstrap-token-auth` and lets anonymous requests read `cluster-info` and nothing else but the health paths ([bootstrap tokens](https://kubernetes.io/docs/reference/access-authn-authz/bootstrap-tokens/), [kubeadm join implementation](https://kubernetes.io/docs/reference/setup-tools/kubeadm/implementation-details/)).
- Bootstrap tokens are created by the cluster's owner with an admin kubeconfig (`kubeadm token create --print-join-command`), by a platform on top, or by hand; Lodia does not mint them.
- These objects are rendered from Lodia's own templates of the `ClusterConfiguration` and `KubeletConfiguration` documents.
  Lodia does not import `k8s.io/kubernetes` (where kubeadm's types live); the documents are small, versioned YAML.
- Lodia does not manage machines.
  A Cluster API provider that creates a `ControlPlane` and uses its kubeconfig is a separate project in the roadmap.

## Why

- `kubeadm join` is what every node tool, image and tutorial already speaks: cloud-init, Ansible, Cluster API's bootstrap provider, Segla's agent.
  Any Linux machine that can run a kubelet can join.
- Importing `k8s.io/kubernetes` pins the whole operator to one Kubernetes minor and drags in its dependency graph; the two documents kubeadm reads are stable, versioned (`kubeadm.k8s.io/v1beta4`, `kubelet.config.k8s.io/v1beta1`) and easy to template.
- Keeping machines out of scope keeps the operator about control planes; the hosted control plane projects that do both carry a provider per infrastructure.

## Rejected

- A Lodia node agent — one more thing to install and trust on a node; the kubelet's TLS bootstrap already covers identity.
- Ignition or image-based joins — tied to one OS family.

## Consequences

- The Kubernetes version skew policy sets the order of upgrades: the control plane first, then nodes, which may lag by up to three minors ([upgrades-and-maintenance-windows](upgrades-and-maintenance-windows.md)).
- Lodia's e2e suite joins real kubeadm nodes, so a regression in the bootstrap objects fails the gate.

## Code

- `internal/controller/tenant/manifests/bootstrap.yaml` — `cluster-info`, `kubeadm-config`, `kubelet-config` and the bootstrap RBAC, named and shaped as [kubeadm v1.35.0](https://github.com/kubernetes/kubernetes/tree/v1.35.0/cmd/kubeadm/app/phases) writes them; `kube-proxy.yaml` and `coredns.yaml` are kubeadm's addons.
- `test/e2e/gate_test.go` — `TestPhase1Gate`: a container of kind's node image joins with `kubeadm join --token …` and a bootstrap token the suite creates as the platform, through the admin kubeconfig.
