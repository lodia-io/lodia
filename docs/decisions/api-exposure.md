# Control planes are exposed through a shared Gateway by SNI; pods reach their API through a node-local proxy and a fixed advertise address

Decided 2026-10-25. Not built.

## Decision

**Three ways out, one default.** `spec.endpoint` is one of:

| mode | what Brygga creates | public endpoint | in-cluster `kubernetes` Service |
|---|---|---|---|
| `gateway` (default) | `TLSRoute`s on the platform's Gateway: passthrough for `hostnames.apiServer` → the API Service and `hostnames.konnectivity` → the konnectivity Service; with `hostnames.tunnel`, a `ListenerSet` with a terminating TLS listener and a `TLSRoute` → the API Service | `https://<hostnames.apiServer>:443` | endpoints `172.20.0.1:6443`, served on every node by `apiserver-proxy` |
| `loadBalancer` | one Service of type LoadBalancer: 443 and 6443 → the API server, 8132 → konnectivity | `https://<LB address>:443` | endpoints `<LB address>:6443`, no proxy |
| `nodePort` | one Service of type NodePort with the same ports | `https://<a management node>:<port>` | endpoints `<node>:<port>`; development only |

**Gateway prerequisites.** Gateway API ≥ 1.5, where `TLSRoute` and
`ListenerSet` are in the Standard channel; an implementation that reports
`TLSRoute` (passthrough) and, for the tunnel, `TLSRouteModeTerminate`, plus
`TLSRouteModeMixed` when the tunnel shares port 443 with the passthrough
listener ([TLSRoute](https://gateway-api.sigs.k8s.io/reference/api-types/tlsroute/),
[ListenerSet](https://gateway-api.sigs.k8s.io/reference/api-types/listenerset/)).
The platform owns the Gateway: a `TLS` listener in `Passthrough` mode on 443
for a wildcard hostname, routes allowed from tenant namespaces by label, and
`allowedListeners` that admits tenant `ListenerSet`s. Brygga only attaches.
Missing features turn `APIServerReady` False with reason `GatewayUnsupported`.

**In-cluster access (the `gateway` mode).** A connection from a pod to the
`kubernetes` ClusterIP carries no SNI, and every tenant's
`kubernetes.default.svc` looks the same, so the shared Gateway cannot route
it. Brygga uses the shape HyperShift ships, with the outer hop Segla's agent
already runs in production:

1. The API server runs with `--advertise-address=172.20.0.1` (configurable
   per `ControlPlane`, never inside the tenant's node, pod or service
   ranges); the default `lease` endpoint reconciler publishes it, so the
   `kubernetes` Service's endpoint is `172.20.0.1:6443`. EndpointSlices reject
   loopback and link-local addresses, which is why the address is a private
   one and not `127.x` or `169.254.x`.
2. The Tenant controller runs `apiserver-proxy` as a `hostNetwork`
   DaemonSet in `kube-system` (priority `system-node-critical`, tolerating
   not-ready nodes, so it starts before the CNI): it adds `172.20.0.1/32` to
   the node's loopback and listens on `172.20.0.1:6443`.
3. For each connection it opens TLS to `<hostnames.tunnel>:443` with that
   SNI, verifying the tunnel certificate against the cluster CA, and copies
   the pod's bytes inside. The pod's own TLS session with the API server
   travels untouched; the API server's serving certificate carries the
   `kubernetes` ClusterIP, the first address of the tenant's service range,
   as a SAN, as always.
4. The Gateway terminates the outer TLS on the tenant's `ListenerSet`
   listener, whose certificate PKI issues from the cluster CA, and its
   `TLSRoute` in `Terminate` mode forwards the inner stream to the API
   Service.

kube-proxy, or any CNI that replaces it, sees a normal Service with a normal
endpoint; nothing on the node rewrites packets. Kubelets and kube-proxy
themselves use the public endpoint by name, so they never need the proxy.

**Konnectivity.** One `konnectivity-server` per API server replica, beside
it in the pod, in gRPC mode on a Unix socket (`--uds-name`,
`--server-port=0`), the egress selector sending only `cluster` traffic
through it; agents authenticate with a projected ServiceAccount token
(`--agent-namespace`, `--agent-service-account`, `--authentication-audience`),
which the server checks with TokenReviews as `system:konnectivity-server`
bound to `system:auth-delegator` and to lease verbs in `kube-system`; the
server's lease controller is on and agents count servers from leases
(`--count-server-leases`, `--sync-forever`), so agents hold a tunnel to every
replica without a static count. Agents run as a DaemonSet on the pod network
with `--agent-identifiers=ipv4=$(HOST_IP)` and the server tries `destHost`
before `default`, so traffic for a kubelet goes through the agent on its own
node
([konnectivity setup](https://kubernetes.io/docs/tasks/extend-kubernetes/setup-konnectivity/),
[server options](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/master/cmd/server/app/options/options.go),
[agent options](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/master/cmd/agent/app/options/options.go)).
The konnectivity version follows the Kubernetes minor (proxy 0.NN for
Kubernetes 1.NN, [README](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/master/README.md)).

**Nothing inbound on a node.** The API server reaches kubelets, webhooks and
aggregated APIs through the agents' tunnels. Brygga's defaults bind what it
configures to the smallest address that works: kubelet health on
`127.0.0.1:10248`, no read-only port, anonymous kubelet access off;
kube-proxy metrics and health on loopback (the health port can be opened for
cloud load balancers with `addons.kubeProxy.healthzBindAddress`); agent admin
on loopback and health on the pod IP. The kubelet's own listener address is
a node flag (`--address`, `--node-ip`), which the node's owner passes through
`kubeletExtraArgs` at join; the docs recommend the node IP.

## Why

- **Cost and limits.** A load balancer per control plane is the line item
  that grows with tenants, and providers cap them; Gardener measured exactly
  this before moving every shoot behind one load balancer per seed with SNI
  ([GEP-08](https://github.com/gardener/enhancements/blob/main/geps/0008-shoot-apiserver-via-sni/README.md)).
  A Gateway with `TLSRoute` gives the same with standard API objects.
- **The advertise address keeps Kubernetes semantics.** Pointing the
  `kubernetes` endpoint at an address every node owns locally (HyperShift's
  `172.20.0.1`, Gardener's per-shoot address) needs no packet rewriting, no
  dependence on how kube-proxy or the CNI program Services, and works before
  the CNI is up
  ([HyperShift haproxy](https://github.com/openshift/hypershift/blob/main/hypershift-operator/controllers/nodepool/apiserver-haproxy/haproxy.go),
  [Gardener apiserver-proxy](https://github.com/gardener/gardener/blob/master/pkg/component/networking/apiserverproxy/apiserver_proxy.go)).
- **TLS inside TLS needs no gateway extensions.** Gardener's proxy adds a
  PROXY protocol header or HTTP CONNECT and needs filters on its Istio
  gateway ([GEP-30](https://github.com/gardener/enhancements/blob/main/geps/0030-apiserver-proxy/README.md));
  an outer TLS session with its own SNI is routed by any Gateway that
  supports `TLSRoute` termination, and Segla's node agent carries in-cluster
  API traffic this way in production today.
- **Konnectivity is upstream** and the API server's own mechanism for
  reaching nodes; one server per replica on a socket keeps that hop local.

## Rejected

- A dedicated IP or load balancer per tenant as the default — fine for a
  few tenants, the cost driver at hundreds; kept as `loadBalancer`.
- A port per tenant on a shared address — not 443, and every node network
  needs a firewall rule per tenant.
- Rewriting the `kubernetes` Service with `--endpoint-reconciler-type=none`
  and hand-written EndpointSlices — the advertise address gives the same
  result with the default reconciler.
- Injecting `KUBERNETES_SERVICE_HOST` into pods with a mutating webhook — a
  webhook in every tenant, and clients that dial the ClusterIP still need the
  proxy.
- A VPN between the control plane and the nodes — konnectivity is enough and
  needs no inbound port.

## Consequences

- Agents share the passthrough listener on 443: the konnectivity agent sends
  its `--proxy-server-host` as SNI
  ([hard way, spike c](../hard-way.md#c-spike-does-the-konnectivity-agent-send-sni)).
  Go sends no SNI for an IP address
  ([`hostnameInSNI`](https://github.com/golang/go/blob/go1.26.10/src/crypto/tls/handshake_client.go#L1305-L1315)),
  so agents dial `konnectivity.<cp>` by name.
- `apiserver-proxy` is a Brygga image (a small Go binary) that runs
  privileged on every node (`NET_ADMIN` for the address); its scope is
  exactly one address and one listener, and it is in the inventory like
  every other in-tenant object.
- The tunnel listener adds a second TLS handshake to in-cluster API
  connections, not to kubelets or users; the phase 2 gate measures the
  overhead.
- Changing `spec.endpoint` after creation is not supported in `v1alpha1`;
  moving between modes changes the address in every kubeconfig and node.

## Code

none yet.
