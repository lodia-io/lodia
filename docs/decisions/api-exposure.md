# Control planes are exposed through a shared Gateway by SNI; pods reach their API through a node-local proxy and a fixed advertise address

Decided 2026-10-25. The `loadBalancer` and `nodePort` modes are built;
`gateway`, the in-cluster `apiserver-proxy` and the tunnel are not.

## Decision

**Three ways out, one default.** `spec.endpoint` is one of:

| mode | what Lodia creates | public endpoint | in-cluster `kubernetes` Service |
|---|---|---|---|
| `gateway` (default) | `TLSRoute`s on the platform's Gateway: passthrough for `hostnames.apiServer` → the API Service and `hostnames.konnectivity` → the konnectivity Service; with `hostnames.tunnel`, a `ListenerSet` with a terminating TLS listener and a `TLSRoute` → the API Service | `https://<hostnames.apiServer>:443` | endpoints `172.20.0.1:6443`, served on every node by `apiserver-proxy` |
| `loadBalancer` | one Service of type LoadBalancer: 443 and 6443 → the API server, 8132 → konnectivity | `https://<LB address>:443` | endpoints `<LB address>:6443`, no proxy |
| `nodePort` | one Service of type NodePort with the same ports | `https://<a management node>:<port>` | endpoints `<node>:<port>`; development only |

**The `kubernetes` Service's endpoints.** In every mode the API server runs
with `--endpoint-reconciler-type=none` and writes neither the `kubernetes`
Endpoints nor its EndpointSlice
([none.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/reconcilers/none.go#L26-L53)).
Workloads publishes the endpoint of the table's last column as
`status.endpoint.inCluster`, and the Tenant controller writes both objects
from it with the port name `https`: the Endpoints labelled
`endpointslice.kubernetes.io/skip-mirror: "true"`, the EndpointSlice
`kubernetes` labelled `kubernetes.io/service-name: kubernetes` and
`endpointslice.kubernetes.io/managed-by: lodia.io`. HyperShift runs `none`
too and writes the two objects from outside, the Endpoints labelled
`skip-mirror` and the slice labelled with the service's name, both with the
port name `https`
([config.go](https://github.com/openshift/hypershift/blob/v0.1.86/control-plane-operator/controllers/hostedcontrolplane/v2/kas/config.go#L211),
[reconcile.go](https://github.com/openshift/hypershift/blob/v0.1.86/control-plane-operator/hostedclusterconfigoperator/controllers/resources/kas/reconcile.go#L11-L52)).
Every API server replica serves that address, so no rollout and no stopping
replica changes them. Nothing else rewrites them: the EndpointSlice mirroring
controller leaves Endpoints labelled `skip-mirror` alone and deletes only the
slices it manages
([endpointslicemirroring_controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/endpointslicemirroring/endpointslicemirroring_controller.go#L314-L318),
[L545-L551](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/endpointslicemirroring/endpointslicemirroring_controller.go#L545-L551)),
and the EndpointSlice and Endpoints controllers skip a Service without a
selector
([endpointslice_controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/endpointslice/endpointslice_controller.go#L398-L402),
[endpoints_controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/endpoint/endpoints_controller.go#L385-L389)).

**Rollouts.** Every endpoint above reaches the API server through something
that follows its pods' readiness, so a stopping replica keeps serving while
it leaves them: it fails `/readyz` at once and serves for 15 more seconds
(`--shutdown-delay-duration=15s`), then shuts its HTTP server down and drains
its watches for up to 10 seconds
(`--shutdown-watch-termination-grace-period=10s`), as Gardener runs it
([deployment.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/component/apiserver/deployment.go#L51-L52)).
Shutting down sends each HTTP/2 client a GOAWAY, and Go's client moves the
requests the server has not taken to a new connection
([h2_bundle.go](https://github.com/golang/go/blob/go1.26.8/src/net/http/h2_bundle.go#L4245),
[L5448-L5478](https://github.com/golang/go/blob/go1.26.8/src/net/http/h2_bundle.go#L5448-L5478),
[L8198-L8231](https://github.com/golang/go/blob/go1.26.8/src/net/http/h2_bundle.go#L8198-L8231);
Go 1.26.8 builds kube-apiserver v1.37.1). `--shutdown-send-retry-after` stays
off, and `apiServer.extraArgs` refuses it: it keeps the server listening
after the delay and answers every request on an open connection with `429`
and `Retry-After`
([with_retry_after.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/server/filters/with_retry_after.go#L55-L105)),
which only a client that retries gets through.

**Gateway prerequisites.** Gateway API ≥ 1.5, where `TLSRoute` and
`ListenerSet` are in the Standard channel; an implementation that reports
`TLSRoute` (passthrough) and, for the tunnel, `TLSRouteModeTerminate`, plus
`TLSRouteModeMixed` when the tunnel shares port 443 with the passthrough
listener ([TLSRoute](https://gateway-api.sigs.k8s.io/reference/api-types/tlsroute/),
[ListenerSet](https://gateway-api.sigs.k8s.io/reference/api-types/listenerset/)).
The platform owns the Gateway: a `TLS` listener in `Passthrough` mode on 443
for a wildcard hostname, routes allowed from tenant namespaces by label, and
`allowedListeners` that admits tenant `ListenerSet`s. Lodia only attaches.
Missing features turn `APIServerReady` False with reason `GatewayUnsupported`
(phase 2).

**In-cluster access (the `gateway` mode).** A connection from a pod to the
`kubernetes` ClusterIP carries no SNI, and every tenant's
`kubernetes.default.svc` looks the same, so the shared Gateway cannot route
it. Lodia uses the shape HyperShift ships, with the outer hop Segla's agent
already runs in production:

1. The API server runs with `--advertise-address=172.20.0.1` (configurable
   per `ControlPlane`, never inside the tenant's node, pod or service
   ranges), which Workloads publishes as `status.endpoint.inCluster`, so the
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
it in the pod as a native sidecar that stops only after the API server, in
gRPC mode on a Unix socket (`--uds-name`, `--server-port=0`), the egress
selector sending only `cluster` traffic through it; agents authenticate with
a projected ServiceAccount token
(`--agent-namespace`, `--agent-service-account`, `--authentication-audience`),
which the server checks with TokenReviews as `system:konnectivity-server`
bound to `system:auth-delegator` and to lease verbs in `kube-system`; the
server's lease controller is on and agents count servers from leases
(`--count-server-leases`), so agents hold a tunnel to every replica without a
static count, and dial only while they hold fewer tunnels than there are
leases
([clientset.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/agent/clientset.go#L268-L271)).
Both ends send a keepalive after 30 seconds without traffic
(`--keepalive-time=30s`) instead of their default hour
([agent](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/agent/app/options/options.go#L285),
[server](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/server/app/options/options.go#L409)),
so that no idle timeout on the way cuts an idle tunnel; 30 seconds is as
often as the server lets an agent ping
([server.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/server/app/server.go#L511-L515)).
Agents run as a DaemonSet on the pod network
with `--agent-identifiers=ipv4=$(HOST_IP)` and the server tries `destHost`
before `default`, so traffic for a kubelet goes through the agent on its own
node
([konnectivity setup](https://kubernetes.io/docs/tasks/extend-kubernetes/setup-konnectivity/),
[server options](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/server/app/options/options.go),
[agent options](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/agent/app/options/options.go)).
The konnectivity version follows the Kubernetes minor (proxy 0.NN for
Kubernetes 1.NN, [README](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/README.md#L37-L40)).

**Nothing inbound on a node.** The API server reaches kubelets, webhooks and
aggregated APIs through the agents' tunnels. Lodia's defaults bind what it
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
- **Endpoints a rollout cannot empty.** The default `lease` reconciler keeps
  one record per advertise address
  ([lease.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/reconcilers/lease.go#L96-L128)),
  and every replica of a Lodia control plane advertises the same one, in
  every mode. A stopping replica deletes that record at the very start of
  its shutdown and rewrites the endpoints from the records left, which are
  none
  ([instance.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/instance.go#L373-L376),
  [genericapiserver.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go#L719-L727),
  [lease.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/reconcilers/lease.go#L227-L234),
  [L334-L344](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/reconcilers/lease.go#L334-L344));
  a survivor writes them again at its next pass, up to 10 seconds later
  ([instance.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/instance.go#L115-L117)).
  Every rollout stops a replica, a single one included: rolled on kind with
  the lease reconciler and no shutdown delay, a tenant's `kubernetes`
  EndpointSlice was empty for 3 seconds and pods inside it were refused,
  while with `none` and the endpoints Tenant writes it never was
  (the rollout check of `TestPhase1Gate`, [test/e2e](../../test/e2e/README.md)).
- **A rollout loses no request.** Rolled on kind under 20 requests a second
  from outside the tenant and 20 from inside it, half on new connections,
  half on reused ones, with clients that retry nothing, the API server lost
  none with the flags above; with `--shutdown-send-retry-after` too, each
  reused connection took one `429`, and without the shutdown delay requests
  were refused and timed out.
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
- The default `lease` endpoint reconciler — every replica advertises the
  same address, so a stopping replica empties the `kubernetes` endpoints
  until a survivor's next pass (Why).
- Agents with `--sync-forever` — a dial that reaches a server an agent
  already holds resets its backoff, so every agent dials, and is
  authenticated with a TokenReview, every second for as long as it runs
  ([clientset.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/agent/clientset.go#L227-L236),
  [server.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/server/server.go#L887-L892));
  counting leases dials only for a server it lacks.
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
- `apiserver-proxy` is a Lodia image (a small Go binary) that runs
  privileged on every node (`NET_ADMIN` for the address); its scope is
  exactly one address and one listener, and it is in the inventory like
  every other in-tenant object.
- The tunnel listener adds a second TLS handshake to in-cluster API
  connections, not to kubelets or users; the phase 2 gate measures the
  overhead.
- Changing `spec.endpoint` after creation is not supported in `v1alpha1`;
  moving between modes changes the address in every kubeconfig and node.
- A `kubernetes` Endpoints or EndpointSlice edited or deleted inside the
  tenant comes back within Tenant's apply period, not within the lease
  reconciler's 10 seconds.

## Code

- `internal/controller/workloads` — the Service of the `loadBalancer` and
  `nodePort` modes, `status.endpoint` with `inCluster`, the API server's
  advertise address and `--endpoint-reconciler-type=none`, and
  konnectivity-server beside it as a native sidecar on a Unix socket.
- `internal/controller/tenant` — the `kubernetes` Endpoints and EndpointSlice
  in `manifests/endpoints.yaml`, and the agents' flags in
  `manifests/konnectivity.yaml`; `render_test.go` and `tenant_test.go` hold
  the objects, and `TestPhase1Gate` in `test/e2e` rolls the API server under
  requests from outside and inside the tenant.
