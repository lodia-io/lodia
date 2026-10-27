# Control planes are exposed through a shared Gateway by SNI; pods reach their API through a node-local proxy and a fixed advertise address

Decided 2026-10-25; the tunnel's shape decided 2026-10-26, and hardened by
its red team on 2026-10-27. The `loadBalancer` and `nodePort` modes are
built; `gateway`, the in-cluster `apiserver-proxy` and the tunnel come with
phase 2's wave 1b, Gateway ([roadmap](../roadmap.md)), and their contracts
are in `api/v1alpha1`.

## Decision

**Three ways out, one default.** `spec.endpoint` is one of:

| mode | what Lodia creates | public endpoint | in-cluster `kubernetes` Service |
|---|---|---|---|
| `gateway` (default) | three passthrough `TLSRoute`s on the platform's Gateway, all to the Service `<cp>-kube-apiserver`: `hostnames.apiServer` → the API server, `hostnames.konnectivity` → konnectivity-server, `hostnames.tunnel` → the tunnel sidecar | `https://<hostnames.apiServer>:443` | endpoints `240.0.0.1:6443`, the advertise address, served on every node by `apiserver-proxy` |
| `loadBalancer` | one Service of type LoadBalancer: 443 and 6443 → the API server, 8132 → konnectivity | `https://<LB address>:443` | endpoints `<LB address>:6443`, no proxy |
| `nodePort` | one Service of type NodePort with the same ports | `https://<a management node>:<port>` | endpoints `<node>:<port>`; development only |

**The `kubernetes` Service's endpoints.** In every mode the API server runs
with `--endpoint-reconciler-type=none` and writes no `kubernetes` endpoints.
Workloads publishes the endpoint of the table's last column as
`status.endpoint.inCluster`, and Tenant writes the Endpoints and the
EndpointSlice from it, as HyperShift does
([config.go](https://github.com/openshift/hypershift/blob/v0.1.86/control-plane-operator/controllers/hostedcontrolplane/v2/kas/config.go#L211),
[reconcile.go](https://github.com/openshift/hypershift/blob/v0.1.86/control-plane-operator/hostedclusterconfigoperator/controllers/resources/kas/reconcile.go#L11-L52)).
Every API server replica serves that address, so no rollout changes them,
and no controller rewrites them: the mirroring controller leaves Endpoints
labelled `skip-mirror` alone, and the EndpointSlice controller skips a
Service without a selector
([endpointslicemirroring_controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/endpointslicemirroring/endpointslicemirroring_controller.go#L314-L318),
[endpointslice_controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/endpointslice/endpointslice_controller.go#L398-L402)).

**Rollouts.** Every endpoint above reaches the API server through something
that follows its pods' readiness, so a stopping replica keeps serving while
it leaves them: it fails `/readyz` at once and serves for 15 more seconds
(`--shutdown-delay-duration=15s`), then shuts down, which sends each HTTP/2
client a GOAWAY that moves its next requests to a new connection, and
drains its watches for up to 10 seconds
(`--shutdown-watch-termination-grace-period=10s`), as Gardener runs it
([deployment.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/component/apiserver/deployment.go#L51-L52)).
A request that reaches it on a connection still open, as it starts to
drain, is answered with `503` and `Retry-After`
([waitgroup.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/server/filters/waitgroup.go#L79-L96)),
which client-go follows. `--shutdown-send-retry-after` stays off, and
`apiServer.extraArgs` refuses it: it answers every request on an open
connection during the drain with `429` and `Retry-After`
([with_retry_after.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/server/filters/with_retry_after.go#L55-L105)),
which costs each such client a retry a second later.

**Gateway prerequisites.** Gateway API ≥ 1.5, where `TLSRoute` graduated
to the Standard channel
([1.5-CHANGELOG.md](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/CHANGELOG/1.5-CHANGELOG.md#L16-L20)),
and an implementation that supports `TLSRoute` in passthrough, its core
feature
([tlsroute.go](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/pkg/features/tlsroute.go#L25-L51)):
no `ListenerSet`, no `TLSRouteModeTerminate` and no `TLSRouteModeMixed`. The
platform owns the Gateway: a `TLS` listener in `Passthrough` mode on 443 for
a wildcard hostname, routes allowed from tenant namespaces by label. Lodia
only attaches. Workloads reports what stops it in `APIServerReady`:
`GatewayUnsupported` when the management cluster serves no `TLSRoute`, which
the operator detects when it starts, or the parent Gateway has no listener
in `TLS` `Passthrough` mode that admits `TLSRoute`s; `GatewayNotFound` when
the parent does not exist; `RouteNotAccepted` when a route is not
`Accepted` and `ResolvedRefs` at its generation, the Gateway's reason in the
message; `HostnameConflict` below.

**In-cluster access (the `gateway` mode).** A connection from a pod to the
`kubernetes` ClusterIP carries no SNI, and every tenant's
`kubernetes.default.svc` looks the same, so the shared Gateway cannot route
it. Lodia wraps it in a TLS session of its own, with the node half Segla's
agent already runs in production:

1. The API server runs with `--advertise-address` set to
   `spec.apiServer.advertiseAddress`, or the default below, which Workloads
   publishes as `status.endpoint.inCluster`, so the `kubernetes` Service's
   endpoint is that address and 6443. EndpointSlices refuse unspecified,
   loopback and link-local addresses
   ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/apis/core/validation/validation.go#L8746-L8766)),
   so the address is one a network could use. A node that holds an address
   on its loopback answers ARP for it on its LAN by default
   ([ip-sysctl.rst](https://github.com/torvalds/linux/blob/v6.17/Documentation/networking/ip-sysctl.rst#L2063-L2075)),
   which the tunnel's red team reproduced, so a private address such as
   HyperShift's `172.20.0.1`
   ([constants.go](https://github.com/openshift/hypershift/blob/v0.1.86/support/config/constants.go#L28-L29))
   can answer for a neighbour's gateway. The IPv4 default is therefore
   `240.0.0.1`, from `240.0.0.0/4`, reserved and never forwarded
   ([RFC 6890](https://www.rfc-editor.org/rfc/rfc6890#section-2.2.2)), where
   Gardener's advertise addresses come from too
   ([types_constants.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/apis/core/v1beta1/constants/types_constants.go#L964-L965),
   [kubeapiserverexposure.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/gardenlet/operation/botanist/kubeapiserverexposure.go#L229-L236)):
   `DefaultAdvertiseAddress` in `api/v1alpha1/names.go`. IPv6 has no
   default: a unique local address must come from a /48 whose global ID is
   random, never a well-known number
   ([RFC 4193](https://www.rfc-editor.org/rfc/rfc4193#section-3.2)), the
   schema cannot draw one, and Lodia never writes `spec`, so a
   `ControlPlane` whose first service range is IPv6 names its address,
   from a /48 its platform drew once; Lodia drawing one per `ControlPlane`,
   kept beside its CAs, can come later without breaking a manifest
   (`todo.md`). The schema refuses an address that is
   not unicast, not in canonical form, of another family than the first
   service range, or inside the tenant's service or pod ranges, the default
   included, and refuses every change of it: the address is in every node's
   loopback, the endpoints and the serving certificate. The setup on the node
   refuses an address that is on-link on any of its interfaces, and says so.
2. The Tenant controller runs `apiserver-proxy` as a `hostNetwork`
   DaemonSet in `kube-system` (priority `system-node-critical`, tolerating
   not-ready nodes, so it starts before the CNI), in two containers of one
   image, `lodia-tunnel`: `addr`, an init container and a restartable
   sidecar with `NET_ADMIN` and no listener, puts the address on the node's
   loopback and keeps it there; `proxy`, as UID 65532 with no capabilities
   and a read-only root filesystem, listens on the address and 6443. The pod
   mounts no ServiceAccount token; it reads `kube-root-ca.crt` as a
   ConfigMap volume.
3. For each connection `proxy` opens TLS to `status.endpoint.tunnel`
   with the SNI `hostnames.tunnel`, verifying the server against the cluster
   CA, presents the tunnel client certificate, and copies the pod's bytes
   inside. The pod's own TLS session with the API server travels untouched;
   the API server's serving certificate carries the `kubernetes` ClusterIP,
   the first address of the tenant's service range, as a SAN, as always.
4. The Gateway passes the outer session through by its SNI, a third
   passthrough `TLSRoute`, to the `tunnel` port of the API server's
   Service. There a native sidecar of every API server pod, `lodia-tunnel
   serve`, terminates it with `<cp>-tunnel-serving`, issued by PKI from the
   cluster CA, requires a client certificate of the tenant's tunnel CA, and
   splices the inner stream to `127.0.0.1:6443`. It counts only
   authenticated sessions against its cap, and bounds the handshakes still
   pending.

kube-proxy, or any CNI that replaces it, sees a normal Service with a normal
endpoint; nothing on the node rewrites packets. Kubelets and kube-proxy
themselves use the public endpoint by name, so they never need the proxy.

**The tunnel's client.** The outer session is mutual TLS. A tunnel CA per
tenant, a CA per purpose
([staged-credential-rotation](staged-credential-rotation.md)), signs a
short-lived client certificate: PKI writes it as `<cp>-tunnel-client`, Tenant
delivers it into a `kube-system` Secret for `apiserver-proxy`, which the
kubelet fetches through the public endpoint, and Health presents it when it
probes the tunnel. Neither existing CA fits: kubelets accept the cluster CA
for client authentication, and the API server accepts the client CA. Without
it, anyone who reaches the Gateway could hold the sidecar's sessions and
lock every pod of the tenant out of its API. The names are in
`api/v1alpha1/names.go`: `TunnelServingSecret`, `TunnelCASecret` and
`TunnelClientSecret`.

**One image.** `lodia-tunnel` is a Go binary on `crypto/tls`, built by ko
like `lodia-signer`, that reloads its certificate through `GetCertificate`;
its modes `serve`, `addr` and `proxy` are the sidecar and the node's two
containers. The operator takes it as `--tunnel-image`, which the chart sets
from `tunnelImage`, and Workloads and Tenant render it.

**One owner per hostname.** Two `TLSRoute`s that claim one SNI on a shared
listener are not a conflict to Gateway API, which gives the older the
traffic
([gateway_types.go](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/apis/v1/gateway_types.go#L459-L469)),
but Envoy Gateway v1.9.3 renders one filter chain per route
([route.go](https://github.com/envoyproxy/gateway/blob/v1.9.3/internal/gatewayapi/route.go#L2433-L2461)),
Envoy refuses a listener with two chains that match alike
([filter_chain_manager_impl.cc](https://github.com/envoyproxy/envoy/blob/v1.39.5/source/common/listener_manager/filter_chain_manager_impl.cc#L188-L194)),
and the tunnel's red team saw the shared listener stop taking every
tenant's changes, and vanish when Envoy restarted. So Workloads applies none
of a `ControlPlane`'s three routes while one of its hostnames is held by
another route on the same parent, or named in another `ControlPlane`'s spec
in any namespace, and reports `HostnameConflict`, naming the holder. A
hostname belongs to the `ControlPlane` that names it first, by
`creationTimestamp` and then namespace and name, so neither a squatter's
route nor the gap while a route is recreated takes it. Workloads creates the
routes one at a time per parent Gateway, reading the routes again without
the cache just before it creates one, and never deletes and recreates its
own. The schema refuses three hostnames that are not distinct. Only Lodia
writes `TLSRoute`s in the namespaces the listener admits; a platform may
bind each hostname to its namespace with a `ValidatingAdmissionPolicy`.

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
static count and dial only for a server they lack. Both ends ping after 30
seconds without traffic (`--keepalive-time=30s`), as often as the server
allows
([server.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/server/app/server.go#L511-L515)),
so no idle timeout on the way cuts a tunnel.
Agents run as a DaemonSet on the pod network
with `--agent-identifiers=ipv4=$(HOST_IP)` and the server tries `destHost`
before `default`, so traffic for a kubelet goes through the agent on its own
node
([konnectivity setup](https://kubernetes.io/docs/tasks/extend-kubernetes/setup-konnectivity/),
[server options](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/server/app/options/options.go),
[agent options](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/agent/app/options/options.go)).
The konnectivity version follows the Kubernetes minor (proxy 0.NN for
Kubernetes 1.NN, [README](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/README.md#L37-L40)).
In gateway mode the agents resolve the public names through the node
(`dnsPolicy: Default`) and count the servers' leases through a kubeconfig to
the public endpoint, since an agent with `--count-server-leases` dials no
server before its lease informer has synced
([server.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/agent/app/server.go#L145-L174)),
so that counting does not depend on the path through the node's proxy.

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
  `172.20.0.1`, Gardener's per-shoot address in `240.0.0.0/8`) needs no
  packet rewriting, no dependence on how kube-proxy or the CNI program
  Services, and works before the CNI is up
  ([HyperShift haproxy](https://github.com/openshift/hypershift/blob/main/hypershift-operator/controllers/nodepool/apiserver-haproxy/haproxy.go),
  [Gardener apiserver-proxy](https://github.com/gardener/gardener/blob/master/pkg/component/networking/apiserverproxy/apiserver_proxy.go)).
- **Endpoints a rollout cannot empty.** The default `lease` reconciler keeps
  one record per advertise address, and every replica of a Lodia control
  plane advertises the same one. A stopping replica deletes that record as
  soon as it gets SIGTERM and, finding no record left, empties the
  endpoints, until a survivor's next pass up to 10 seconds later
  ([lease.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/reconcilers/lease.go#L227-L234),
  [instance.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/instance.go#L373-L376)).
  Every rollout stops a replica, a single one included: on kind with the
  lease reconciler, a tenant's `kubernetes` EndpointSlice was empty for 3
  seconds and pods inside it were refused; with `none` it never was
  (`TestPhase1Gate`, [test/e2e](../../test/e2e/README.md)).
- **A rollout loses no request.** Rolled on kind under 20 requests a second
  from outside the tenant and 20 from inside it, half on new connections,
  half on reused ones, with clients that follow only a `Retry-After`, once,
  the API server lost none with the flags above; with
  `--shutdown-send-retry-after` too, each reused connection was told to
  retry once (`429`), and without the shutdown delay requests were refused
  and timed out.
- **TLS inside TLS needs nothing but core Gateway API.** Gardener's proxy
  adds a PROXY protocol header or HTTP CONNECT and needs filters on its Istio
  gateway ([GEP-30](https://github.com/gardener/enhancements/blob/main/geps/0030-apiserver-proxy/README.md));
  an outer TLS session with its own SNI is routed by any Gateway that
  supports `TLSRoute` in passthrough, Segla's node agent carries in-cluster
  API traffic this way in production today, and konnectivity-server sits
  behind SNI beside the API server in Kamaji and HyperShift as it does in
  Lodia. The whole chain ran on kind behind Envoy Gateway 1.9.3 and Istio
  1.31.3, where ending the outer session in the sidecar cost 0.05 to 0.08
  ms more at p99 on reused connections than ending it on the Gateway.
- **No tenant secret reaches the Gateway, and no tenant can starve
  another.** Every certificate stays per tenant, issued from its own CAs and
  never read by the Gateway controller; the client certificate keeps
  strangers from the sidecar, and each tenant's three routes are three
  filter chains of their own.
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
- Agents with `--sync-forever` — every agent dials, at a TokenReview each,
  every second for as long as it runs
  ([clientset.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/agent/clientset.go#L227-L236)).
- Injecting `KUBERNETES_SERVICE_HOST` into pods with a mutating webhook — a
  webhook in every tenant, and clients that dial the ClusterIP still need the
  proxy.
- A VPN between the control plane and the nodes — konnectivity is enough and
  needs no inbound port.
- Ending the tunnel's outer session on the Gateway, with a `ListenerSet`
  and a `TLSRoute` in `Terminate` mode — sharing port 443 with the
  passthrough routes needs `TLSRouteModeMixed`, which is experimental, and a
  Gateway without it marks every listener on the port conflicted and fails
  every route there, every tenant's
  ([GEP-2643](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/geps/gep-2643/index.md#L407-L408),
  [#L540](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/geps/gep-2643/index.md#L540));
  Lodia could not tell beforehand, since Envoy Gateway leaves
  `supportedFeatures` empty
  ([gatewayclass.go](https://github.com/envoyproxy/gateway/blob/v1.9.3/internal/gatewayapi/status/gatewayclass.go#L33-L36)).
- `172.20.0.1` and `fd00::1`, HyperShift's defaults, as Lodia's — networks
  use both ranges, and a node answers ARP for the address on its loopback.
- An IPv6 default drawn once per installation and kept in the operator's
  configuration — neither the configuration nor `status` moves with a
  `ControlPlane`, and the address may never change; a ULA fixed in Lodia's
  code would be the well-known number RFC 4193 rules out.
- The proxy built on Envoy, HAProxy or ghostunnel, or in Rust — a Go binary
  on `crypto/tls` is enough, and the contract (Service, port, certificate,
  SNI) stays if G6 or the metrics ever show the proxy as a bottleneck.
- `NET_ADMIN` or root in the process that listens — a compromised proxy
  would own the node's networking, every other pod's traffic included;
  Gardener and HyperShift hold the address with a process that takes no
  network input too
  ([apiserver_proxy.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/component/networking/apiserverproxy/apiserver_proxy.go#L299-L386),
  [setup-apiserver-ip.sh](https://github.com/openshift/hypershift/blob/v0.1.86/hypershift-operator/controllers/nodepool/apiserver-haproxy/assets/setup-apiserver-ip.sh#L3-L4)).

## Consequences

- Agents share the passthrough listener on 443: the konnectivity agent sends
  its `--proxy-server-host` as SNI
  ([hard way, spike c](../hard-way.md#c-spike-does-the-konnectivity-agent-send-sni)).
  Go sends no SNI for an IP address
  ([`hostnameInSNI`](https://github.com/golang/go/blob/go1.26.10/src/crypto/tls/handshake_client.go#L1305-L1315)),
  so agents dial `konnectivity.<cp>` by name.
- `apiserver-proxy` runs on every node: `NET_ADMIN` in its `addr` container
  alone, which opens no listener, and no capability in `proxy`; its scope is
  exactly one address and one listener, and it is in the inventory like
  every other in-tenant object.
- The tunnel adds a second TLS session to every in-cluster API connection,
  not to kubelets or users; the phase 2 gate measures the overhead.
- A `ControlPlane` keeps its advertise address for life, and two whose
  hostnames collide stop the younger one's routes, not the Gateway.
- Changing `spec.endpoint` after creation is not supported in `v1alpha1`;
  moving between modes changes the address in every kubeconfig and node.
- A `kubernetes` Endpoints or EndpointSlice edited or deleted inside the
  tenant comes back within Tenant's apply period, not within the lease
  reconciler's 10 seconds.

## Code

- `api/v1alpha1` — the contracts of the gateway mode: `GatewayHostnames`
  with the tunnel required and the three distinct, `APIServer.AdvertiseAddress`
  with its rules and `DefaultAdvertiseAddress`, `EndpointStatus.Tunnel`, the
  tunnel's Secrets in `names.go` and the reasons of `APIServerReady` in
  `conditions.go`; `test/api/validation_test.go` holds the rules.
- `internal/controller/workloads` — the Service of the `loadBalancer` and
  `nodePort` modes, `status.endpoint` with `inCluster`, the API server's
  advertise address and `--endpoint-reconciler-type=none`, and
  konnectivity-server beside it as a native sidecar on a Unix socket.
- `internal/controller/tenant` — the `kubernetes` Endpoints and EndpointSlice
  in `manifests/endpoints.yaml`, and the agents' flags in
  `manifests/konnectivity.yaml`; `render_test.go` and `tenant_test.go` hold
  the objects, and `TestPhase1Gate` in `test/e2e` rolls the API server under
  requests from outside and inside the tenant.
