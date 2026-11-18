# Every request to a tenant goes through `internal/tenantclient`

Decided 2026-11-18.
Built; `internal/tenantclient` holds it.

## Decision

- **One package builds, keeps and uses every client of a tenant's API server** for PKI, Tenant, `health` and `health-nodes`: the transport of [tenant-clients-speak-http1](tenant-clients-speak-http1.md), a request sent once, an answer cut at 8 MiB and its headers at 64 KiB, and a failed answer read as a Status within bounds, or as a Status of Lodia's own that carries none of its text.
  `clientcmd.RESTConfigFromKubeConfig`, `client.New`, `kubernetes.NewForConfig`, `rest.HTTPClientFor` and `rest.UnversionedRESTClientForConfigAndClient` appear under `internal/controller` and `internal/operator` in tests only.
- **A caller reads an answer into the fields it keeps and nothing else**, with each array counted ([who-is-trusted](who-is-trusted.md)): no object of a tenant is decoded into an API type or into `unstructured`.
- **A controller keeps a client for each `ControlPlane`, rebuilt when the bytes of its kubeconfig change, and forgets it once the `ControlPlane` is gone.**
  Each controller has clients of its own, with `MaxConnsPerHost` equal to the most requests its pass has in flight: 7 for `health`, whose held restore lists seven kinds at once, 1 for `health-nodes`, Tenant and PKI, whose requests go one after another.
  A pass of PKI's bootstrap identity uses a client of its own, built from the `rest.Config` and closed after use.
- **Tenant addresses an object by path, from a table of the kinds it writes**: group and kind to version, resource and scope.
  Every kind of the render is in it, and one that leaves the render stays, so that the prune deletes what earlier revisions wrote.
  An inventory entry of a kind outside the table is of a kind no longer served, and an entry at another version is addressed at the table's.
  Writes are server-side applies as `lodia/tenant` that force, deletes carry the UID and `resourceVersion` the prune read and propagate in the background, and reads decode a UID, a `resourceVersion`, whether one of the `managedFields` entries is Lodia's apply, the first ten sorted names of the others, a workload's rollout fields and the inventory's entries, counted up to 256.
- **A request takes its deadline from its context**: 10 s for the delivery of the tunnel's pair and 30 s for the apply pass in Tenant, 10 s for PKI's bindings, and 10 s for every dial and handshake of both.

## Why

- **Three controllers built a client three ways.**
  PKI built a client of client-go's typed set for each pass with no timeout; Tenant built a controller-runtime client with a dynamic REST mapper, so each new client discovered the tenant's API before its first read, and a client, a transport and a mapper for each pass and twice in a gateway pass; Health kept a bounded one.
  The first two decoded whatever a tenant answered, whole, and logged its text, though the administrator of a tenant can put a server of their own in place of a built-in group, discovery included.
- **No kind Tenant writes needs discovery.**
  Its kinds are the render's, and a table holds each one's version, resource and scope.
- **A client that follows `Retry-After` is not a bound.**
  client-go sends a request up to ten times on a 429 or a 5xx that carries `Retry-After` ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L167)), each wait as long as the server says; a request now goes once and its pass returns the error, which controller-runtime requeues with its backoff, 5 ms doubling to 1,000 s ([controller.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/controller/controller.go#L275), [default_rate_limiters.go](https://github.com/kubernetes/client-go/blob/v0.37.1/util/workqueue/default_rate_limiters.go#L50-L56)).

## Rejected

- **A client shared by the controllers:** its cap would couple them, one controller's slow tenant holding the places another's reads wait for.
- **Discovery under bounds:** no kind Tenant writes needs it, and it would give a tenant's server a say in the version and scope Lodia writes at.

## Consequences

- A tenant that answers 429 or 5xx with `Retry-After` is no longer waited for inside a pass: the pass fails, and the next one is the retry.
- A tenant keeps up to four idle connections, one of each client, and Tenant's and PKI's close 90 s after a pass, where their client of each pass left about 7 for each tenant after its creation ([operations](../operations.md#scale)).
- An answer of a tenant is read within the bounds of [operations](../operations.md#capacity) whichever controller reads it.

## Code

- `internal/tenantclient` — `client.go` builds a client from a `rest.Config` and keeps the clients of a controller; `bounded.go` is the cap on a response, its headers and its connections; `read.go` sends a request once and reads a failed answer; `fields.go` holds the bounds of what every reader keeps.
- `internal/controller/tenant` — `table.go` is the table of kinds, `api.go` reaches the tenant by it, `reads.go` holds what Tenant keeps of an answer; `table_test.go` ties the table to the goldens of the render, and `hijacked_internal_test.go` puts a server of the test's own where the tenant's is.
