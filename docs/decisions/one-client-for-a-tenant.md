# Every request to a tenant goes through `internal/tenantclient`, and the data of its access Secrets is read by `resourceVersion`

Decided 2026-11-18.
Built; `internal/tenantclient` and `internal/secretcache` hold it.

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
- **The data of the Secrets of a tenant's access is read from memory for as long as their `resourceVersion` stays.**
  `internal/secretcache` is a `client.Reader` that the operator builds once, and that Tenant, `health`, `health-nodes` and `DialEtcd` read through: each reconciler has it as its field `Secrets`, so that `Reader` keeps meaning a live read ([conditions](../../.claude/rules/conditions.md)).
  A `Get` of a `*corev1.Secret` reads its metadata from the manager's cache.
  When the cache holds it at the UID and `resourceVersion` of the copy in memory, the reader returns a deep copy of that copy and sends nothing.
  Otherwise it reads the Secret from the API server and returns that, and keeps the result only when the cache, read again under the lock that guards the copies, holds that same UID and `resourceVersion`.
  A Secret the cache does not hold, missing or unlabelled or deleted, is read from the API server every time and its copy dropped.
  The informer's delete events, tombstones included, drop a copy.
  Every other `Get` and every `List` goes to the API server unchanged.
  PKI reads its own Secrets from the API server, since it writes them.
- **A phase takes its deadline from the pass's context, as a whole**: 10 s for Tenant's delivery of the tunnel's pair, 30 s for its apply phase, from the inventory's read through the rollout and a held pass's reads included, 10 s for PKI's three bindings, and 10 s for every dial and handshake of both.
  A pass that a deadline cuts stops where a failed request stops it, which the inventory allows anywhere ([tenant-resources-by-inventory](tenant-resources-by-inventory.md#consequences)), and the status write keeps the pass's context, with the 20 s of the minute that Tenant's two phases leave it.

## Why

- **Three controllers built a client three ways.**
  PKI built a client of client-go's typed set for each pass with no timeout; Tenant built a controller-runtime client with a dynamic REST mapper, so each new client discovered the tenant's API before its first read, and a client, a transport and a mapper for each pass and twice in a gateway pass; Health kept a bounded one.
  The first two decoded whatever a tenant answered, whole, and logged its text, though the administrator of a tenant can put a server of their own in place of a built-in group, discovery included.
- **No kind Tenant writes needs discovery.**
  Its kinds are the render's, and a table holds each one's version, resource and scope.
- **A client that follows `Retry-After` is not a bound.**
  client-go sends a request up to ten times on a 429 or a 5xx that carries `Retry-After` ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L167)), each wait as long as the server says; a request now goes once and its pass returns the error, which controller-runtime requeues with its backoff, 5 ms doubling to 1,000 s ([controller.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/controller/controller.go#L275), [default_rate_limiters.go](https://github.com/kubernetes/client-go/blob/v0.37.1/util/workqueue/default_rate_limiters.go#L50-L56)).

- **A Secret's data read from memory is as old as the cache.**
  A copy is kept only at the UID and `resourceVersion` the cache holds, so a copy from memory is never older than the cache's view, which lags the API server by the time its watch takes to deliver a write.
  A caller that reads an older copy after a newer read from the API server, which is not kept, costs a client rebuilt for the older kubeconfig within that lag and nothing more.
  None of the four readers watches Secrets, so none passes on a write: each catches up on its next periodic pass, 5 s for `health` and `health-nodes`, a minute for the sampler, 10 minutes for Tenant, or on the pass that another object's event brings, which can still read the previous copy.
  The content a lagging copy can serve is one its writer still honours: PKI renews a leaf at two thirds of its life and keeps a replaced CA in the bundle ([staged-credential-rotation](staged-credential-rotation.md)), a generation of etcd's Secrets is immutable and the next has Secrets of new names, and a Secret the cache does not hold yet is read from the API server.
  A wait is never decided on a copy: where a copy would make Tenant, `health` or `health-nodes` wait or report a Secret unusable, because another owner keeps it, this `ControlPlane` does not control it or it holds no certificate or key pair, the pass reads that Secret from the API server and decides on what it holds.
  A pass that waits sends that read, and a tenant at rest never does.
- **At rest the operator sends the API server no read of a Secret.**
  They were its only requests, 4.16 for each tenant in 5 s, 833 a second at 1,000 tenants ([operations](../operations.md#scale)), and the cache already knows each Secret's `resourceVersion`, since PKI's `Owns(&corev1.Secret{}, builder.OnlyMetadata)` holds the metadata of the Secrets under the operator's label selector.

## Rejected

- **A client shared by the controllers:** its cap would couple them, one controller's slow tenant holding the places another's reads wait for.
- **Discovery under bounds:** no kind Tenant writes needs it, and it would give a tenant's server a say in the version and scope Lodia writes at.
- **A full informer of Secrets for the three Secrets:** it needs a label of its own on them to hold only those, a second informer of the kind beside PKI's of metadata, and every cached `Get` of a Secret answered from a subset.

## Consequences

- A tenant that answers 429 or 5xx with `Retry-After` is no longer waited for inside a pass: the pass fails, and the next one is the retry.
- A tenant keeps up to four idle connections, one of each client, and Tenant's and PKI's close 90 s after a pass, where their client of each pass left about 7 for each tenant after its creation ([operations](../operations.md#scale)).
- An answer of a tenant is read within the bounds of [operations](../operations.md#capacity) whichever controller reads it.
- The copies of a tenant's five Secrets, the three of its access and the two `DialEtcd` reads, hold 13 KiB of live heap on envtest's PKI, 64 KiB by the rule of [Capacity](../operations.md#capacity) (`TestMeasureWhatTheCopiesOfATenantsAccessSecretsHold`).
- A tenant that is deleted leaves no copy, and a Secret the cache lacks is read from the API server each time, as it was before.

## Code

- `internal/tenantclient` — `client.go` builds a client from a `rest.Config` and keeps the clients of a controller; `bounded.go` is the cap on a response, its headers and its connections; `read.go` sends a request once and reads a failed answer; `fields.go` holds the bounds of what every reader keeps.
- `internal/secretcache` — the reader, whose tests count the reads that reach the API server for a hit, a changed `resourceVersion`, a recreated Secret, a Secret outside the cache, a failed read, a read newer than the cache and one the cache overtook, a delete and a tombstone and a cancelled context, and, against envtest's informer, a delete in the cluster; `internal/operator` builds it once, in `newControllers`, whose test finds Tenant, `health`, `health-nodes` and the three dials of the etcd, the Datastore controller's, the Backup reconciler's and the sampler's, reading through it.
- `internal/controller/tenant` — `table.go` is the table of kinds, `api.go` reaches the tenant by it, `reads.go` holds what Tenant keeps of an answer; `table_test.go` ties the table to the goldens of the render, and `hijacked_internal_test.go` puts a server of the test's own where the tenant's is.
