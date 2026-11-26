# Lodia's tenant clients speak HTTP/1.1 with no compression, and net/http caps their connections

Decided 2026-11-14.
Built; `internal/tenantclient` holds it, for Health, Tenant and PKI ([one-client-for-a-tenant](one-client-for-a-tenant.md)).

## Decision

- The four clients of a tenant, `health`'s, `health-nodes`', Tenant's and PKI's, set `h2` out of `NextProtos`, so that client-go configures no HTTP/2 for them ([http.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L196-L208)) and every request goes over HTTP/1.1, whatever the tenant's server offers.
- All set `DisableCompression`, so that net/http asks for no gzip and decodes none ([transport.go](https://github.com/golang/go/blob/go1.27.4/src/net/http/transport.go#L3021-L3038)).
- All follow no redirect, as the prober does, so that the one host of the kubeconfig is the only host they dial, whatever a group that the tenant's untrusted administrator serves answers ([who-is-trusted](who-is-trusted.md)): a redirected read is a failed read that names no byte of the `Location`.
- Each client's transport has `MaxConnsPerHost` equal to the most reads its pass has in flight at once: 1 for `health-nodes`, whose two lists go one after the other, 1 for Tenant and PKI, whose requests go one after another, and 7 for `health`, the lists of a held restore.
  net/http counts every connection of a host, dialing, in use and idle, and a dial past the cap waits for a connection to close or to be given back ([transport.go](https://github.com/golang/go/blob/go1.27.4/src/net/http/transport.go#L218-L223)).
- Each keeps one connection idle between passes, `MaxIdleConnsPerHost` of 1, and closes the others as they are given back; an idle connection closes after 90 s, client-go's `IdleConnTimeout` ([http.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L123-L124)), or when its tenant is forgotten.
- Each bounds a dial and a TLS handshake by the `Timeout` of its controller, `deliveryTimeout` of Tenant and `bindTimeout` of PKI, as the prober does: net/http dials on a context detached from the request's, and client-go lets a dial run 30 s and a handshake 10 s ([cache.go](https://github.com/kubernetes/client-go/blob/v0.37.1/transport/cache.go#L122)).
- Each cancels the request of an answer it did not read to its end before it closes the body, a read the cap cuts among them, so that net/http closes that connection before the close returns and drains nothing of the rest, unless its read loop had not reached its wait by then ([transport.go](https://github.com/golang/go/blob/go1.27.4/src/net/http/transport.go#L2628-L2643), [operations](../operations.md#capacity)); the prober's request asks to close ([request.go](https://github.com/golang/go/blob/go1.27.4/src/net/http/request.go#L215-L218)), since a probe reads no body and reuses no connection, so that net/http never drains it.
- An honest pass never waits on the cap for a connection that another of its reads holds: a read waits only for a count that no read uses, a dial still in flight or a connection that is going, and ends at its own deadline at the latest; the argument and its tests are in [operations](../operations.md#capacity).

## Why

- **Over HTTP/2 a server decides what the client's pool keeps.**
  client-go configures HTTP/2 through x/net v0.62.0, which under Go 1.27 wraps net/http's own transport ([transport_wrap.go](https://github.com/golang/net/blob/v0.62.0/http2/transport_wrap.go#L5)), and net/http keeps its HTTP/2 connections in a pool of their own, beside `MaxConnsPerHost` ([http2.go](https://github.com/golang/go/blob/go1.27.4/src/net/http/http2.go#L354-L364)).
  A stream the client cancels before the server sent a frame still counts against the connection's stream limit until a PING is answered ([transport.go](https://github.com/golang/go/blob/go1.27.4/src/net/http/internal/http2/transport.go#L1520-L1534), [L884](https://github.com/golang/go/blob/go1.27.4/src/net/http/internal/http2/transport.go#L884)), so a server that allows one stream and answers none makes the client dial again for every read its deadline cuts.
  A server may fill each stream's window, 4 MiB, while the client reads nothing ([L50](https://github.com/golang/go/blob/go1.27.4/src/net/http/internal/http2/transport.go#L50)), a term the figures would need for each read in flight.
  A GOAWAY on a connection with no stream now closes it ([L2697-L2701](https://github.com/golang/go/blob/go1.27.4/src/net/http/internal/http2/transport.go#L2697-L2701)).
  Under x/net's own transport, at go1.26, such behaviours were found one round after another, each needing a term, a count and a test of its own, and a cap at the dialer that tolerated them all could not be argued to leave an honest tenant unblocked: seven lists over HTTP/2 reached 16 to 20 sockets at once, from dials in flight, duplicates x/net closes later and sockets the server closed first.
- **Over HTTP/1.1 the bound is the standard library's own.**
  `MaxConnsPerHost` is documented, counts the states of a connection the same way for every server, and holds whatever the server does.
- **None of them gains anything from HTTP/2.**
  They have no watch, send one request at a time but for the seven lists of a held restore, and read the pages of a list one after another.
- **A redirect would take the cap's host and its certificate elsewhere.**
  `MaxConnsPerHost` counts per host, and a client that follows a redirect has a cap for every host the tenant's server names, 17 connections for `health`'s cap of 7 against five hosts, and sends its client certificate to each.
  A tenant's API server answers 502 in place of an aggregated server's redirect only while `--aggregator-reject-forwarding-redirect` is true, its default ([options.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/apiserver/options/options.go#L130), [upgradeaware.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/proxy/upgradeaware.go#L262-L274)), and Lodia does not pin the flag.
- **A gzip reader is more than the figures can count.**
  net/http's pool keeps a reader after the read, reset to an empty reader, which returns at the header before it resets the decompressor ([gunzip.go](https://github.com/golang/go/blob/go1.27.4/src/compress/gzip/gunzip.go#L103-L115), [L236-L240](https://github.com/golang/go/blob/go1.27.4/src/compress/gzip/gunzip.go#L236-L240), [transport.go](https://github.com/golang/go/blob/go1.27.4/src/net/http/transport.go#L3336-L3342)).
  The decompressor keeps its link tables and the buffered reader over the body, 44,960 bytes beside the 41,184 of the reader, the decompressor and its window: 86,144 in all, and with the buffered reader the response of a read the cap or a deadline cut, its head up to 2.5 MiB, until a second collection.
  Without compression no reader is made, and nothing of it needs a term, a part or a test.

## Rejected

- A dialer of Health's own that caps the sockets of a client over HTTP/2: the honest demand of seven reads over HTTP/2 passes any cap derived from the pool.
- Closing a client's idle connections after a read fails, as an HTTP/2 client needs: over HTTP/1.1 net/http closes the connection of a read its deadline cuts, so the close keeps nothing out.
- HTTP/2 with `StrictMaxConcurrentStreams`: client-go leaves it off and nothing in Lodia sets it, and with it the reads of a pass wait on one connection's stream limit, which the server sets.
- Letting net/http drain an answer not read to its end: each worker's figure would need a term for the cut reads of its last pass, whose responses and connections stay up to 50 ms into its next, and each such read reads up to 256 KiB and a byte more of an answer already refused ([transport.go](https://github.com/golang/go/blob/go1.27.4/src/net/http/transport.go#L2417-L2448)).
- Connections of Lodia's own through `Transport.NewClientConn`, since Go 1.26 ([go1.26.txt](https://github.com/golang/go/blob/go1.27.4/api/go1.26.txt#L163), [clientconn.go](https://github.com/golang/go/blob/go1.27.4/src/net/http/clientconn.go#L93-L112)): a client would hold exactly the connections it dialed and close a cut one itself, over HTTP/1.1 or over one HTTP/2 connection, at the price of a pool of Lodia's own, a wait within the read's deadline, the replacement of a dead connection, the idle close and the close when a tenant is forgotten, all of which `MaxConnsPerHost` and net/http's pool give now, and for HTTP/2 new terms in the figures.
  It is worth it once a client watches a tenant.

## Consequences

- An answer of a tenant's API server crosses the management cluster's network uncompressed.
  A page of 100 of the repository's kubelet Nodes is 750,385 bytes, and 20,839 to 23,707 gzipped, 32 to 36 times fewer, in twelve runs of `TestMeasureTheBytesOfAPageOf100KubeletNodesWithAndWithoutGzip` on envtest's kube-apiserver 1.35.0, which [operations](../operations.md#capacity) runs; a tenant of 5,000 Nodes reads 50 of them, 37.5 MB, each time `health-nodes` samples it, every 30 s.
- A held restore reads over seven TCP connections of a tenant where HTTP/2 used one, and dials six of them in every pass, since its client keeps one idle: a TLS handshake for each, about 0.6 ms of the operator's CPU in the fleet's measure ([operations](../operations.md#scale)), and the margin of 2 MiB for each connection in the figures of [operations](../operations.md#capacity).
- A tenant at rest keeps at most 2 idle connections, one of each of Health's clients, and Tenant's and PKI's add one each for 90 s after a pass, which [operations](../operations.md#capacity) counts beside its summaries, where the fleet measures 46 to 48 KiB of live heap and stacks for an idle connection; a tenant whose server never answers keeps at most the cap of them open, whatever its cadence.
- The connections of a client stay with the replica of a highly available tenant that they reached until they close, since `--goaway-chance` closes HTTP/2 connections only ([goaway.go](https://github.com/kubernetes/apiserver/blob/v0.37.1/pkg/server/filters/goaway.go#L67)): Health's reads go to the replicas its connections reached when they were dialed, not to each in turn.
  A read on such a connection reads what any replica would, since Health asks for no `resourceVersion` and a Get or a List with none is a most recent read ([api-concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions)).
- A read can find the cap reached by a connection that no read uses, a connection that is going or the dial of another read, and wait for it, at most until its own deadline.
  A dial is bound by `Timeout` and its TLS handshake by `Timeout` after it, as the prober's are, so a place is held for at most two Timeouts, the connect and the handshake; a read that waits for a place can fail at its deadline behind a dial that is slow, whether it ends in a connection or not, where its own dial would have been quick, and after a dial that fails it dials only then.

## Code

- `internal/tenantclient` — `client.go` sets `NextProtos`, `DisableCompression`, the dial and `CheckRedirect` of each client and takes the cap and the `Timeout` of its controller; `bounded.go` sets `MaxConnsPerHost`, `MaxIdleConnsPerHost` and `TLSHandshakeTimeout` beside the cap on a response, and `cappedBody.Close` cancels the request before it closes the body, which closes the connection of an answer not read to its end.
  `bounded_test.go` holds a read cut at the bound closing its connection, for a chunked answer 1 MiB and one 100 KiB past it, and a read to its end and an answer with no body closed unread leaving theirs to the next read.
- `internal/controller/health` — `health.go` and `nodes.go` give each client the cap and the `Timeout` of its reconciler, `restoreReads` and `nodesReads`, and `probe` in `health.go` sets `Close` on its request.
  `cap_internal_test.go` holds the protocol against a server that offers HTTP/2 first, the cap with the next read waiting and failing at its deadline, what a pass leaves open after a server closes it and after a deadline cuts it, the pages of a list on the connections of the pages before them, the one connection a pass leaves idle, the passes of both clients needing no more sockets than the cap against a server, a read waiting for a dial that is still in flight, the passes against a real kube-apiserver failing no read under the cap, and a changed kubeconfig.
  `settings_internal_test.go` holds the request that asks for no compression, a gzip answer sent unasked, a probe answered chunked and without `Connection: close`, a redirect refused, and a dial and a handshake held past `Timeout`.
