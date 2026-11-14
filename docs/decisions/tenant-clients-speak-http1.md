# Health's tenant clients speak HTTP/1.1, and net/http caps their connections

Decided 2026-11-14.
Built; `internal/controller/health` holds it.

## Decision

- Health's two clients of a tenant, `health`'s and `health-nodes`', set `h2` out of `NextProtos`, so that client-go configures no HTTP/2 for them ([http.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L196-L208)) and every request goes over HTTP/1.1, whatever the tenant's server offers.
- Each client's transport has `MaxConnsPerHost` equal to the most reads its pass has in flight at once: 2 for `health-nodes`, the two lists, and 7 for `health`, the lists of a held restore.
  net/http counts every connection of a host, dialing, in use and idle, and a dial past the cap waits for a connection to close or to be given back ([transport.go](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L214-L219)).
- An honest pass never waits on the cap, and a read that does ends at its own deadline: the argument and its tests are in [operations](../operations.md#capacity).

## Why

- **Over HTTP/2 a server decides what the client's pool keeps.**
  A stream the client cancels before the server sent a frame counts against the connection's stream limit until a PING is answered, so a server that allows one stream and answers none makes the client dial again for every read its deadline cuts.
  A GOAWAY on an idle connection makes x/net drop it from its pool, and it stays open for 90 s, out of the reach of the close of idle connections ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2502-L2514)).
  Each of those was found one round after another, and each needed a term, a count and a test of its own.
  A cap at the dialer that tolerated them all could not be argued to leave an honest tenant unblocked: seven lists over HTTP/2 reached 16 to 20 sockets at once, from dials in flight, duplicates x/net closes later and sockets the server closed first.
- **Over HTTP/1.1 the bound is the standard library's own.**
  `MaxConnsPerHost` is documented, counts the states of a connection the same way for every server, and holds whatever the server does.
- **Health gains nothing from HTTP/2.**
  It has no watch, at most two reads at once for `health-nodes` and seven for `health`, and reads the pages of a list one after another.

## Rejected

- A dialer of Health's own that caps the sockets of a client over HTTP/2: the honest demand of seven reads over HTTP/2 passes any cap derived from the pool.
- Closing a client's idle connections after a read fails, as an HTTP/2 client needs: over HTTP/1.1 net/http closes the connection of a read its deadline cuts, so the close keeps nothing out.
- HTTP/2 with `StrictMaxConcurrentStreams`: client-go leaves it off and nothing in Lodia sets it, and with it the reads of a pass wait on one connection's stream limit, which the server sets.

## Consequences

- A held restore reads over seven TCP connections of a tenant where HTTP/2 used one, and `health-nodes` over two: a TLS handshake for each of them when it is first opened or after 90 s idle, and the margin of 2 MiB for each connection in the figures of [operations](../operations.md#capacity).
- A tenant at rest keeps at most 9 idle connections beside its summaries, 22.0 MiB, and a tenant whose server never answers keeps at most the cap of them open, whatever its cadence.
- A read that needs a connection while net/http has not yet counted one down waits for it, at most until its own deadline; a tenant whose connections a server closes after every answer, a kube-apiserver's `--goaway-chance` among them, may wait a moment.

## Code

- `internal/controller/health` — `health.go` sets `NextProtos` and gives each client the cap of its reconciler, `restoreReads` and `nodesReads`; `bounded.go` sets `MaxConnsPerHost` beside the cap on a response.
  `cap_internal_test.go` holds the protocol against a server that offers HTTP/2 first, the cap with the next read waiting and failing at its deadline, 40 passes against a server that closes after every answer and one that never answers, the pages of a list on the connections of the pages before them, the passes of both clients needing no more sockets than the cap against a server and a real kube-apiserver, and a changed kubeconfig.
