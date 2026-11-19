# A tenant's JWT authenticators are upstream's `JWTAuthenticator`, reached through the tenant's nodes and reloaded without a restart

Decided 2026-11-19.
Built; `api/v1alpha1/authentication_types.go`, `internal/authnconfig` and Workloads hold it.

## Decision

- **`spec.apiServer.authentication.jwt` is a copy of kube-apiserver's `apiserver.config.k8s.io/v1` `JWTAuthenticator`**, field for field, with Lodia's bounds and the CEL rules a schema can see ([field reference](../api/controlplane.md#spec)).
  A test holds every JSON path of upstream's type, in the `k8s.io/apiserver` the operator links, to a path of the copy of the same kind, and finds none more; a filled object of either type survives a strict decode into the other and back.
  A bump of `k8s.io/apiserver` that changes the type fails it, and the pull request that bumps adds the field with its bounds and the first minor that knows it ([rule](../../.claude/rules/upstream-types.md)).
- **Workloads renders the list into the `AuthenticationConfiguration` it already writes**, `<cp>-authentication-config`, beside its own anonymous section, converted to upstream's type by a case-sensitive strict decode of its JSON, as kube-apiserver decodes the file ([authentication.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/options/authentication.go#L873-L915)).
- **Every issuer is reached through konnectivity**: Workloads sets `egressSelectorType: cluster` on each authenticator, and the schema admits `cluster` alone, defaults to it, and refuses `StructuredAuthenticationConfigurationEgressSelector: false`.
  kube-apiserver then sends every request for that issuer, discovery and keys, through the dialer of the egress selection `cluster` ([oidc.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/plugin/pkg/authenticator/token/oidc/oidc.go#L317-L340)), which Lodia's egress configuration sends through konnectivity-server's socket to the agents on the tenant's nodes ([api-exposure](api-exposure.md)).
- **Every authenticator ends with a user validation rule of Lodia's**: `!user.username.startsWith('system:') && !user.username.startsWith('lodia:') && !user.groups.exists(g, g.startsWith('system:') || g.startsWith('lodia:'))`, with the message `system: and lodia: names are reserved`.
  An authenticator whose spec holds that expression keeps its own rule, since kube-apiserver refuses an expression twice ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/apis/apiserver/validation/validation.go#L546-L566)).
- **A JWT edit reloads, and only the anonymous section restarts the API server.**
  The API server's restart hash reads the authentication configuration rendered without JWT authenticators, which are the bytes it read before this decision, so the release that brings it rolls no tenant and an edit of `jwt` rolls nothing.
  kube-apiserver watches the file and polls it every minute, and swaps in new bytes once they decode, validate and every issuer in them answers, within a minute; it refuses a changed anonymous section ([authentication.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/options/authentication.go#L760-L815), [config.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/authenticator/config.go#L314-L337)), so an operator revision that changes that section rolls the API server.

## Why

- **Upstream's type is the file its users already write.**
  A copy keeps `go.lodia.io/lodia/api` on `k8s.io/api` and `k8s.io/apimachinery` alone, so an importer keeps its own client-go ([releases-and-support](releases-and-support.md)); importing `k8s.io/apiserver` would add it and `k8s.io/component-base` to the API module, and controller-gen would build the schema from upstream's markers, which carry no bounds.
  A generator would need the same bounds chosen by hand and would add a field silently with a dependency bump; the test makes the bump decide.
- **The platform takes the whole `jwt` section as the tenant's administrator wrote it** ([who-is-trusted](who-is-trusted.md)): the URLs kube-apiserver fetches, the CA it trusts for them, the expressions it evaluates on every token.
  Through `cluster` the API server reaches an issuer only from the tenant's own nodes, where that administrator runs pods already, and never the management network, a node's metadata service or another tenant's Services.
- **A claim mapped with an empty prefix, or any expression, can yield any name**, `system:masters` and `lodia:user:alice` among them.
  The reserved names keep a token from being a node, a ServiceAccount or a member of `system:masters`, and from holding the bindings of a person's short-lived kubeconfig ([short-lived-kubeconfigs](short-lived-kubeconfigs.md)), so the two credential systems stay apart.
  kube-apiserver adds `system:authenticated` after the rule, so the rule does not refuse every token.
- **kube-apiserver takes a JWT edit without a restart**, so a roll for each would cost every replica for nothing.

## Rejected

- **A flag and a file of the tenant's**, through `extraArgs`: no tenant can put a file into the API server's pod, and the schema refuses `authentication-config` and every `oidc-*` flag.
- **A reference to a ConfigMap of the tenant's**: without admission webhooks ([no-admission-webhooks](no-admission-webhooks.md)) nothing would check it before a reconcile, and the operator's cache holds only labelled objects ([independent-controllers](independent-controllers.md)).
- **A dialect of Lodia's own**: a third shape for one file, with fields upstream has and it lacks.
- **A roll on every edit**: kube-apiserver needs none.
- **A direct dial from the API server's pod**: it reaches the management network; one limited to public networks is a loosening for later, with the egress policy that would enforce it.

What the neighbours do is in [analysis](../analysis.md).

## Consequences

- Before a tenant's first node joins, no agent is connected: a token's login fails and an edit does not load, while the admin kubeconfig and `KubeconfigRequest`s work ([short-lived-kubeconfigs](short-lived-kubeconfigs.md)).
- An issuer that does not answer holds back every edit of the file while it stays in it, and kube-apiserver logs `failed to update authentication config`.
- A platform that wants OIDC administrators binds a group of its own inside the tenant; `lodia:cluster-admins` stays the certificates' group.

## Code

- `api/v1alpha1/authentication_types.go` — the copy; `test/api/authentication_test.go` holds each CEL rule, the default egress and the feature gate.
- `internal/authnconfig` — `Upstream`, the conversion; `drift_test.go` holds the shape and the round trips.
- `internal/controller/workloads/render.go` — `jwtAuthenticators` and `configMaps`; `authentication_internal_test.go` holds the egress and the reserved names once on every authenticator, the restart hash of before for any `jwt` and its change with the anonymous section, and the golden `testdata/render/authentication.yaml`.
