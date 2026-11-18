---
paths:
  - "internal/**"
---

# Reading a tenant

- A client of a tenant's API server comes from `internal/tenantclient` and nowhere else; `clientcmd.RESTConfigFromKubeConfig`, `client.New`, `kubernetes.NewForConfig`, `rest.HTTPClientFor` and `rest.UnversionedRESTClientForConfigAndClient` stay out of the controllers ([decision](../../docs/decisions/one-client-for-a-tenant.md)).
- An answer of a tenant is decoded into a projection with `internal/jsonscan`, each array counted, never into an API type or `unstructured`: its administrator is untrusted ([decision](../../docs/decisions/who-is-trusted.md)).
