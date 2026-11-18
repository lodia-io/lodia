---
paths:
  - "internal/**"
---

# Reading a tenant

- A client of a tenant's API server comes from `internal/tenantclient` and nowhere else; `clientcmd.RESTConfigFromKubeConfig`, `client.New`, `kubernetes.NewForConfig`, `rest.HTTPClientFor` and `rest.UnversionedRESTClientForConfigAndClient` stay out of the controllers ([decision](../../docs/decisions/one-client-for-a-tenant.md)).
- An answer of a tenant is decoded into a projection with `internal/jsonscan`, each array counted, never into an API type or `unstructured`: its administrator is untrusted ([decision](../../docs/decisions/who-is-trusted.md)).
- The data of a Secret the cache holds only the metadata of is read through the operator's one `internal/secretcache` reader, by anyone but its writer, so that it costs no request while its `resourceVersion` stays.
