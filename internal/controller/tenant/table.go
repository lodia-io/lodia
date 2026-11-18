// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

type kind struct {
	version, resource string
	namespaced        bool
}

// kinds holds every group and kind Tenant writes, and a kind that leaves the
// render stays here so that the prune can delete what earlier revisions wrote.
var kinds = map[schema.GroupKind]kind{
	{Kind: "ConfigMap"}:      {"v1", "configmaps", true},
	{Kind: "Endpoints"}:      {"v1", "endpoints", true},
	{Kind: "Secret"}:         {"v1", "secrets", true},
	{Kind: "Service"}:        {"v1", "services", true},
	{Kind: "ServiceAccount"}: {"v1", "serviceaccounts", true},

	{Group: "apps", Kind: "DaemonSet"}:  {"v1", "daemonsets", true},
	{Group: "apps", Kind: "Deployment"}: {"v1", "deployments", true},

	{Group: "discovery.k8s.io", Kind: "EndpointSlice"}: {"v1", "endpointslices", true},

	{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole"}:        {"v1", "clusterroles", false},
	{Group: "rbac.authorization.k8s.io", Kind: "ClusterRoleBinding"}: {"v1", "clusterrolebindings", false},
	{Group: "rbac.authorization.k8s.io", Kind: "Role"}:               {"v1", "roles", true},
	{Group: "rbac.authorization.k8s.io", Kind: "RoleBinding"}:        {"v1", "rolebindings", true},
}

// addressed addresses e at the table's version, not the entry's. It reports
// false for an entry of a kind outside the table, or whose scope or names the
// kind does not take: one of a kind no longer served.
func addressed(request *rest.Request, e entry) (*rest.Request, bool) {
	k, ok := kinds[schema.GroupKind{Group: e.Group, Kind: e.Kind}]
	if !ok || k.namespaced != (e.Namespace != "") {
		return nil, false
	}
	if e.Group == "" {
		request = request.Prefix("api", k.version)
	} else {
		request = request.Prefix("apis", e.Group, k.version)
	}
	request = request.NamespaceIfScoped(e.Namespace, k.namespaced).Resource(k.resource).Name(e.Name)
	return request, request.Error() == nil
}
