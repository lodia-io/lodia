# Source from the repository root: `source hack/hard-way/env.sh`.
# Everything the hard way needs: versions, names, address ranges, and helpers.

# Kubernetes 1.35 and the versions kubeadm pins for it.
export K8S_VERSION=v1.35.8
export KIND_NODE_IMAGE="kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
export ETCD_IMAGE=registry.k8s.io/etcd:3.6.6-0
export KONNECTIVITY_VERSION=v0.35.0
export METRICS_SERVER_VERSION=v0.9.0

export MGMT=brygga-hardway      # the kind management cluster
export CP=tenant-a              # the tenant control plane; also its namespace
export NODE=tenant-a-node-1     # the tenant's worker: a kind node container

# The tenant cluster's own ranges, deliberately unlike kind's 10.96/10.244.
export SERVICE_CIDR=10.32.0.0/16
export KUBERNETES_SVC_IP=10.32.0.1
export DNS_IP=10.32.0.10
export POD_CIDR=10.200.0.0/16

export APISERVER_PORT=30443     # the API server's secure port and its NodePort
export KONNECTIVITY_PORT=30132  # the NodePort in front of konnectivity-server's agent port 8132

export HW="$PWD/hack/hard-way"
export WORK="$PWD/work/hard-way"   # keys, certificates, kubeconfigs; gitignored
export PKI="$WORK/pki"
mkdir -p "$PKI"

# mk: the management cluster, through its own kubeconfig, never your current context.
mk() { kubectl --kubeconfig "$WORK/management.kubeconfig" "$@"; }
# tk: the tenant cluster, as kubernetes-admin.
tk() { kubectl --kubeconfig "$WORK/admin.conf" "$@"; }

# render FILE: replaces ${VAR} with the environment's value and stops on an unset one.
# Kubernetes' own $(VAR) syntax is left alone.
render() { perl -pe 's/\$\{(\w+)\}/defined $ENV{$1} ? $ENV{$1} : die "render: $1 is not set\n"/ge' "$1"; }

# The management node's address on the docker network "kind": the API server
# advertises it, and tenant nodes reach the control plane through it.
if docker inspect "$MGMT-control-plane" >/dev/null 2>&1; then
  MGMT_IP="$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$MGMT-control-plane")"
  export MGMT_IP
fi
