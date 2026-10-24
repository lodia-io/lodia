# Source from the repository root: `source hack/etcd-spike/env.sh`.
# The hard way's environment with the spike's own kind cluster, namespace, port
# and work directory, so its helpers (mk, tk, render) and scripts (pki.sh,
# kubeconfigs.sh) work unchanged; plus the versions the spike pins.
source "$PWD/hack/hard-way/env.sh"

export MGMT=brygga-etcd-spike   # the spike's kind cluster
export CP=tenant-e              # the tenant whose etcd the spike runs; also its namespace
export APISERVER_PORT=31443     # beside the hard way's 30443
export WORK="$PWD/work/etcd-spike"
export PKI="$WORK/pki"
export ES="$PWD/hack/etcd-spike"
mkdir -p "$PKI"

export DRUID_VERSION=v0.39.0
export RUSTFS_IMAGE=rustfs/rustfs:1.0.3
export AWS_CLI_IMAGE=amazon/aws-cli:2.38.5

unset MGMT_IP
if docker inspect "$MGMT-control-plane" >/dev/null 2>&1; then
  MGMT_IP="$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$MGMT-control-plane")"
  export MGMT_IP
fi
