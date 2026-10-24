#!/usr/bin/env bash
# Writes a ConfigMap and reads it back through the API server on top of the
# spike's etcd, once a second: one line per round, FAIL with the error. Run
# after `source hack/etcd-spike/env.sh`; stop it with Ctrl-C or kill.
set -uo pipefail
: "${WORK:?source hack/etcd-spike/env.sh first}"

k() { kubectl --kubeconfig "$WORK/admin.conf" --request-timeout=3s -n default "$@"; }

i=0
while true; do
  i=$((i + 1))
  if out=$(k create configmap "load-$i" --from-literal=n="$i" 2>&1) && out=$(k get configmap "load-$i" -o name 2>&1); then
    echo "$(date -u +%T) ok $i"
  else
    echo "$(date -u +%T) FAIL $i ${out//$'\n'/ }"
  fi
  sleep 1
done
