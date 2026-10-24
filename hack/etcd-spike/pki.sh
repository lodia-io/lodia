#!/usr/bin/env bash
# Issues etcd-druid's certificates for an Etcd called $CP-etcd from the hard
# way's etcd CA. The names follow etcd-wrapper's development PKI script; the
# sidecar's also carry the client Service, through which etcd-druid calls it
# for the snapshot before a scale-out.
#
#   bash hack/etcd-spike/pki.sh                     issue everything
#   bash hack/etcd-spike/pki.sh etcd-druid-backup   issue only the named certificates
set -euo pipefail
: "${PKI:?source hack/etcd-spike/env.sh first}"
[ -f "$PKI/etcd-ca.crt" ] || { echo "$PKI/etcd-ca.crt is missing: run hack/hard-way/pki.sh first" >&2; exit 1; }

n=$CP-etcd
want=" $* "
pick() { [ "$want" = "  " ] || [[ "$want" == *" $1 "* ]]; }

# dns NAME... — each name as DNS SANs in its short, namespaced and cluster forms.
dns() {
  local s out=()
  for s in "$@"; do out+=("DNS:$s" "DNS:$s.$CP" "DNS:$s.$CP.svc" "DNS:$s.$CP.svc.cluster.local"); done
  (IFS=,; echo "${out[*]}")
}

# leaf NAME SUBJECT EXTENDED_USAGE [SANS] — signed by the etcd CA, one year.
leaf() {
  local name=$1 subject=$2 usage=$3 sans=${4:-}
  pick "$name" || return 0
  openssl genrsa -out "$PKI/$name.key" 2048 2>/dev/null
  openssl req -new -key "$PKI/$name.key" -subj "$subject" -out "$PKI/$name.csr"
  {
    echo "basicConstraints=critical,CA:FALSE"
    echo "keyUsage=critical,digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=$usage"
    if [ -n "$sans" ]; then echo "subjectAltName=$sans"; fi
  } >"$PKI/$name.ext"
  openssl x509 -req -in "$PKI/$name.csr" -CA "$PKI/etcd-ca.crt" -CAkey "$PKI/etcd-ca.key" -CAcreateserial \
    -days 365 -sha256 -extfile "$PKI/$name.ext" -out "$PKI/$name.crt" 2>/dev/null
  rm -f "$PKI/$name.csr" "$PKI/$name.ext" "$PKI"/*.srl
  echo "$name: $(openssl x509 -in "$PKI/$name.crt" -noout -ext subjectAltName 2>/dev/null | tail -n +2 | tr -d ' ')"
}

leaf etcd-druid-server "/CN=$n" serverAuth,clientAuth "DNS:$n-local,$(dns "$n-client" "*.$n-peer"),DNS:localhost,IP:127.0.0.1"
leaf etcd-druid-peer "/CN=$n-peer" serverAuth,clientAuth "$(dns "$n-peer" "*.$n-peer")"
leaf etcd-druid-backup "/CN=$n-backup-restore" serverAuth "DNS:$n-local,$(dns "$n-client"),DNS:localhost,IP:127.0.0.1"
leaf etcd-druid-client "/CN=$n-client" clientAuth
