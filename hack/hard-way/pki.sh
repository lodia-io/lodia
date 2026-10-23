#!/usr/bin/env bash
# Issues the tenant's PKI with openssl, one certificate per line, named the way
# kubeadm names them (flattened: etcd-ca instead of etcd/ca). Run after
# `source hack/hard-way/env.sh` and once MGMT_IP is set.
#
#   bash hack/hard-way/pki.sh              issue everything
#   bash hack/hard-way/pki.sh apiserver    re-issue only the named certificates
set -euo pipefail
: "${PKI:?source hack/hard-way/env.sh first}" "${MGMT_IP:?create the management cluster first}"

want=" $* "
pick() { [ "$want" = "  " ] || [[ "$want" == *" $1 "* ]]; }

# ca NAME CN — a self-signed CA, ten years.
ca() {
  pick "$1" || return 0
  openssl genrsa -out "$PKI/$1.key" 2048 2>/dev/null
  openssl req -x509 -new -key "$PKI/$1.key" -subj "/CN=$2" -days 3650 -sha256 \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign,digitalSignature,keyEncipherment" \
    -out "$PKI/$1.crt"
}

# leaf NAME CA SUBJECT EXTENDED_USAGE [SANS] — a certificate signed by CA, one year.
leaf() {
  local name=$1 ca=$2 subject=$3 usage=$4 sans=${5:-}
  pick "$name" || return 0
  openssl genrsa -out "$PKI/$name.key" 2048 2>/dev/null
  openssl req -new -key "$PKI/$name.key" -subj "$subject" -out "$PKI/$name.csr"
  {
    echo "basicConstraints=critical,CA:FALSE"
    echo "keyUsage=critical,digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=$usage"
    if [ -n "$sans" ]; then echo "subjectAltName=$sans"; fi
  } >"$PKI/$name.ext"
  openssl x509 -req -in "$PKI/$name.csr" -CA "$PKI/$ca.crt" -CAkey "$PKI/$ca.key" -CAcreateserial \
    -days 365 -sha256 -extfile "$PKI/$name.ext" -out "$PKI/$name.crt" 2>/dev/null
  rm -f "$PKI/$name.csr" "$PKI/$name.ext"
}

# The three CAs: the cluster's, aggregation's (front proxy), etcd's.
ca ca kubernetes
ca front-proxy-ca front-proxy-ca
ca etcd-ca etcd-ca

# The API server's serving certificate: every name a client may dial.
leaf apiserver ca "/CN=kube-apiserver" serverAuth \
  "DNS:kubernetes,DNS:kubernetes.default,DNS:kubernetes.default.svc,DNS:kubernetes.default.svc.cluster.local,DNS:$CP-apiserver,DNS:$CP-apiserver.$CP.svc,DNS:$CP-apiserver.$CP.svc.cluster.local,DNS:localhost,IP:$KUBERNETES_SVC_IP,IP:127.0.0.1,IP:$MGMT_IP"

# The API server as a client: of kubelets, of etcd, of aggregated API servers.
leaf apiserver-kubelet-client ca "/CN=kube-apiserver-kubelet-client" clientAuth
leaf apiserver-etcd-client etcd-ca "/CN=kube-apiserver-etcd-client" clientAuth
leaf front-proxy-client front-proxy-ca "/CN=front-proxy-client" clientAuth

# etcd's serving certificate: the headless Service and every member under it.
leaf etcd-server etcd-ca "/CN=$CP-etcd" serverAuth,clientAuth \
  "DNS:$CP-etcd,DNS:$CP-etcd.$CP.svc,DNS:$CP-etcd.$CP.svc.cluster.local,DNS:*.$CP-etcd.$CP.svc,DNS:*.$CP-etcd.$CP.svc.cluster.local,DNS:localhost,IP:127.0.0.1"

# Component identities: Kubernetes knows these user names from its bootstrap RBAC.
leaf controller-manager ca "/CN=system:kube-controller-manager" clientAuth
leaf scheduler ca "/CN=system:kube-scheduler" clientAuth

# People. super-admin is system:masters, used once to bind kubeadm:cluster-admins;
# admin is in that group and is revocable by deleting the binding.
leaf super-admin ca "/O=system:masters/CN=kubernetes-super-admin" clientAuth
leaf admin ca "/O=kubeadm:cluster-admins/CN=kubernetes-admin" clientAuth

# konnectivity-server: serving for agents (the management node's address and the
# name the SNI spike uses), and an identity for TokenReviews.
leaf konnectivity-server ca "/CN=konnectivity-server" serverAuth \
  "IP:$MGMT_IP,DNS:konnectivity.$CP.example,DNS:localhost,IP:127.0.0.1"
leaf konnectivity-client ca "/CN=system:konnectivity-server" clientAuth

# ServiceAccount tokens: a key pair, no certificate.
if pick sa; then
  openssl genrsa -out "$PKI/sa.key" 2048 2>/dev/null
  openssl rsa -in "$PKI/sa.key" -pubout -out "$PKI/sa.pub" 2>/dev/null
fi

rm -f "$PKI"/*.srl
ls -1 "$PKI" | tr '\n' ' '
echo
