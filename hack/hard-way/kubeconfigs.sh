#!/usr/bin/env bash
# Writes the kubeconfigs the components and people use. Each one names the
# address its user dials from where it runs. Run after pki.sh.
set -euo pipefail
: "${PKI:?source hack/hard-way/env.sh first}" "${MGMT_IP:?create the management cluster first}"

# kubeconfig FILE SERVER CERT — the cluster CA, a server, and a client certificate, embedded.
kubeconfig() {
  local file=$WORK/$1 server=$2 cert=$3
  rm -f "$file"
  kubectl config set-cluster "$CP" --server="$server" --certificate-authority="$PKI/ca.crt" --embed-certs=true --kubeconfig="$file" >/dev/null
  kubectl config set-credentials "$cert" --client-certificate="$PKI/$cert.crt" --client-key="$PKI/$cert.key" --embed-certs=true --kubeconfig="$file" >/dev/null
  kubectl config set-context default --cluster="$CP" --user="$cert" --kubeconfig="$file" >/dev/null
  kubectl config use-context default --kubeconfig="$file" >/dev/null
  echo "$file → $server as $cert"
}

# Inside the management cluster: the API server's Service by name.
kubeconfig controller-manager.conf "https://$CP-apiserver.$CP.svc:$APISERVER_PORT" controller-manager
kubeconfig scheduler.conf "https://$CP-apiserver.$CP.svc:$APISERVER_PORT" scheduler
# In the API server's own pod.
kubeconfig konnectivity-server.conf "https://127.0.0.1:$APISERVER_PORT" konnectivity-client
# On your machine, through the kind port mapping.
kubeconfig super-admin.conf "https://127.0.0.1:$APISERVER_PORT" super-admin
kubeconfig admin.conf "https://127.0.0.1:$APISERVER_PORT" admin
# On the docker network "kind", where the tenant's nodes and the kubeadm container live.
kubeconfig admin-net.conf "https://$MGMT_IP:$APISERVER_PORT" admin
