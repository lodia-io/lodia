// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command oidcissuer serves the files of a directory over TLS: the discovery
// documents and keys of the OIDC issuers the e2e suite signs tokens for.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"
)

func main() {
	listen := flag.String("listen", ":8443", "the address to serve on")
	dir := flag.String("dir", "/etc/oidcissuer/files", "the directory to serve")
	cert := flag.String("cert", "/etc/oidcissuer/tls/tls.crt", "the serving certificate")
	key := flag.String("key", "/etc/oidcissuer/tls/tls.key", "the serving certificate's key")
	flag.Parse()
	server := &http.Server{Addr: *listen, Handler: http.FileServer(http.Dir(*dir)), ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServeTLS(*cert, *key))
}
