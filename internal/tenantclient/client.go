// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package tenantclient builds, keeps and uses every client of a tenant's API
// server. The tenant's administrator is untrusted inside the tenant, and can put
// a server of their own in place of a built-in group
// (docs/decisions/who-is-trusted.md), so a request goes over HTTP/1.1 with no
// compression and no redirect, is sent once, and has its answer cut at a size
// and decoded by the caller into the fields it keeps
// (docs/decisions/tenant-clients-speak-http1.md).
package tenantclient

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Options are what a controller sets for its clients.
type Options struct {
	// Connections is the most requests the controller has in flight against one
	// tenant at once, the cap on the connections of the client.
	Connections int
	// Timeout bounds a dial and a TLS handshake, which net/http runs on a
	// context detached from the request's.
	Timeout time.Duration
	// Dial, when set, opens every connection; tests set it.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

type Client struct {
	api *rest.RESTClient
}

func Config(kubeconfig []byte) (*rest.Config, error) {
	return clientcmd.RESTConfigFromKubeConfig(kubeconfig)
}

// New returns a client for cfg, which it leaves as it is. Close it to close its
// idle connections.
func New(cfg *rest.Config, opts Options) (*Client, error) {
	cfg = rest.CopyConfig(cfg)
	// The in-namespace Service is never behind a proxy from the environment.
	cfg.Proxy = http.ProxyURL(nil)
	cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	// A QPS below zero turns client-go's limiter off. Its default, 5 requests a
	// second with a burst of 10, stops a list of the Nodes at about 34 pages
	// inside Timeout. Without it, since Do sends a request once, a pass of
	// health-nodes sends at most maxPages requests for each of the two lists and
	// the hold's: a read of the controller manager's Lease, then up to
	// retry.DefaultRetry's five patches, each after the first preceded by a read
	// of the Lease again (5 patches and 5 reads when every patch conflicts); a
	// pass of health sends the reads of the two leader Leases, or one page of each
	// of a held restore's seven lists. capResponses cuts the headers and the body
	// of every answer; the tenant's API server limits what it serves itself
	// (https://kubernetes.io/docs/concepts/cluster-administration/flow-control/).
	cfg.QPS = -1
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return capResponses(next, opts.Connections, opts.Timeout)
	})
	// Leaving h2 out of NextProtos keeps client-go from configuring HTTP/2
	// (https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L196-L208):
	// the transport counts and caps its connections, which an HTTP/2 pool does
	// not.
	cfg.NextProtos = []string{"http/1.1"}
	// net/http then asks for no gzip and decodes none (https://github.com/golang/go/blob/go1.27.4/src/net/http/transport.go#L3021-L3038),
	// so no gzip reader is made, whose pool keeps one past the read.
	cfg.DisableCompression = true
	cfg.Dial = opts.dialContext
	cfg.AcceptContentTypes, cfg.ContentType = "application/json", "application/json"
	// client-go asks for a serializer, and DoRaw never uses it: no response is
	// decoded into the API's types.
	cfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	transport, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	transport.CheckRedirect = noRedirects
	api, err := rest.UnversionedRESTClientForConfigAndClient(cfg, transport)
	if err != nil {
		return nil, err
	}
	return &Client{api: api}, nil
}

func noRedirects(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// dialContext ends a dial within Timeout, since net/http dials on a context
// detached from the request's; the TLS handshake that follows has its own bound
// in capResponses. Dial, when set, replaces the dialer in tests.
func (o Options) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	dial := o.Dial
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	return dial(ctx, network, address)
}

func (c *Client) Close() {
	utilnet.CloseIdleConnectionsFor(c.api.Client.Transport)
}

func (c *Client) HTTP() *http.Client { return c.api.Client }

func (c *Client) Get() *rest.Request { return c.api.Get() }

func (c *Client) Post() *rest.Request { return c.api.Post() }

func (c *Client) Patch(patch types.PatchType) *rest.Request { return c.api.Patch(patch) }

func (c *Client) Delete() *rest.Request { return c.api.Delete() }

// Clients keep a client for each control plane, which they build again when the
// bytes of its kubeconfig change. A controller keeps its own, with its own
// options, so that one controller's slow tenant never waits on another's cap.
type Clients struct {
	opts Options
	mu   sync.Mutex
	kept map[types.NamespacedName]*kept
}

type kept struct {
	kubeconfig []byte
	client     *Client
}

func NewClients(opts Options) *Clients {
	return &Clients{opts: opts, kept: map[types.NamespacedName]*kept{}}
}

// Of returns the client of key that reads inside the tenant as the kubeconfig's
// user. The client of a previous kubeconfig closes its idle connections.
func (c *Clients) Of(key types.NamespacedName, kubeconfig []byte) (*Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.kept[key]
	if previous != nil && bytes.Equal(previous.kubeconfig, kubeconfig) {
		return previous.client, nil
	}
	cfg, err := Config(kubeconfig)
	if err != nil {
		return nil, err
	}
	client, err := New(cfg, c.opts)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		previous.client.Close()
	}
	c.kept[key] = &kept{kubeconfig: kubeconfig, client: client}
	return client, nil
}

func (c *Clients) Forget(key types.NamespacedName) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if previous := c.kept[key]; previous != nil {
		previous.client.Close()
	}
	delete(c.kept, key)
}
