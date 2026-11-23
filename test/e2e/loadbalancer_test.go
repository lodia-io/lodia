// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && loadbalancer

package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

type balanced struct {
	key    client.ObjectKey
	class  string
	access *tenantAccess
	node   string
}

func TestLoadBalancerGate(t *testing.T) {
	var (
		k8s  client.WithWatch
		loop reference.NamedTagged
	)
	setup := func(ctx context.Context, t *testing.T, cfg *envconf.Config) {
		k8s = newClient(t, cfg)
		if loop == nil {
			image, err := build(ctx, cfg.KubeconfigFile(), "./test/"+apiLoop)
			if err != nil {
				t.Fatal(err)
			}
			loop = image
		}
	}

	direct := &balanced{key: client.ObjectKey{Namespace: "team-lb-ip", Name: controlPlane}, class: "e2e-lb-ip"}
	var ipEndpoint lodiav1alpha1.EndpointStatus
	ipHost := poolAddress(10)
	asked := features.New("an IP host that MetalLB's annotation asks for: published before the balancer has the address, reached directly").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			setup(ctx, t, cfg)
			deletePool(ctx, t, k8s)
			createTenant(ctx, t, k8s, direct.key, direct.class, balancer(ipHost, "", map[string]string{metallbAnnotation: ipHost}))
			return ctx
		}).
		Assess("with no pool the Service has no address, and status.endpoint is host:443 with the tenant built, LoadBalancerReady False, WaitingForLoadBalancer", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 8*time.Minute, "the endpoint at "+ipHost+" is published, the tenant built and waiting for the balancer", func(ctx context.Context) error {
				cp, err := publishedAt(ctx, k8s, direct.key, ipHost, hostPort(ipHost, lodiav1alpha1.APIServerServicePort))
				if err != nil {
					return err
				}
				if err := built(cp); err != nil {
					return err
				}
				if err := direct.reports(ctx, k8s, ""); err != nil {
					return err
				}
				for _, conditionType := range []string{lodiav1alpha1.ConditionLoadBalancerReady, lodiav1alpha1.ConditionReady} {
					if _, err := conditionAt(ctx, k8s, direct.key, conditionType, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForLoadBalancer); err != nil {
						return err
					}
				}
				ipEndpoint = *cp.Status.Endpoint
				return nil
			})
			return ctx
		}).
		Assess("once MetalLB has a pool it gives host, and LoadBalancerReady turns Ready with the endpoint where it was", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			createPool(ctx, t, k8s)
			until(ctx, t, 3*time.Minute, "the Service has "+ipHost+" and the tenant is Ready", func(ctx context.Context) error {
				return settled(ctx, k8s, direct, ipHost, ipEndpoint)
			})
			return ctx
		}).
		Assess("a node joins, default/kubernetes is host:443 with no apiserver-proxy, and a pod reaches its API through https://kubernetes.default", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			direct.access = connect(t, adminKubeconfig(ctx, t, k8s, direct.key))
			direct.reach(ctx, t, cfg, k8s, loop, hostPort(ipHost, lodiav1alpha1.APIServerServicePort), false)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			direct.close(ctx, t, k8s)
			return ctx
		}).
		Feature()

	mismatched := &balanced{key: client.ObjectKey{Namespace: "team-lb-mismatch", Name: controlPlane}, class: "e2e-lb-mismatch"}
	var before struct {
		endpoint   lodiav1alpha1.EndpointStatus
		pod        string
		kubeconfig []byte
	}
	named, given := poolAddress(20), poolAddress(21)
	repaired := features.New("an IP host whose annotation asks for another address: AddressMismatch, the tenant built, then the annotation repaired in place").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			setup(ctx, t, cfg)
			createPool(ctx, t, k8s)
			createTenant(ctx, t, k8s, mismatched.key, mismatched.class, balancer(named, "", map[string]string{metallbAnnotation: given}))
			return ctx
		}).
		Assess("MetalLB gives "+given+" and the Service reports it: AddressMismatch names both addresses, the tenant is built at "+named+", and a Normal event says so", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 8*time.Minute, "LoadBalancerReady is False, AddressMismatch, over a tenant built at "+named, func(ctx context.Context) error {
				cp, err := publishedAt(ctx, k8s, mismatched.key, named, hostPort(named, lodiav1alpha1.APIServerServicePort))
				if err != nil {
					return err
				}
				if err := built(cp); err != nil {
					return err
				}
				if err := mismatched.reports(ctx, k8s, given); err != nil {
					return err
				}
				cp, err = conditionAt(ctx, k8s, mismatched.key, lodiav1alpha1.ConditionLoadBalancerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonAddressMismatch)
				if err != nil {
					return err
				}
				if m := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady).Message; !strings.Contains(m, named) || !strings.Contains(m, given) {
					return fmt.Errorf("the message does not name %s and %s: %s", named, given, m)
				}
				if _, err := conditionAt(ctx, k8s, mismatched.key, lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonAddressMismatch); err != nil {
					return err
				}
				before.endpoint = *cp.Status.Endpoint
				return nil
			})
			until(ctx, t, 2*time.Minute, "a Normal event AddressMismatch of LoadBalancerReady names "+named+" and "+given, func(ctx context.Context) error {
				return mismatchRecorded(ctx, k8s, mismatched.key, named, given)
			})
			before.pod = string(apiServerPod(ctx, t, k8s, mismatched.key).UID)
			before.kubeconfig = adminKubeconfigBytes(ctx, t, k8s, mismatched.key)
			return ctx
		}).
		Assess("the annotation, repaired in place, makes MetalLB move the address to host: Ready, and nothing of the tenant's identity moved", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, mismatched.key, cp); err != nil {
				t.Fatal(err)
			}
			patch := client.MergeFrom(cp.DeepCopy())
			cp.Spec.Endpoint.LoadBalancer.Annotations = map[string]string{metallbAnnotation: named}
			if err := k8s.Patch(ctx, cp, patch); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s asks for %s instead of %s at generation %d", metallbAnnotation, named, given, cp.Generation)
			until(ctx, t, 3*time.Minute, "the Service has "+named+" and the tenant is Ready", func(ctx context.Context) error {
				return settled(ctx, k8s, mismatched, named, before.endpoint)
			})
			if pod := string(apiServerPod(ctx, t, k8s, mismatched.key).UID); pod != before.pod {
				t.Errorf("the API server pod %s was replaced by %s", before.pod, pod)
			}
			if kubeconfig := adminKubeconfigBytes(ctx, t, k8s, mismatched.key); !bytes.Equal(kubeconfig, before.kubeconfig) {
				t.Error("the admin kubeconfig changed")
			}
			return ctx
		}).
		Assess("a node joins, default/kubernetes is host:443 with no apiserver-proxy, and a pod reaches its API through https://kubernetes.default", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			mismatched.access = connect(t, adminKubeconfig(ctx, t, k8s, mismatched.key))
			mismatched.reach(ctx, t, cfg, k8s, loop, hostPort(named, lodiav1alpha1.APIServerServicePort), false)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			mismatched.close(ctx, t, k8s)
			return ctx
		}).
		Feature()

	byName := &balanced{key: client.ObjectKey{Namespace: "team-lb-name", Name: controlPlane}, class: "e2e-lb-name"}
	nameHost := "api." + byName.key.Namespace + "." + gatewayZone
	var nameAddress string
	var nameEndpoint lodiav1alpha1.EndpointStatus
	served := features.New("a name host the DNS container serves once MetalLB has given the address: published at once, reached through apiserver-proxy").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			setup(ctx, t, cfg)
			createPool(ctx, t, k8s)
			createTenant(ctx, t, k8s, byName.key, byName.class, balancer(nameHost, "", nil))
			return ctx
		}).
		Assess("the endpoint is the name before any record exists: LoadBalancerReady is Ready, and Reachable fails until the name resolves", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 8*time.Minute, "the endpoint at "+nameHost+" is published, the balancer ready, the name unresolved", func(ctx context.Context) error {
				cp, err := publishedAt(ctx, k8s, byName.key, nameHost, proxied())
				if err != nil {
					return err
				}
				if err := built(cp); err != nil {
					return err
				}
				if nameAddress, err = byName.address(ctx, k8s); err != nil {
					return err
				}
				if nameAddress == "" {
					return errors.New("the Service reports no address")
				}
				if _, err := conditionAt(ctx, k8s, byName.key, lodiav1alpha1.ConditionLoadBalancerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady); err != nil {
					return err
				}
				for _, conditionType := range []string{lodiav1alpha1.ConditionReachable, lodiav1alpha1.ConditionReady} {
					if _, err := conditionAt(ctx, k8s, byName.key, conditionType, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed); err != nil {
						return err
					}
				}
				nameEndpoint = *cp.Status.Endpoint
				return nil
			})
			return ctx
		}).
		Assess("the record, pointed at the address MetalLB gave, makes Reachable and Ready True with the endpoint where it was", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			publish(ctx, t, cfg, nameHost, nameAddress)
			until(ctx, t, 5*time.Minute, nameHost+" resolves to "+nameAddress+" and the tenant is Ready", func(ctx context.Context) error {
				return settled(ctx, k8s, byName, nameAddress, nameEndpoint)
			})
			return ctx
		}).
		Assess("a node joins by the name, default/kubernetes is 240.0.0.1:6443 served by apiserver-proxy, and a pod reaches its API through https://kubernetes.default", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			kubeconfig := adminKubeconfig(ctx, t, k8s, byName.key)
			atAddress(nameAddress)(kubeconfig)
			byName.access = connect(t, kubeconfig)
			byName.reach(ctx, t, cfg, k8s, loop, proxied(), true)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			byName.close(ctx, t, k8s)
			return ctx
		}).
		Feature()

	discovered := &balanced{key: client.ObjectKey{Namespace: "team-lb-discovered", Name: controlPlane}, class: "e2e-lb-discovered"}
	cloudHost := "api." + discovered.key.Namespace + "." + gatewayZone
	var cloudAddress string
	cloud := features.New("a discovered host name: a Service no controller serves, which the gate gives the name as a cloud would, over a second Service at the name's address").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			setup(ctx, t, cfg)
			createPool(ctx, t, k8s)
			createTenant(ctx, t, k8s, discovered.key, discovered.class, balancer("", unservedClass, nil))
			return ctx
		}).
		Assess("without a host and with no address the endpoint is not published: WaitingForLoadBalancer, and the Service stays without an address", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 5*time.Minute, "LoadBalancerReady is False, WaitingForLoadBalancer, and no endpoint exists", func(ctx context.Context) error {
				cp, err := conditionAt(ctx, k8s, discovered.key, lodiav1alpha1.ConditionLoadBalancerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForLoadBalancer)
				if err != nil {
					return err
				}
				if cp.Status.Endpoint != nil {
					return fmt.Errorf("status.endpoint is %+v", *cp.Status.Endpoint)
				}
				if _, err := conditionAt(ctx, k8s, discovered.key, lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint); err != nil {
					return err
				}
				svc, err := discovered.service(ctx, k8s)
				if err != nil {
					return err
				}
				if class := svc.Spec.LoadBalancerClass; class == nil || *class != unservedClass || len(svc.Status.LoadBalancer.Ingress) != 0 {
					return fmt.Errorf("the Service has class %v and reports %+v", class, svc.Status.LoadBalancer.Ingress)
				}
				return nil
			})
			return ctx
		}).
		Assess("the name, resolving to a second MetalLB Service over the API server pods and written to the first Service's status, is the endpoint, and the tenant turns Ready", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			second := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: discovered.key.Namespace, Name: lodiav1alpha1.APIServerServiceName(discovered.key.Name) + "-second"},
				Spec: corev1.ServiceSpec{
					Type:     corev1.ServiceTypeLoadBalancer,
					Selector: map[string]string{lodiav1alpha1.ControlPlaneLabel: discovered.key.Name, lodiav1alpha1.ComponentLabel: lodiav1alpha1.APIServerComponent},
					Ports: []corev1.ServicePort{
						{Name: "https", Port: lodiav1alpha1.APIServerServicePort, TargetPort: intstr.FromString("https")},
						{Name: "konnectivity", Port: konnectivityPort, TargetPort: intstr.FromString("konnectivity")},
					},
				},
			}
			create(ctx, t, k8s, second)
			until(ctx, t, 2*time.Minute, "MetalLB gives the second Service an address", func(ctx context.Context) error {
				if err := k8s.Get(ctx, client.ObjectKeyFromObject(second), second); err != nil {
					return err
				}
				if len(second.Status.LoadBalancer.Ingress) == 0 {
					return errors.New("the second Service has no address")
				}
				cloudAddress = second.Status.LoadBalancer.Ingress[0].IP
				return nil
			})
			publish(ctx, t, cfg, cloudHost, cloudAddress)
			svc, err := discovered.service(ctx, k8s)
			if err != nil {
				t.Fatal(err)
			}
			patch := client.MergeFrom(svc.DeepCopy())
			svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{Hostname: cloudHost}}
			if err := k8s.Status().Patch(ctx, svc, patch); err != nil {
				t.Fatal(err)
			}
			t.Logf("Service %s reports the host name %s, which the DNS container answers with %s, the address of Service %s", svc.Name, cloudHost, cloudAddress, second.Name)
			until(ctx, t, 6*time.Minute, "the endpoint is "+cloudHost+", LoadBalancerReady is Ready and the tenant is Ready", func(ctx context.Context) error {
				cp, err := publishedAt(ctx, k8s, discovered.key, cloudHost, proxied())
				if err != nil {
					return err
				}
				if _, err := conditionAt(ctx, k8s, discovered.key, lodiav1alpha1.ConditionLoadBalancerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady); err != nil {
					return err
				}
				return controlPlaneReady(ctx, k8s, discovered.key, cp)
			})
			return ctx
		}).
		Assess("a node joins by the name, default/kubernetes is 240.0.0.1:6443 served by apiserver-proxy, and a pod reaches its API through https://kubernetes.default", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			kubeconfig := adminKubeconfig(ctx, t, k8s, discovered.key)
			atAddress(cloudAddress)(kubeconfig)
			discovered.access = connect(t, kubeconfig)
			discovered.reach(ctx, t, cfg, k8s, loop, proxied(), true)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			discovered.close(ctx, t, k8s)
			return ctx
		}).
		Feature()

	testenv.Test(t, asked, repaired, served, cloud)
}

func balancer(host, class string, annotations map[string]string) func(*lodiav1alpha1.ControlPlaneSpec) {
	return func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Endpoint = lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{
			Host: lodiav1alpha1.EndpointHost(host), Class: class, Annotations: annotations,
		}}
	}
}

func hostPort(host string, port int32) lodiav1alpha1.HostPort {
	return lodiav1alpha1.HostPort{Host: host, Port: port}
}

func proxied() lodiav1alpha1.HostPort {
	return hostPort(lodiav1alpha1.DefaultAdvertiseAddress, 6443)
}

func atAddress(address string) func(*clientcmdapi.Config) {
	return func(kubeconfig *clientcmdapi.Config) {
		for _, cluster := range kubeconfig.Clusters {
			server, err := url.Parse(cluster.Server)
			if err != nil {
				continue
			}
			cluster.TLSServerName = server.Hostname()
			cluster.Server = "https://" + net.JoinHostPort(address, server.Port())
		}
	}
}

func publishedAt(ctx context.Context, k8s client.Client, key client.ObjectKey, host string, inCluster lodiav1alpha1.HostPort) (*lodiav1alpha1.ControlPlane, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		return nil, err
	}
	want := lodiav1alpha1.EndpointStatus{
		APIServer:    hostPort(host, lodiav1alpha1.APIServerServicePort),
		Konnectivity: hostPort(host, konnectivityPort),
		InCluster:    inCluster,
	}
	if cp.Status.Endpoint == nil || !reflect.DeepEqual(*cp.Status.Endpoint, want) {
		return nil, fmt.Errorf("status.endpoint is %+v, want %+v", cp.Status.Endpoint, want)
	}
	return cp, nil
}

func built(cp *lodiav1alpha1.ControlPlane) error {
	for _, conditionType := range []string{
		lodiav1alpha1.ConditionCertificatesReady, lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ConditionAPIServerReady,
		lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ConditionAddonsApplied,
	} {
		if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != cp.Generation {
			return fmt.Errorf("%s is %+v at generation %d", conditionType, c, cp.Generation)
		}
	}
	return nil
}

func settled(ctx context.Context, k8s client.Client, b *balanced, address string, endpoint lodiav1alpha1.EndpointStatus) error {
	if err := b.reports(ctx, k8s, address); err != nil {
		return err
	}
	if _, err := conditionAt(ctx, k8s, b.key, lodiav1alpha1.ConditionLoadBalancerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady); err != nil {
		return err
	}
	cp := &lodiav1alpha1.ControlPlane{}
	if err := controlPlaneReady(ctx, k8s, b.key, cp); err != nil {
		return err
	}
	if !reflect.DeepEqual(*cp.Status.Endpoint, endpoint) {
		return fmt.Errorf("status.endpoint moved from %+v to %+v", endpoint, *cp.Status.Endpoint)
	}
	return nil
}

func mismatchRecorded(ctx context.Context, k8s client.Client, key client.ObjectKey, named, given string) error {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		return err
	}
	events := &eventsv1.EventList{}
	if err := k8s.List(ctx, events, client.InNamespace(key.Namespace)); err != nil {
		return err
	}
	var regarding []string
	for _, e := range events.Items {
		if e.Regarding.Kind != "ControlPlane" || e.Regarding.Name != key.Name || e.Regarding.UID != cp.UID {
			continue
		}
		if e.Action == lodiav1alpha1.ConditionLoadBalancerReady && e.Reason == lodiav1alpha1.ReasonAddressMismatch && e.Type == corev1.EventTypeNormal && strings.Contains(e.Note, named) && strings.Contains(e.Note, given) {
			return nil
		}
		regarding = append(regarding, fmt.Sprintf("%s %s %s %q", e.Type, e.Action, e.Reason, e.Note))
	}
	return fmt.Errorf("the events regarding it are %v", regarding)
}

func adminKubeconfigBytes(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) []byte {
	t.Helper()
	secret := &corev1.Secret{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.AdminKubeconfigSecret.Name(key.Name)}, secret); err != nil {
		t.Fatal(err)
	}
	return secret.Data[lodiav1alpha1.KubeconfigKey]
}

func (b *balanced) service(ctx context.Context, k8s client.Client) (*corev1.Service, error) {
	svc := &corev1.Service{}
	err := k8s.Get(ctx, client.ObjectKey{Namespace: b.key.Namespace, Name: lodiav1alpha1.APIServerServiceName(b.key.Name)}, svc)
	return svc, err
}

func (b *balanced) address(ctx context.Context, k8s client.Client) (string, error) {
	svc, err := b.service(ctx, k8s)
	if err != nil || len(svc.Status.LoadBalancer.Ingress) == 0 {
		return "", err
	}
	return svc.Status.LoadBalancer.Ingress[0].IP, nil
}

func (b *balanced) reports(ctx context.Context, k8s client.Client, want string) error {
	got, err := b.address(ctx, k8s)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("the Service reports %q, want %q", got, want)
	}
	return nil
}

func (b *balanced) reach(ctx context.Context, t *testing.T, cfg *envconf.Config, k8s client.Client, loop reference.NamedTagged, inCluster lodiav1alpha1.HostPort, viaProxy bool) {
	t.Helper()
	nodeImages = nil
	if viaProxy {
		nodeImages = []reference.NamedTagged{tunnelImage(ctx, t, b.access)}
	}
	b.node = joinOneNode(ctx, t, cfg, k8s, b.key, b.access)
	want := net.JoinHostPort(inCluster.Host, strconv.Itoa(int(inCluster.Port)))
	until(ctx, t, time.Minute, "the EndpointSlice default/kubernetes holds "+want, func(ctx context.Context) error {
		slice := &discoveryv1.EndpointSlice{}
		if err := b.access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: "kubernetes"}, slice); err != nil {
			return err
		}
		if got := sliceAddresses(slice); len(got) != 1 || got[0] != want {
			return fmt.Errorf("it holds %v", got)
		}
		return nil
	})
	if !viaProxy {
		if err := gone(b.access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: apiserverProxy}, &appsv1.DaemonSet{})); err != nil {
			t.Errorf("DaemonSet %s in the tenant, whose pods dial an IP host: %v", apiserverProxy, err)
		}
	} else {
		until(ctx, t, 2*time.Minute, lodiav1alpha1.DefaultAdvertiseAddress+"/32 is on "+b.node+"'s lo, held by lodia-tunnel at port 6443", func(ctx context.Context) error {
			if err := onLoopback(ctx, b.node); err != nil {
				return err
			}
			listening, err := docker(ctx, "exec", b.node, "ss", "-Hltnp", "src", want)
			if err != nil {
				return err
			}
			if !strings.Contains(listening, `"lodia-tunnel"`) {
				return fmt.Errorf("%s is not lodia-tunnel's: %q", want, listening)
			}
			return nil
		})
	}
	until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
		return coreDNSAvailable(ctx, b.access.client)
	})
	loops := startLoops(ctx, t, cfg, k8s, b.access, b.key, b.node, loop)
	readLoops(ctx, t, "both loops answer on new and reused connections, the pod's through https://kubernetes.default at "+want, loops)
}

func (b *balanced) close(ctx context.Context, t *testing.T, k8s client.Client) {
	t.Helper()
	if k8s == nil {
		return
	}
	if b.node != "" {
		if _, err := docker(ctx, "rm", "--force", "--volumes", b.node); err != nil {
			t.Error(err)
		}
	}
	if err := client.IgnoreNotFound(k8s.Delete(ctx, &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: b.key.Namespace, Name: b.key.Name}})); err != nil {
		t.Error(err)
		return
	}
	err := eventually(ctx, 4*time.Minute, "ControlPlane "+b.key.String()+" is gone", func(ctx context.Context) error {
		return gone(k8s.Get(ctx, b.key, &lodiav1alpha1.ControlPlane{}))
	})
	if err != nil {
		t.Error(err)
	}
	closeTenant(ctx, t, k8s, b.key, b.class)
}
