// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && (restore || upgrade)

package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	s3Namespace = "lodia-e2e-s3"
	s3Bucket    = "lodia-e2e"
	s3Secret    = "e2e-s3"
	s3Region    = "us-east-1"
	versitygw   = "docker.io/versity/versitygw:v1.10.0@sha256:89cad13cc981986ee284cbafea4613f9533ce6fd5f37a2c96c4c4fb8adb3a316"
	awsCLI      = "docker.io/amazon/aws-cli:2.39.3@sha256:d557b3524475d70dd3104cf44d96568ee3188897b73a6ece2b37aa50e1af362f"
)

var s3Endpoint = "http://versitygw." + s3Namespace + ".svc:7070"

func startStore(ctx context.Context, t *testing.T, k8s client.Client) {
	t.Helper()
	access, secret := random(t, 10), random(t, 20)
	labels := map[string]string{"app.kubernetes.io/name": "versitygw"}
	keys := map[string]string{"accessKeyID": access, "secretAccessKey": secret}
	create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: s3Namespace}})
	create(ctx, t, k8s, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: s3Namespace, Name: "root"}, StringData: keys})
	create(ctx, t, k8s, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: s3Namespace, Name: "versitygw"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:           "versitygw",
						Image:          versitygw,
						Args:           []string{"--port", ":7070", "posix", "/data"},
						Env:            []corev1.EnvVar{rootKey("ROOT_ACCESS_KEY", "accessKeyID"), rootKey("ROOT_SECRET_KEY", "secretAccessKey")},
						Ports:          []corev1.ContainerPort{{Name: "s3", ContainerPort: 7070}},
						ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("s3")}}},
						VolumeMounts:   []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
					}},
					Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
				},
			},
		},
	})
	create(ctx, t, k8s, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: s3Namespace, Name: "versitygw"},
		Spec:       corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{Name: "s3", Port: 7070, TargetPort: intstr.FromString("s3")}}},
	})
	until(ctx, t, 3*time.Minute, "versitygw is Available", func(ctx context.Context) error {
		deployment := &appsv1.Deployment{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: s3Namespace, Name: "versitygw"}, deployment); err != nil {
			return err
		}
		for _, c := range deployment.Status.Conditions {
			if c.Type == appsv1.DeploymentAvailable && c.Status == corev1.ConditionTrue {
				return nil
			}
		}
		return fmt.Errorf("conditions %+v", deployment.Status.Conditions)
	})
	runAWS(ctx, t, k8s, "bucket", "s3api", "create-bucket", "--bucket", s3Bucket)
	keys["region"], keys["s3ForcePathStyle"] = s3Region, "true"
	create(ctx, t, k8s, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: s3Secret}, StringData: keys})
}

func rootKey(name, key string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "root"}, Key: key}}}
}

func runAWS(ctx context.Context, t *testing.T, k8s client.Client, name string, args ...string) {
	t.Helper()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: s3Namespace, Name: name},
		Spec: batchv1.JobSpec{
			BackoffLimit: new(int32(3)),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:  "aws",
					Image: awsCLI,
					Args:  append([]string{"--endpoint-url", s3Endpoint}, args...),
					Env: []corev1.EnvVar{
						rootKey("AWS_ACCESS_KEY_ID", "accessKeyID"), rootKey("AWS_SECRET_ACCESS_KEY", "secretAccessKey"),
						{Name: "AWS_DEFAULT_REGION", Value: s3Region},
					},
				}},
			}},
		},
	}
	create(ctx, t, k8s, job)
	until(ctx, t, 3*time.Minute, "the Job "+name+" completed", func(ctx context.Context) error {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
			return err
		}
		for _, c := range job.Status.Conditions {
			if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
				return nil
			}
		}
		return fmt.Errorf("conditions %+v", job.Status.Conditions)
	})
}

func random(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}
