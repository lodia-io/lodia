// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	version = "v9.9.9"
	payload = "the etcd binary"
)

type release struct {
	server   *httptest.Server
	requests atomic.Int32
}

func serve(t *testing.T, change func(name string, archive []byte) (listed, served []byte)) *release {
	t.Helper()
	files := map[string][]byte{}
	var sums strings.Builder
	for _, goos := range []string{"darwin", "linux"} {
		for _, goarch := range []string{"amd64", "arm64"} {
			name, err := archiveName(version, goos, goarch)
			if err != nil {
				t.Fatal(err)
			}
			listed := build(t, name)
			served := listed
			if change != nil {
				listed, served = change(name, listed)
			}
			sum := sha256.Sum256(listed)
			files[name] = served
			fmt.Fprintf(&sums, "%x  %s\n", sum, name)
		}
	}
	files["SHA256SUMS"] = []byte(sums.String())
	r := &release{}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.requests.Add(1)
		body, ok := files[strings.TrimPrefix(req.URL.Path, "/"+version+"/")]
		if !ok {
			http.NotFound(w, req)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(r.server.Close)
	return r
}

func build(t *testing.T, name string) []byte {
	t.Helper()
	dir := strings.TrimSuffix(strings.TrimSuffix(name, ".zip"), ".tar.gz") + "/"
	entries := map[string]string{dir + "etcdctl": "the other binary", dir + "etcd": payload, dir + "README.md": "readme"}
	order := []string{dir + "README.md", dir + "etcdctl", dir + "etcd"}
	var out bytes.Buffer
	if strings.HasSuffix(name, ".zip") {
		w := zip.NewWriter(&out)
		for _, entry := range order {
			f, err := w.Create(entry)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = f.Write([]byte(entries[entry]))
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return out.Bytes()
	}
	gz := gzip.NewWriter(&out)
	w := tar.NewWriter(gz)
	for _, entry := range order {
		if err := w.WriteHeader(&tar.Header{Name: entry, Mode: 0o644, Size: int64(len(entries[entry])), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(entries[entry]))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestTheBinaryOfEachArchiveIsExtractedAndExecutable(t *testing.T) {
	r := serve(t, nil)
	for _, c := range []struct{ goos, goarch string }{{"darwin", "arm64"}, {"darwin", "amd64"}, {"linux", "amd64"}, {"linux", "arm64"}} {
		t.Run(c.goos+"/"+c.goarch, func(t *testing.T) {
			dir := t.TempDir()
			got, err := ensure(t.Context(), r.server.URL, dir, version, c.goos, c.goarch)
			if err != nil {
				t.Fatal(err)
			}
			if want := filepath.Join(dir, version+"-"+c.goos+"-"+c.goarch, "etcd"); got != want {
				t.Errorf("the binary is at %s, want %s", got, want)
			}
			content, err := os.ReadFile(got)
			if err != nil || string(content) != payload {
				t.Errorf("the binary holds %q, %v, want %q", content, err, payload)
			}
			if info, err := os.Stat(got); err != nil || info.Mode().Perm()&0o111 == 0 {
				t.Errorf("the binary is %v, %v, want it executable", info, err)
			}
			entries, err := os.ReadDir(filepath.Dir(got))
			if err != nil || len(entries) != 1 {
				t.Errorf("its directory holds %v, %v, want the binary alone", entries, err)
			}
		})
	}
}

func TestAWarmCacheMakesNoRequest(t *testing.T) {
	r := serve(t, nil)
	dir := t.TempDir()
	first, err := ensure(t.Context(), r.server.URL, dir, version, "linux", "amd64")
	if err != nil {
		t.Fatal(err)
	}
	fetched := r.requests.Load()
	if fetched != 2 {
		t.Fatalf("the first run made %d requests, want SHA256SUMS and the archive", fetched)
	}
	second, err := ensure(t.Context(), r.server.URL, dir, version, "linux", "amd64")
	if err != nil || second != first {
		t.Fatalf("the second run returned %s, %v, want %s", second, err, first)
	}
	if n := r.requests.Load(); n != fetched {
		t.Errorf("the second run made %d requests more, want none", n-fetched)
	}
}

func leftIn(dir string) []string {
	var left []string
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, _ error) error {
		if !d.IsDir() {
			left = append(left, path)
		}
		return nil
	})
	return left
}

func TestAnArchiveThatDiffersFromItsChecksumLeavesNothingInTheCache(t *testing.T) {
	r := serve(t, func(_ string, archive []byte) ([]byte, []byte) {
		return archive, append(bytes.Clone(archive), 0)
	})
	dir := t.TempDir()
	_, err := ensure(t.Context(), r.server.URL, dir, version, "linux", "amd64")
	if err == nil || !strings.Contains(err.Error(), "SHA256SUMS") {
		t.Fatalf("a tampered archive gave %v, want a mismatch with SHA256SUMS", err)
	}
	if left := leftIn(dir); len(left) > 0 {
		t.Errorf("the cache holds %v after a refused archive, want nothing", left)
	}
}

func TestAnArchiveWithoutEtcdLeavesNothingInTheCache(t *testing.T) {
	r := serve(t, func(name string, _ []byte) ([]byte, []byte) {
		var out bytes.Buffer
		gz := gzip.NewWriter(&out)
		w := tar.NewWriter(gz)
		if err := w.WriteHeader(&tar.Header{Name: strings.TrimSuffix(name, ".tar.gz") + "/etcdctl", Mode: 0o644, Size: 1, Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte("x"))
		_ = w.Close()
		_ = gz.Close()
		return out.Bytes(), out.Bytes()
	})
	dir := t.TempDir()
	_, err := ensure(t.Context(), r.server.URL, dir, version, "linux", "amd64")
	if err == nil || !strings.Contains(err.Error(), "no etcd in the archive") {
		t.Fatalf("an archive without etcd gave %v, want no etcd in the archive", err)
	}
	if left := leftIn(dir); len(left) > 0 {
		t.Errorf("the cache holds %v after a refused archive, want nothing", left)
	}
}

func TestAReleaseWithoutTheArchiveOrItsChecksumIsAnError(t *testing.T) {
	r := serve(t, nil)
	if _, err := ensure(t.Context(), r.server.URL, t.TempDir(), "v0.0.1", "linux", "amd64"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("a release the server lacks gave %v, want its 404", err)
	}
}

func TestAPlatformEtcdDoesNotReleaseIsAnError(t *testing.T) {
	for _, c := range []struct{ goos, goarch string }{{"windows", "amd64"}, {"linux", "386"}, {"darwin", "ppc64"}} {
		if name, err := archiveName(version, c.goos, c.goarch); err == nil {
			t.Errorf("%s/%s is named %s, want an error", c.goos, c.goarch, name)
		}
	}
}
