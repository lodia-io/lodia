// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"go.lodia.io/lodia/internal/versions"
)

const (
	version = "v9.9.9"
	payload = "the etcd binary"
)

type release struct {
	server   *httptest.Server
	pinned   map[string]string
	requests atomic.Int32
}

func serve(t *testing.T, change func(name string, archive []byte) (pinned, served []byte)) *release {
	t.Helper()
	files := map[string][]byte{}
	r := &release{pinned: map[string]string{}}
	for _, goos := range []string{"darwin", "linux"} {
		for _, goarch := range []string{"amd64", "arm64"} {
			name, err := archiveName(version, goos, goarch)
			if err != nil {
				t.Fatal(err)
			}
			pinned := build(t, name)
			served := pinned
			if change != nil {
				pinned, served = change(name, pinned)
			}
			sum := sha256.Sum256(pinned)
			files[name] = served
			r.pinned[name] = hex.EncodeToString(sum[:])
		}
	}
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
			got, err := ensure(t.Context(), r.server.URL, dir, version, c.goos, c.goarch, r.pinned)
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
	first, err := ensure(t.Context(), r.server.URL, dir, version, "linux", "amd64", r.pinned)
	if err != nil {
		t.Fatal(err)
	}
	fetched := r.requests.Load()
	if fetched != 1 {
		t.Fatalf("the first run made %d requests, want the archive alone", fetched)
	}
	second, err := ensure(t.Context(), r.server.URL, dir, version, "linux", "amd64", r.pinned)
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

func TestAnArchiveThatDiffersFromItsPinnedChecksumLeavesNothingInTheCache(t *testing.T) {
	r := serve(t, func(_ string, archive []byte) ([]byte, []byte) {
		return archive, append(bytes.Clone(archive), 0)
	})
	dir := t.TempDir()
	_, err := ensure(t.Context(), r.server.URL, dir, version, "linux", "amd64", r.pinned)
	if err == nil || !strings.Contains(err.Error(), "not the pinned") {
		t.Fatalf("a tampered archive gave %v, want a mismatch with the pinned SHA-256", err)
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
	_, err := ensure(t.Context(), r.server.URL, dir, version, "linux", "amd64", r.pinned)
	if err == nil || !strings.Contains(err.Error(), "no etcd in the archive") {
		t.Fatalf("an archive without etcd gave %v, want no etcd in the archive", err)
	}
	if left := leftIn(dir); len(left) > 0 {
		t.Errorf("the cache holds %v after a refused archive, want nothing", left)
	}
}

func TestAnArchiveWithoutAPinnedChecksumOrOneTheReleaseLacksIsAnError(t *testing.T) {
	r := serve(t, nil)
	if _, err := ensure(t.Context(), r.server.URL, t.TempDir(), "v0.0.1", "linux", "amd64", r.pinned); err == nil || !strings.Contains(err.Error(), "no SHA-256 is pinned for etcd-v0.0.1-linux-amd64.tar.gz") {
		t.Errorf("an archive without a pinned SHA-256 gave %v, want it refused", err)
	}
	if n := r.requests.Load(); n != 0 {
		t.Errorf("an archive without a pinned SHA-256 made %d requests, want none", n)
	}
	pinned := map[string]string{"etcd-v0.0.1-linux-amd64.tar.gz": r.pinned["etcd-"+version+"-linux-amd64.tar.gz"]}
	if _, err := ensure(t.Context(), r.server.URL, t.TempDir(), "v0.0.1", "linux", "amd64", pinned); err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("a release the server lacks gave %v, want its 404", err)
	}
}

func TestEveryArchiveOfTheEtcdVersionIsPinned(t *testing.T) {
	var names []string
	for _, c := range []struct{ goos, goarch string }{{"darwin", "arm64"}, {"darwin", "amd64"}, {"linux", "amd64"}, {"linux", "arm64"}} {
		name, err := archiveName(versions.EtcdVersion, c.goos, c.goarch)
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
		if sum, err := hex.DecodeString(versions.EtcdArchives[name]); err != nil || len(sum) != sha256.Size {
			t.Errorf("versions.EtcdArchives pins %q for %s, want its SHA-256", versions.EtcdArchives[name], name)
		}
	}
	if got := slices.Sorted(maps.Keys(versions.EtcdArchives)); !slices.Equal(got, slices.Sorted(slices.Values(names))) {
		t.Errorf("versions.EtcdArchives pins %v, want the archives of etcd %s alone: %v", got, versions.EtcdVersion, names)
	}
}

func TestAPlatformEtcdDoesNotReleaseIsAnError(t *testing.T) {
	for _, c := range []struct{ goos, goarch string }{{"windows", "amd64"}, {"linux", "386"}, {"darwin", "ppc64"}} {
		if name, err := archiveName(version, c.goos, c.goarch); err == nil {
			t.Errorf("%s/%s is named %s, want an error", c.goos, c.goarch, name)
		}
	}
}
