// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command etcdbinary prints the path of etcd's release binary at
// versions.EtcdVersion for this machine, for make test, which hands it to the
// tests in LODIA_ETCD_BINARY, with etcdutl beside it. It fetches the release
// into the directory it is given when either binary is not there, and checks
// the archive against the SHA-256 versions.EtcdArchives pins first.
package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"go.lodia.io/lodia/internal/versions"
)

const (
	releases   = "https://github.com/etcd-io/etcd/releases/download"
	maxArchive = 128 << 20
	maxBinary  = 256 << 20
)

// binaries are what the cache holds of the archive, in the order they are
// written: etcd last, so that a cache that holds etcd holds both.
var binaries = []string{"etcdutl", "etcd"}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: etcdbinary <cache directory>")
		os.Exit(2)
	}
	binary, err := run(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "etcdbinary:", err)
		os.Exit(1)
	}
	fmt.Println(binary)
}

func run(cache string) (string, error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	dir, err := filepath.Abs(cache)
	if err != nil {
		return "", err
	}
	return ensure(ctx, releases, dir, versions.EtcdVersion, runtime.GOOS, runtime.GOARCH, versions.EtcdArchives)
}

func archiveName(version, goos, goarch string) (string, error) {
	var extension string
	switch goos {
	case "darwin":
		extension = "zip"
	case "linux":
		extension = "tar.gz"
	default:
		return "", fmt.Errorf("etcd's release has no binary for %s", goos)
	}
	if goarch != "amd64" && goarch != "arm64" {
		return "", fmt.Errorf("etcd's release has no binary for %s/%s", goos, goarch)
	}
	return fmt.Sprintf("etcd-%s-%s-%s.%s", version, goos, goarch, extension), nil
}

func ensure(ctx context.Context, base, dir, version, goos, goarch string, pinned map[string]string) (string, error) {
	name, err := archiveName(version, goos, goarch)
	if err != nil {
		return "", err
	}
	want, ok := pinned[name]
	if !ok {
		return "", fmt.Errorf("no SHA-256 is pinned for %s", name)
	}
	target := filepath.Join(dir, version+"-"+goos+"-"+goarch)
	if !slices.ContainsFunc(binaries, func(binary string) bool { return !executable(filepath.Join(target, binary)) }) {
		return filepath.Join(target, "etcd"), nil
	}
	fmt.Fprintf(os.Stderr, "etcdbinary: fetching %s/%s/%s\n", base, version, name)
	archive, err := get(ctx, base+"/"+version+"/"+name, maxArchive)
	if err != nil {
		return "", err
	}
	if got := sha256.Sum256(archive); hex.EncodeToString(got[:]) != want {
		return "", fmt.Errorf("%s has the SHA-256 %x, not the pinned %s", name, got, want)
	}
	if err := os.MkdirAll(target, 0o750); err != nil { //nolint:gosec // G703: the directory is this program's argument, which make test gives it
		return "", err
	}
	files := map[string]*os.File{}
	defer func() {
		for _, file := range files {
			_ = file.Close()
			_ = os.Remove(file.Name())
		}
	}()
	writers := map[string]io.Writer{}
	for _, binary := range binaries {
		file, err := os.CreateTemp(target, binary+"-*")
		if err != nil {
			return "", err
		}
		files[binary], writers[binary] = file, file
	}
	if strings.HasSuffix(name, ".zip") {
		err = fromZip(writers, archive)
	} else {
		err = fromTar(writers, archive)
	}
	if err != nil {
		return "", fmt.Errorf("%s: %w", name, err)
	}
	for _, binary := range binaries {
		file := files[binary]
		err := file.Chmod(0o750)
		if closed := file.Close(); err == nil {
			err = closed
		}
		if err == nil {
			err = os.Rename(file.Name(), filepath.Join(target, binary)) //nolint:gosec // G703: the directory is this program's argument, which make test gives it
		}
		if err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
	}
	return filepath.Join(target, "etcd"), nil
}

func executable(path string) bool {
	info, err := os.Stat(path) //nolint:gosec // G703: the directory is this program's argument, which make test gives it
	return err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o100 != 0
}

func get(ctx context.Context, url string, limit int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, res.Status)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("GET %s: more than %d bytes", url, limit)
	}
	return body, nil
}

// fromZip and fromTar copy each binary the archive holds under the name of a
// writer into that writer.
func fromZip(writers map[string]io.Writer, archive []byte) error {
	reader, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return err
	}
	copied := map[string]bool{}
	for _, f := range reader.File {
		binary := path.Base(f.Name)
		if dst, ok := writers[binary]; ok && !copied[binary] && !f.FileInfo().IsDir() {
			if err := copyZipped(dst, binary, f); err != nil {
				return err
			}
			copied[binary] = true
		}
	}
	return missing(writers, copied)
}

func copyZipped(dst io.Writer, binary string, f *zip.File) error {
	src, err := f.Open()
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	return copyBinary(dst, binary, src)
}

func fromTar(writers map[string]io.Writer, archive []byte) error {
	unzipped, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return err
	}
	defer func() { _ = unzipped.Close() }()
	reader := tar.NewReader(unzipped)
	copied := map[string]bool{}
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return missing(writers, copied)
		}
		if err != nil {
			return err
		}
		binary := path.Base(header.Name)
		if dst, ok := writers[binary]; ok && !copied[binary] && header.Typeflag == tar.TypeReg {
			if err := copyBinary(dst, binary, reader); err != nil {
				return err
			}
			copied[binary] = true
		}
	}
}

func missing(writers map[string]io.Writer, copied map[string]bool) error {
	for _, binary := range binaries {
		if _, wanted := writers[binary]; wanted && !copied[binary] {
			return fmt.Errorf("no %s in the archive", binary)
		}
	}
	return nil
}

func copyBinary(dst io.Writer, binary string, src io.Reader) error {
	n, err := io.CopyN(dst, src, maxBinary+1)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("%s is more than %d bytes (read %d)", binary, maxBinary, n)
	}
	return err
}
