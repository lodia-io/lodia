// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command etcdbinary prints the path of etcd's release binary at
// versions.EtcdVersion for this machine, for make test, which hands it to the
// tests in LODIA_ETCD_BINARY. It fetches the release into the directory it is
// given when the binary is not there, and checks the archive against the
// release's SHA256SUMS first.
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
	"strings"
	"time"

	"go.lodia.io/lodia/internal/versions"
)

const (
	releases   = "https://github.com/etcd-io/etcd/releases/download"
	maxArchive = 128 << 20
	maxBinary  = 256 << 20
)

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
	return ensure(ctx, releases, dir, versions.EtcdVersion, runtime.GOOS, runtime.GOARCH)
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

func ensure(ctx context.Context, base, dir, version, goos, goarch string) (string, error) {
	name, err := archiveName(version, goos, goarch)
	if err != nil {
		return "", err
	}
	target := filepath.Join(dir, version+"-"+goos+"-"+goarch, "etcd")
	if info, err := os.Stat(target); err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o100 != 0 {
		return target, nil
	}
	fmt.Fprintf(os.Stderr, "etcdbinary: fetching %s/%s/%s\n", base, version, name)
	sums, err := get(ctx, base+"/"+version+"/SHA256SUMS", 1<<20)
	if err != nil {
		return "", err
	}
	want, err := checksum(sums, name)
	if err != nil {
		return "", err
	}
	archive, err := get(ctx, base+"/"+version+"/"+name, maxArchive)
	if err != nil {
		return "", err
	}
	if got := sha256.Sum256(archive); hex.EncodeToString(got[:]) != want {
		return "", fmt.Errorf("%s has the SHA-256 %x, its release's SHA256SUMS says %s", name, got, want)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return "", err
	}
	file, err := os.CreateTemp(filepath.Dir(target), "etcd-*")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if strings.HasSuffix(name, ".zip") {
		err = fromZip(file, archive)
	} else {
		err = fromTar(file, archive)
	}
	if err == nil {
		err = file.Chmod(0o750)
	}
	if closed := file.Close(); err == nil {
		err = closed
	}
	if err != nil {
		return "", fmt.Errorf("%s: %w", name, err)
	}
	if err := os.Rename(file.Name(), target); err != nil {
		return "", err
	}
	return target, nil
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

func checksum(sums []byte, name string) (string, error) {
	for line := range strings.Lines(string(sums)) {
		if fields := strings.Fields(line); len(fields) == 2 && fields[1] == name {
			return strings.ToLower(fields[0]), nil
		}
	}
	return "", fmt.Errorf("SHA256SUMS lists no %s", name)
}

func fromZip(dst io.Writer, archive []byte) error {
	reader, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return err
	}
	for _, f := range reader.File {
		if path.Base(f.Name) == "etcd" && !f.FileInfo().IsDir() {
			return copyZipped(dst, f)
		}
	}
	return errors.New("no etcd in the archive")
}

func copyZipped(dst io.Writer, f *zip.File) error {
	src, err := f.Open()
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	return copyBinary(dst, src)
}

func fromTar(dst io.Writer, archive []byte) error {
	unzipped, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return err
	}
	defer func() { _ = unzipped.Close() }()
	reader := tar.NewReader(unzipped)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return errors.New("no etcd in the archive")
		}
		if err != nil {
			return err
		}
		if header.Typeflag == tar.TypeReg && path.Base(header.Name) == "etcd" {
			return copyBinary(dst, reader)
		}
	}
}

func copyBinary(dst io.Writer, src io.Reader) error {
	n, err := io.CopyN(dst, src, maxBinary+1)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("etcd is more than %d bytes (read %d)", maxBinary, n)
	}
	return err
}
