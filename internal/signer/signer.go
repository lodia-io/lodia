// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package signer implements kube-apiserver's external ServiceAccount token
// signer (KEP-740) on a Unix socket, serving the keys of a mounted Secret.
package signer

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	externaljwtv1 "k8s.io/externaljwt/apis/v1"
)

// Signer signs with the key set it last loaded from its directory and keeps
// that set when a later version of the directory does not load.
type Signer struct {
	externaljwtv1.UnimplementedExternalJWTSignerServer

	dir                string
	maxTokenExpiration time.Duration
	refreshHint        time.Duration
	log                *slog.Logger
	current            atomic.Pointer[keySet]
}

// New returns a Signer for the keys in dir; it has no key set until Reload or
// Watch loads one.
func New(dir string, maxTokenExpiration, refreshHint time.Duration, log *slog.Logger) *Signer {
	return &Signer{dir: dir, maxTokenExpiration: maxTokenExpiration, refreshHint: refreshHint, log: log}
}

// Ready reports whether a key set has loaded.
func (s *Signer) Ready() bool {
	return s.current.Load() != nil
}

// Reload reads the directory and serves its key set if it loads.
func (s *Signer) Reload() {
	next, err := loadKeySet(s.dir)
	if err != nil {
		s.log.Error("key set not loaded, serving the last one", "dir", s.dir, "error", err)
		return
	}
	prev := s.current.Load()
	next.loadedAt = time.Now()
	if prev != nil && slices.Equal(prev.kids(), next.kids()) {
		if prev.signingKID == next.signingKID {
			return
		}
		next.loadedAt = prev.loadedAt
	}
	s.current.Store(next)
	s.log.Info("key set loaded", "signing", next.signingKID, "keys", next.kids())
}

// Watch loads the directory, then reloads it whenever it changes, until ctx
// is done.
func (s *Signer) Watch(ctx context.Context) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(s.dir); err != nil {
		return err
	}
	s.Reload()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-w.Events:
			s.Reload()
		case err := <-w.Errors:
			s.log.ErrorContext(ctx, "watch the key directory", "dir", s.dir, "error", err)
		}
	}
}

// Listen replaces whatever file is at path with a Unix socket only its owner
// can connect to.
func Listen(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = lis.Close()
		return nil, err
	}
	return lis, nil
}

// Serve serves gRPC on lis until ctx is done, then stops gracefully.
func (s *Signer) Serve(ctx context.Context, lis net.Listener) error {
	srv := grpc.NewServer()
	externaljwtv1.RegisterExternalJWTSignerServer(srv, s)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		srv.GracefulStop()
	}()
	if err := srv.Serve(lis); err != nil {
		return err
	}
	<-stopped
	return nil
}

func (s *Signer) Sign(_ context.Context, req *externaljwtv1.SignJWTRequest) (*externaljwtv1.SignJWTResponse, error) {
	set := s.current.Load()
	if set == nil {
		return nil, status.Error(codes.Unavailable, "no key set loaded")
	}
	// Strict decoding accepts only the canonical encoding, which is the one
	// go-jose re-encodes, so the signed claims are the received ones byte for byte.
	payload, err := base64.RawURLEncoding.Strict().DecodeString(req.GetClaims())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "claims are not unpadded base64url: %v", err)
	}
	jws, err := set.signer.Sign(payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "sign: %v", err)
	}
	detached, err := jws.DetachedCompactSerialize()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "serialize: %v", err)
	}
	header, signature, _ := strings.Cut(detached, "..")
	return &externaljwtv1.SignJWTResponse{Header: header, Signature: signature}, nil
}

func (s *Signer) FetchKeys(context.Context, *externaljwtv1.FetchKeysRequest) (*externaljwtv1.FetchKeysResponse, error) {
	set := s.current.Load()
	if set == nil {
		return nil, status.Error(codes.Unavailable, "no key set loaded")
	}
	return &externaljwtv1.FetchKeysResponse{
		Keys:               set.keys,
		DataTimestamp:      timestamppb.New(set.loadedAt),
		RefreshHintSeconds: int64(s.refreshHint.Seconds()),
	}, nil
}

func (s *Signer) Metadata(context.Context, *externaljwtv1.MetadataRequest) (*externaljwtv1.MetadataResponse, error) {
	return &externaljwtv1.MetadataResponse{MaxTokenExpirationSeconds: int64(s.maxTokenExpiration.Seconds())}, nil
}
