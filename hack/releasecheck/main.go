// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command releasecheck refuses a release tag before it is pushed, naming every
// rule the tag breaks, in the repository's root:
//
//	releasecheck tag [--commit=HEAD] vX.Y.Z
//
// It reads GitHub with $GITHUB_TOKEN, as the repository of origin, and the
// tags and files of the clone.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/go-github/v92/github"
)

const usage = "usage: releasecheck tag [--commit=HEAD] vX.Y.Z"

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "tag" {
		return errors.New(usage)
	}
	flags := flag.NewFlagSet("tag", flag.ContinueOnError)
	commit := flags.String("commit", "HEAD", "the commit the tag will name")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New(usage)
	}
	owner, repo, err := originRepository(ctx, ".")
	if err != nil {
		return err
	}
	gh, err := newGitHub(owner, repo, os.Getenv("GITHUB_TOKEN"), github.WithTimeout(time.Minute))
	if err != nil {
		return err
	}
	sha, err := checkTag(ctx, ".", gh, flags.Arg(0), *commit)
	if err != nil {
		return err
	}
	fmt.Printf("%s may be tagged on %s\n", flags.Arg(0), sha)
	return nil
}
