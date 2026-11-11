// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

var normalReasons = []string{
	lodiav1alpha1.ReasonReconciling,
	lodiav1alpha1.ReasonReady,
	lodiav1alpha1.ReasonDeleting,
	lodiav1alpha1.ReasonWaitingForEndpoint,
	lodiav1alpha1.ReasonWaitingForTarget,
	lodiav1alpha1.ReasonWaitingForCertificates,
	lodiav1alpha1.ReasonWaitingForDatastore,
	lodiav1alpha1.ReasonWaitingForAPIServer,
	lodiav1alpha1.ReasonWaitingForAccess,
	lodiav1alpha1.ReasonWaitingForBackup,
	lodiav1alpha1.ReasonNoBackupStore,
	lodiav1alpha1.ReasonIssued,
	lodiav1alpha1.ReasonIssuing,
	lodiav1alpha1.ReasonValid,
	lodiav1alpha1.ReasonBound,
	lodiav1alpha1.ReasonBinding,
	lodiav1alpha1.ReasonProvisioning,
	lodiav1alpha1.ReasonRestoring,
	lodiav1alpha1.ReasonAccepted,
	lodiav1alpha1.ReasonApplied,
	lodiav1alpha1.ReasonApplying,
	lodiav1alpha1.ReasonAvailable,
	lodiav1alpha1.ReasonRolloutPending,
	lodiav1alpha1.ReasonReachable,
	lodiav1alpha1.ReasonLeading,
	lodiav1alpha1.ReasonHeld,
	lodiav1alpha1.ReasonAwaitingRelease,
	lodiav1alpha1.ReasonSourceExists,
	lodiav1alpha1.ReasonBackupSucceeded,
	lodiav1alpha1.ReasonSnapshotsPending,
	lodiav1alpha1.ReasonSnapshotting,
	lodiav1alpha1.ReasonCopying,
	lodiav1alpha1.ReasonCompleted,
	lodiav1alpha1.ReasonNotHeld,
	lodiav1alpha1.ReasonDisabled,
	lodiav1alpha1.ReasonIdle,
	lodiav1alpha1.ReasonPaused,
	lodiav1alpha1.ReasonRollingOut,
	lodiav1alpha1.ReasonUpgrading,
	lodiav1alpha1.ReasonMaintaining,
	lodiav1alpha1.ReasonWaitingForWindow,
	lodiav1alpha1.ReasonWaitingForReady,
	lodiav1alpha1.ReasonFleetLimit,
	lodiav1alpha1.ReasonWaitingForNodes,
	lodiav1alpha1.ReasonControllerManagerHeld,
	lodiav1alpha1.ReasonLatestGenerationNotObserved,
}

func TestEveryReasonIsClassified(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "conditions.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	normal := map[string]bool{}
	for _, reason := range normalReasons {
		normal[reason] = true
	}
	found := 0
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if !strings.HasPrefix(name.Name, "Reason") {
					continue
				}
				reason, err := strconv.Unquote(value.Values[i].(*ast.BasicLit).Value)
				if err != nil {
					t.Fatalf("%s: %v", name.Name, err)
				}
				found++
				_, warning := lodiav1alpha1.WarningReasons[reason]
				switch {
				case warning && normal[reason]:
					t.Errorf("%s is both in WarningReasons and Normal", name.Name)
				case !warning && !normal[reason]:
					t.Errorf("%s is neither in WarningReasons nor Normal", name.Name)
				}
			}
		}
	}
	if want := len(lodiav1alpha1.WarningReasons) + len(normal); found != want {
		t.Errorf("conditions.go declares %d reasons, WarningReasons and Normal name %d", found, want)
	}
}
