// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package authnconfig

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/parser"
	"k8s.io/apimachinery/pkg/util/version"
	authenticationcel "k8s.io/apiserver/pkg/authentication/cel"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
)

// compiler compiles claim and user expressions in the NewExpressions
// environment of one minor, since upstream's compiler offers only every
// library of its own release, with the operator's cel-go. Before escapesFrom
// it emulates cel-go v0.26.0 on it, refusing what v0.26.0 reads otherwise: a
// backtick-escaped identifier, which its parser refuses (google/cel-go#1295),
// and a dot-prefixed identifier that names a comprehension variable around it,
// which its checker resolves to that variable and later versions to a global
// (google/cel-go#1256). It agrees with each minor's kube-apiserver on the
// corpus of kubeapiserver_test.go but for the refusals that test lists; a
// difference outside the corpus can remain.
// @todo: mirrors k8s.io/apiserver's unexported compiler, mustBuildEnvs and buildUserType (pkg/authentication/cel/compile.go at v0.37.1), and the parser and the name resolution of cel-go v0.26.0, which 1.35 and 1.36 link; drop it for upstream's validation of a given version and gates once k8s.io/apiserver has one (todo.md).
type compiler struct {
	claims, user *cel.Env
	unescaped    *parser.Parser
}

// escapesFrom is the first minor that links cel-go v0.29.2, which parses
// backtick-escaped identifiers and resolves a dot-prefixed one to a global;
// 1.35 and 1.36 link v0.26.0.
var escapesFrom = version.MajorMinor(1, 37)

var compilers sync.Map

// compilerOf is the compiler of minor, "1.35", built once.
func compilerOf(minor string) (authenticationcel.Compiler, error) {
	if c, ok := compilers.Load(minor); ok {
		return c.(compiler), nil
	}
	v, err := version.ParseMajorMinor(minor)
	if err != nil {
		return nil, err
	}
	base := environment.MustBaseEnvSet(v)
	extend := func(name string, declType *apiservercel.DeclType) (*cel.Env, error) {
		set, err := base.Extend(environment.VersionedOptions{
			IntroducedVersion: version.MajorMinor(1, 0),
			EnvOptions:        []cel.EnvOption{cel.Variable(name, declType.CelType())},
			DeclTypes:         []*apiservercel.DeclType{declType},
		})
		if err != nil {
			return nil, err
		}
		return set.Env(environment.NewExpressions)
	}
	field := func(name string, declType *apiservercel.DeclType) *apiservercel.DeclField {
		return apiservercel.NewDeclField(name, declType, false, nil, nil)
	}
	user := apiservercel.NewObjectType("kubernetes.UserInfo", map[string]*apiservercel.DeclField{
		"username": field("username", apiservercel.StringType),
		"uid":      field("uid", apiservercel.StringType),
		"groups":   field("groups", apiservercel.NewListType(apiservercel.StringType, -1)),
		"extra":    field("extra", apiservercel.NewMapType(apiservercel.StringType, apiservercel.NewListType(apiservercel.StringType, -1), -1)),
	})
	claims, err := extend("claims", apiservercel.NewMapType(apiservercel.StringType, apiservercel.AnyType, -1))
	if err != nil {
		return nil, err
	}
	users, err := extend("user", user)
	if err != nil {
		return nil, err
	}
	built := compiler{claims: claims, user: users}
	if v.LessThan(escapesFrom) {
		if built.unescaped, err = parser.NewParser(parser.Macros(claims.Macros()...), parser.EnableOptionalSyntax(claims.HasLibrary("cel.lib.optional")), parser.EnableIdentEscapeSyntax(false)); err != nil {
			return nil, err
		}
	}
	c, _ := compilers.LoadOrStore(minor, built)
	return c.(compiler), nil
}

func (c compiler) CompileClaimsExpression(a authenticationcel.ExpressionAccessor) (authenticationcel.CompilationResult, error) {
	return c.compile(c.claims, a)
}

func (c compiler) CompileUserExpression(a authenticationcel.ExpressionAccessor) (authenticationcel.CompilationResult, error) {
	return c.compile(c.user, a)
}

func (c compiler) compile(env *cel.Env, a authenticationcel.ExpressionAccessor) (authenticationcel.CompilationResult, error) {
	failed := func(detail string, errorType apiservercel.ErrorType) (authenticationcel.CompilationResult, error) {
		return authenticationcel.CompilationResult{}, &apiservercel.Error{Type: errorType, Detail: detail}
	}
	if c.unescaped != nil {
		parsed, errs := c.unescaped.Parse(common.NewTextSource(a.GetExpression()))
		if len(errs.GetErrors()) > 0 {
			return failed("compilation failed: "+cel.NewIssues(errs).String(), apiservercel.ErrorTypeInvalid)
		}
		if name := shadowed(parsed.Expr(), nil); name != "" {
			return failed(fmt.Sprintf("compilation failed: %s names the comprehension variable %s at this minor and a global at later ones (google/cel-go#1256); drop the dot or rename the variable", name, name[1:]), apiservercel.ErrorTypeInvalid)
		}
	}
	ast, issues := env.Compile(a.GetExpression())
	if issues != nil {
		return failed("compilation failed: "+issues.String(), apiservercel.ErrorTypeInvalid)
	}
	returns := a.ReturnTypes()
	found := false
	for _, t := range returns {
		found = found || ast.OutputType() == t || t == cel.AnyType
	}
	if !found {
		if len(returns) == 1 {
			return failed(fmt.Sprintf("must evaluate to %v", returns[0].String()), apiservercel.ErrorTypeInvalid)
		}
		return failed(fmt.Sprintf("must evaluate to one of %v", returns), apiservercel.ErrorTypeInvalid)
	}
	program, err := env.Program(ast)
	if err != nil {
		return failed("program instantiation failed: "+err.Error(), apiservercel.ErrorTypeInternal)
	}
	return authenticationcel.CompilationResult{Program: program, AST: ast, ExpressionAccessor: a}, nil
}

// shadowed is the first dot-prefixed identifier in e that names a variable of
// a comprehension around it, bound naming those of the comprehensions around
// e: the accumulator in the loop and the result, the iteration variables in
// the loop alone, as cel-go's checker scopes them.
func shadowed(e ast.Expr, bound []string) string {
	first := func(exprs ...ast.Expr) string {
		for _, x := range exprs {
			if name := shadowed(x, bound); name != "" {
				return name
			}
		}
		return ""
	}
	switch e.Kind() {
	case ast.IdentKind:
		if name, dotted := strings.CutPrefix(e.AsIdent(), "."); dotted && slices.Contains(bound, name) {
			return e.AsIdent()
		}
	case ast.SelectKind:
		return first(e.AsSelect().Operand())
	case ast.CallKind:
		call := e.AsCall()
		if call.IsMemberFunction() {
			return first(append([]ast.Expr{call.Target()}, call.Args()...)...)
		}
		return first(call.Args()...)
	case ast.ListKind:
		return first(e.AsList().Elements()...)
	case ast.MapKind:
		for _, entry := range e.AsMap().Entries() {
			if name := first(entry.AsMapEntry().Key(), entry.AsMapEntry().Value()); name != "" {
				return name
			}
		}
	case ast.StructKind:
		for _, field := range e.AsStruct().Fields() {
			if name := first(field.AsStructField().Value()); name != "" {
				return name
			}
		}
	case ast.ComprehensionKind:
		c := e.AsComprehension()
		if name := first(c.IterRange(), c.AccuInit()); name != "" {
			return name
		}
		accumulating := append(slices.Clone(bound), c.AccuVar())
		looping := append(slices.Clone(accumulating), c.IterVar())
		if c.HasIterVar2() {
			looping = append(looping, c.IterVar2())
		}
		if name := shadowed(c.LoopCondition(), looping); name != "" {
			return name
		}
		if name := shadowed(c.LoopStep(), looping); name != "" {
			return name
		}
		return shadowed(c.Result(), accumulating)
	case ast.LiteralKind, ast.UnspecifiedExprKind:
	}
	return ""
}
