package main

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
)

const testParamsOrder = "test_params_order"

// newTestParamsOrder returns an analyzer to detect parameters of test helper functions.
// testing.TB arguments should always be passed as first parameter, followed by context.Context.
//
//   - Bad
//     func testHelper(paramA string, t *testing.T)
//
//   - Bad
//     func testHelper(ctx context.Context, t *testing.T)
//
//   - Bad
//     func testHelper(t *testing.T, paramA string, ctx context.Context)
//
//   - Good
//     func testHelper(t *testing.T)
//
//   - Good
//     func testHelper(t *testing.T, ctx context.Context)
//
//   - Good
//     func testHelper(t *testing.T, ctx context.Context, paramA string)
//
// For more information:
// https://gitlab.com/gitlab-org/gitaly/-/blob/master/STYLE.md?ref_type=heads#test-helpers
func newTestParamsOrder() *analysis.Analyzer {
	return &analysis.Analyzer{
		Name: testParamsOrder,
		Doc:  `testing.TB arguments should always be passed as first parameter, followed by context.Context if required: https://gitlab.com/gitlab-org/gitaly/-/blob/master/STYLE.md?ref_type=heads#test-helpers`,
		Run:  runTestParamsOrder,
	}
}

func runTestParamsOrder(pass *analysis.Pass) (interface{}, error) {
	for _, file := range pass.Files {
		ast.Inspect(file, func(n ast.Node) bool {
			if decl, ok := n.(*ast.FuncDecl); ok {
				analyzeTestHelperParams(pass, decl)
			}
			return true
		})
	}
	return nil, nil
}

func analyzeTestHelperParams(pass *analysis.Pass, decl *ast.FuncDecl) {
	params := decl.Type.Params
	// Either case is fine:
	// - No param. Out of scope.
	// - The only param is not *testing.T or *testing.B. Out of scope.
	// - The only param is *testing.T or *testing.B. This is perfectly fine.
	if params.NumFields() <= 1 {
		return
	}

	testingParamIndex := -1
	contextContextIndex := -1
	for index, field := range params.List {
		fieldType := pass.TypesInfo.TypeOf(field.Type)
		if isTestingParam(fieldType) {
			// More than one *testing.T or *testing.B parameter
			if testingParamIndex != -1 {
				pass.Report(analysis.Diagnostic{
					Pos:     params.Pos(),
					End:     params.End(),
					Message: "more than one *testing.T or *testing.B parameter",
				})
				return
			}
			testingParamIndex = index
		}
		if isContextContext(fieldType) {
			contextContextIndex = index
		}
	}

	switch {
	case testingParamIndex == -1:
		// No *testing.T or *testing.B parameter is present. The function is probably not a test helper function.
		return
	case testingParamIndex != 0:
		testingParamField := params.List[testingParamIndex]
		pass.Report(analysis.Diagnostic{
			Pos:     testingParamField.Pos(),
			End:     testingParamField.End(),
			Message: "testing.TB argument should always be passed as first parameter",
		})
	case contextContextIndex != -1 && contextContextIndex != 1:
		contextContextField := params.List[contextContextIndex]
		pass.Report(analysis.Diagnostic{
			Pos:     contextContextField.Pos(),
			End:     contextContextField.End(),
			Message: "context.Context should follow after testing.TB",
		})
	}
}
