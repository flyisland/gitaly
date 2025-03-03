package main

import (
	"fmt"
	"go/importer"
	"go/types"
)

var testingTB = mustFindPackageInterface("testing", "TB")

func mustFindBuiltinInterface(name string) *types.Interface {
	obj := types.Universe.Lookup(name)
	if obj == nil {
		panic(fmt.Sprintf("fail to find universe interface %q", name))
	}
	return obj.Type().Underlying().(*types.Interface)
}

func mustFindPackageInterface(pkgName string, name string) *types.Interface {
	pkg, err := importer.Default().Import(pkgName)
	if err != nil {
		panic(fmt.Sprintf("fail to find package %q", pkgName))
	}
	obj := pkg.Scope().Lookup(name)
	if obj == nil {
		panic(fmt.Sprintf("fail to find universe interface %q of package %q", name, pkgName))
	}
	return obj.Type().Underlying().(*types.Interface)
}

func isTestingParam(t types.Type) bool {
	if ptr, ok := t.(*types.Pointer); ok {
		if named, ok := ptr.Elem().(*types.Named); ok {
			obj := named.Obj()
			return obj.Pkg() != nil && obj.Pkg().Path() == "testing" && (obj.Name() == "T" || obj.Name() == "B")
		}
	} else if t.Underlying().String() == testingTB.String() {
		return true
	}
	return false
}

func isContextContext(t types.Type) bool {
	if named, ok := t.(*types.Named); ok {
		obj := named.Obj()
		return obj.Pkg() != nil && obj.Pkg().Path() == "context" && obj.Name() == "Context"
	}
	return false
}
