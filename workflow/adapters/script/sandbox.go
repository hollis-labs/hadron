package script

import (
	"reflect"
	"sort"
	"strings"

	"github.com/dop251/goja/ast"
	"github.com/dop251/goja/file"
)

type sandboxViolation struct {
	capability string
	idx        file.Idx
}

var deniedIdentifiers = map[string]string{
	"require": "module loading", "module": "module loading", "exports": "module loading",
	"process": "process access", "global": "ambient global access", "globalThis": "ambient global access",
	"hadron": "Hadron host access", "Deno": "process access", "Bun": "process access",
	"fetch": "network access", "XMLHttpRequest": "network access", "WebSocket": "network access",
	"Date": "clock access", "performance": "clock access", "crypto": "random or crypto access",
	"console": "console access", "setTimeout": "timer access", "setInterval": "timer access",
	"setImmediate": "timer access", "queueMicrotask": "asynchronous execution",
	"eval": "dynamic code", "Function": "dynamic code", "Proxy": "dynamic code", "Reflect": "dynamic code",
	"WebAssembly": "native module access", "Promise": "asynchronous execution",
	"WeakRef": "nondeterministic garbage-collection access", "FinalizationRegistry": "nondeterministic garbage-collection access",
	"SharedArrayBuffer": "shared memory", "Atomics": "shared memory",
	"Array": "bulk allocation", "ArrayBuffer": "bulk allocation", "DataView": "bulk allocation",
	"BigInt": "bulk allocation", "Map": "bulk allocation", "Set": "bulk allocation",
	"WeakMap": "nondeterministic garbage-collection access", "WeakSet": "nondeterministic garbage-collection access",
	"RegExp": "uninterruptible regular expression", "Intl": "ambient locale access",
	"Int8Array": "bulk allocation", "Uint8Array": "bulk allocation", "Uint8ClampedArray": "bulk allocation",
	"Int16Array": "bulk allocation", "Uint16Array": "bulk allocation", "Int32Array": "bulk allocation",
	"Uint32Array": "bulk allocation", "Float32Array": "bulk allocation", "Float64Array": "bulk allocation",
	"BigInt64Array": "bulk allocation", "BigUint64Array": "bulk allocation",
}

var deniedMembers = map[string]string{
	"constructor": "dynamic code", "prototype": "prototype access", "__proto__": "prototype access",
	"__defineGetter__": "accessor definition", "__defineSetter__": "accessor definition",
	"defineProperty": "accessor definition", "defineProperties": "accessor definition",
	"setPrototypeOf": "prototype mutation", "getPrototypeOf": "prototype access",
	"random": "random access", "now": "clock access",
	"repeat": "bulk allocation", "padStart": "bulk allocation", "padEnd": "bulk allocation",
	"concat": "bulk allocation", "flat": "bulk allocation", "flatMap": "bulk allocation", "join": "bulk allocation",
	"replace": "uninterruptible regular expression", "replaceAll": "uninterruptible regular expression",
	"match": "uninterruptible regular expression", "matchAll": "uninterruptible regular expression",
	"search": "uninterruptible regular expression", "split": "uninterruptible regular expression",
	"localeCompare": "ambient locale access", "toLocaleString": "ambient locale access",
	"toLocaleLowerCase": "ambient locale access", "toLocaleUpperCase": "ambient locale access",
	"compile": "dynamic code",
}

func validateSandbox(program *ast.Program) *sandboxViolation {
	if program == nil {
		return &sandboxViolation{capability: "invalid syntax"}
	}
	return inspectAST(reflect.ValueOf(program))
}

func inspectAST(value reflect.Value) *sandboxViolation {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return inspectAST(value.Elem())
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		if program, ok := value.Interface().(*ast.Program); ok {
			return inspectAST(reflect.ValueOf(program.Body))
		}
		if _, ok := value.Interface().(ast.Node); !ok {
			return nil
		} else if violation := inspectNode(value.Interface()); violation != nil {
			return violation
		}
		return inspectAST(value.Elem())
	}
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
		for index := 0; index < value.Len(); index++ {
			if violation := inspectAST(value.Index(index)); violation != nil {
				return violation
			}
		}
		return nil
	}
	if value.Kind() != reflect.Struct {
		return nil
	}
	for index := 0; index < value.NumField(); index++ {
		field := value.Type().Field(index)
		// Dot property identifiers are inspected with their owning expression;
		// treating them as ambient identifiers would reject ordinary input fields.
		if value.Type() == reflect.TypeOf(ast.DotExpression{}) && field.Name == "Identifier" {
			continue
		}
		if violation := inspectAST(value.Field(index)); violation != nil {
			return violation
		}
	}
	return nil
}

func inspectNode(value any) *sandboxViolation {
	switch node := value.(type) {
	case *ast.Identifier:
		if capability := deniedIdentifiers[node.Name.String()]; capability != "" {
			return &sandboxViolation{capability: capability, idx: node.Idx0()}
		}
	case *ast.DotExpression:
		if capability := deniedMembers[node.Identifier.Name.String()]; capability != "" {
			return &sandboxViolation{capability: capability, idx: node.Identifier.Idx0()}
		}
	case *ast.BracketExpression:
		if literal, ok := node.Member.(*ast.StringLiteral); ok {
			if capability := deniedMembers[literal.Value.String()]; capability != "" {
				return &sandboxViolation{capability: capability, idx: literal.Idx0()}
			}
		}
	case *ast.NewExpression:
		return &sandboxViolation{capability: "object construction", idx: node.Idx0()}
	case *ast.RegExpLiteral:
		return &sandboxViolation{capability: "uninterruptible regular expression", idx: node.Idx0()}
	case *ast.NumberLiteral:
		if strings.HasSuffix(node.Literal, "n") {
			return &sandboxViolation{capability: "bulk allocation", idx: node.Idx0()}
		}
	case *ast.ThisExpression:
		return &sandboxViolation{capability: "ambient global access", idx: node.Idx0()}
	case *ast.WithStatement:
		return &sandboxViolation{capability: "ambient scope access", idx: node.Idx0()}
	case *ast.ClassLiteral:
		return &sandboxViolation{capability: "object construction", idx: node.Idx0()}
	case *ast.AwaitExpression:
		return &sandboxViolation{capability: "asynchronous execution", idx: node.Idx0()}
	case *ast.YieldExpression:
		return &sandboxViolation{capability: "generator execution", idx: node.Idx0()}
	case *ast.FunctionLiteral:
		if node.Async || node.Generator {
			return &sandboxViolation{capability: "asynchronous or generator execution", idx: node.Idx0()}
		}
	case *ast.ArrowFunctionLiteral:
		if node.Async {
			return &sandboxViolation{capability: "asynchronous execution", idx: node.Idx0()}
		}
	case *ast.PropertyKeyed:
		if node.Kind == ast.PropertyKindGet || node.Kind == ast.PropertyKindSet {
			return &sandboxViolation{capability: "accessor definition", idx: node.Idx0()}
		}
	}
	return nil
}

func deniedGlobalNames() []string {
	names := make([]string, 0, len(deniedIdentifiers))
	for name := range deniedIdentifiers {
		if strings.HasPrefix(name, "__hadron_") {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
