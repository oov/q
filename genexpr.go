// +build ignore

package main

import (
	"bytes"
	"go/format"
	"log"
	"os"
	"text/template"
)

func main() {
	type nameOp struct {
		Name string
		Op   string
	}
	type nameBool struct {
		Name string
		Bool bool
	}
	vars := struct {
		SimpleExpr  []nameOp
		EqExpr      []nameBool
		InExpr      []nameBool
		LogicalExpr []nameOp
	}{
		SimpleExpr: []nameOp{
			{Name: "gt", Op: ">"},
			{Name: "gte", Op: ">="},
			{Name: "lt", Op: "<"},
			{Name: "lte", Op: "<="},
			{Name: "simpleIn", Op: "IN"},
			{Name: "simpleNotIn", Op: "NOT IN"},
		},
		EqExpr: []nameBool{
			{Name: "eq", Bool: true},
			{Name: "neq", Bool: false},
		},
		InExpr: []nameBool{
			{Name: "in", Bool: true},
			{Name: "notIn", Bool: false},
		},
		LogicalExpr: []nameOp{
			{Name: "and", Op: "AND"},
			{Name: "or", Op: "OR"},
		},
	}

	b := bytes.NewBufferString("")
	t, err := template.New("").Parse(tpl)
	if err != nil {
		log.Fatal(err)
	}
	if err = t.Execute(b, vars); err != nil {
		log.Fatal(err)
	}
	buf, err := format.Source(b.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create("exprs.go")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(buf); err != nil {
		log.Fatal(err)
	}
}

var tpl = `package q

import "github.com/oov/q/qutil"

{{range .SimpleExpr}}
type {{.Name}}Expr struct{
	Left interface{}
	Right interface{}
}
func (e *{{.Name}}Expr) String() string               { return expressionToString(e) }
func (e *{{.Name}}Expr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *{{.Name}}Expr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " {{.Op}} "...)
	buf = writeIntf(e.Right, ctx, buf)
	return buf
}
{{end}}

{{range .EqExpr}}
type {{.Name}}Expr struct{
	Left interface{}
	Right interface{}
}
func (e {{.Name}}Expr) String() string               { return expressionToString(e) }
func (e {{.Name}}Expr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e {{.Name}}Expr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	lv, rv := e.Left, e.Right
	if lv == nil {
		lv, rv = rv, lv
	}
	if rv == nil {
		buf = writeIntf(lv, ctx, buf)
		return append(buf, " IS {{if not .Bool}}NOT {{end}}NULL"...)
	}

	buf = writeIntf(lv, ctx, buf)
	buf = append(buf, " {{if not .Bool}}!{{end}}= "...)
	buf = writeIntf(rv, ctx, buf)
	return buf
}
{{end}}

{{range .InExpr}}
type {{.Name}}Expr struct {
	Left  interface{}
	Right inVariable
}

func (e *{{.Name}}Expr) String() string               { return expressionToString(e) }
func (e *{{.Name}}Expr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *{{.Name}}Expr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	if len(e.Right) == 0 {
		// x {{if not .Bool}}NOT {{end}}IN () is invaild syntax.
		// But at the same time, a result is a obvious expression.
		// So replace the alternative valid expression which is the same result.
		return append(buf, "'IN' {{if not .Bool}}!{{end}}= '()'"...)
	}

	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " {{if not .Bool}}NOT {{end}}IN "...)
	buf = e.Right.WriteExpression(ctx, buf)
	return buf
}
{{end}}

{{range .LogicalExpr}}
type {{.Name}}Expr struct {
	Exprs []Expression
}

func (e *{{.Name}}Expr) String() string               { return expressionToString(e) }
func (e *{{.Name}}Expr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *{{.Name}}Expr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	switch len(e.Exprs) {
	case 0:
		buf = append(buf, "('empty' = '{{.Op}}')"...)
		return buf
	case 1:
		return e.Exprs[0].WriteExpression(ctx, buf)
	}
	buf = append(buf, '(')
	buf = e.Exprs[0].WriteExpression(ctx, buf)
	buf = append(buf, ')')
	for _, cd := range e.Exprs[1:] {
		buf = append(buf, "{{.Op}}("...)
		buf = cd.WriteExpression(ctx, buf)
		buf = append(buf, ')')
	}
	return buf
}

func (e *{{.Name}}Expr) Add(exprs ...Expression) Expressions {
	e.Exprs = append(e.Exprs, exprs...)
	return e
}

func (e *{{.Name}}Expr) Len() int {
	return len(e.Exprs)
}
{{end}}
`
