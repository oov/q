//go:generate go run genexpr.go

package q

import (
	"fmt"
	"reflect"

	"github.com/oov/q/qutil"
)

// Expression represents expressions.
type Expression interface {
	// C creates Column from Expression.
	C(aliasName ...string) Column
	// for internal use.
	WriteExpression(ctx *qutil.Context, buf []byte) []byte
}

func expressionToString(e Expression) string {
	buf, ctx := qutil.NewContext(e, 32, 1, nil)
	buf = e.WriteExpression(ctx, buf)
	return toString(buf, ctx.Args)
}

func writeIntf(x interface{}, ctx *qutil.Context, buf []byte) []byte {
	if x == nil {
		return append(buf, "NULL"...)
	}
	if v, ok := x.(Expression); ok {
		return v.WriteExpression(ctx, buf)
	}
	ctx.Args = append(ctx.Args, x)
	return ctx.Placeholder.Next(buf)
}

func interfaceToExpression(x interface{}) Expression {
	if x == nil {
		return nullExpr{}
	}
	if v, ok := x.(Expression); ok {
		return v
	}
	return V(x)
}

func columnExpr(e Expression, aliasName ...string) Column {
	r := &exprAsColumn{e}
	if len(aliasName) == 0 {
		return r
	}
	return &columnAlias{Column: r, Alias: aliasName[0]}
}

type nullExpr struct{}

func (e nullExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e nullExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	return append(buf, "NULL"...)
}

func valueToInV(v reflect.Value) inVariable {
	ln := v.Len()
	r := make(inVariable, ln)
	for i := 0; i < ln; i++ {
		r[i] = v.Index(i).Interface()
	}
	return r
}

// Eq creates Expression such as "l = r".
// But when you pass nil to one of a pair, Eq creates "x IS NULL" instead.
// In the same way, when you pass slice of any type to r, Eq creates "x IN (?)".
func Eq(l, r interface{}) Expression {
	if rv := reflect.ValueOf(r); rv.Kind() == reflect.Slice {
		if _, ok := r.(Expression); !ok {
			return &inExpr{Left: l, Right: valueToInV(rv)}
		}
	}
	return &eqExpr{Left: l, Right: r}
}

// Neq creates Expression such as "l != r".
// But when you pass nil to one of a pair, Neq creates "x IS NOT NULL" instead.
// In the same way, when you pass slice of any type to r, Neq creates "x NOT IN (?)".
func Neq(l, r interface{}) Expression {
	if rv := reflect.ValueOf(r); rv.Kind() == reflect.Slice {
		if _, ok := r.(Expression); !ok {
			return &notInExpr{Left: l, Right: valueToInV(rv)}
		}
	}
	return &neqExpr{Left: l, Right: r}
}

// In creates Expression such as "l IN r".
func In(l, r interface{}) Expression {
	if rv := reflect.ValueOf(r); rv.Kind() == reflect.Slice {
		if _, ok := r.(Expression); !ok {
			return &inExpr{Left: l, Right: valueToInV(rv)}
		}
	}
	return &simpleInExpr{Left: l, Right: r}
}

// NotIn creates Expression such as "l NOT IN r".
func NotIn(l, r interface{}) Expression {
	if rv := reflect.ValueOf(r); rv.Kind() == reflect.Slice {
		if _, ok := r.(Expression); !ok {
			return &notInExpr{Left: l, Right: valueToInV(rv)}
		}
	}
	return &simpleNotInExpr{Left: l, Right: r}
}

// Gt creates Expression such as "l > r".
func Gt(l, r interface{}) Expression { return &gtExpr{Left: l, Right: r} }

// Gte creates Expression such as "l >= r".
func Gte(l, r interface{}) Expression { return &gteExpr{Left: l, Right: r} }

// Lt creates Expression such as "l < r".
func Lt(l, r interface{}) Expression { return &ltExpr{Left: l, Right: r} }

// Lte creates Expression such as "l <= r".
func Lte(l, r interface{}) Expression { return &lteExpr{Left: l, Right: r} }

// Expressions represents combination of an expression.
type Expressions Expression

// And creates Expression such as "(exprs[0])AND(exprs[1])AND(exprs[2])".
//
// If you output expression which isn't adding Expression at all,
// it generates "('empty' = 'AND')".
func And(exprs ...Expression) Expressions {
	return ZAndExpr(exprs)
}

// Or creates Expression such as "(exprs[0])OR(exprs[1])OR(exprs[2])".
//
// If you output expression which isn't adding Expression at all,
// it generates "('empty' = 'OR')".
func Or(exprs ...Expression) Expressions {
	return ZOrExpr(exprs)
}

// Unsafe creates any custom expressions.
//
// But IT DOES NOT ESCAPE so if want to use input from outside, should wrap by V or InV.
//
// The basic how to use is similar to fmt.Print.
// Please refer to the example for more details.
func Unsafe(v ...interface{}) UnsafeExpression {
	return unsafeExpr(v)
}

// UnsafeExpression represents unsafe expression.
type UnsafeExpression Expression

type unsafeExpr []interface{}

func (e unsafeExpr) String() string               { return expressionToString(e) }
func (e unsafeExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e unsafeExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	for _, i := range e {
		switch v := i.(type) {
		case Expression:
			buf = v.WriteExpression(ctx, buf)
		case string:
			buf = append(buf, v...)
		case nil:
			buf = append(buf, "NULL"...)
		default:
			buf = append(buf, fmt.Sprint(i)...)
		}
	}
	return buf
}

// Variable represents the argument to which is given from outside.
type Variable Expression

// V creates Variable from a single input.
//
// If aliasForPrepared is given, the value can be rewritten using an alias name in ZArgsBuilder.Set.
// Please refer to the Select.ToPrepared example for more details.
func V(v interface{}, aliasForPrepared ...interface{}) Variable {
	if len(aliasForPrepared) > 0 {
		return &aliasedVariable{v, aliasForPrepared[0]}
	}
	return &variable{v}
}

type variable struct {
	V interface{}
}

func (v *variable) String() string               { return expressionToString(v) }
func (v *variable) C(aliasName ...string) Column { return columnExpr(v, aliasName...) }
func (v *variable) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	ctx.Args = append(ctx.Args, v.V)
	return ctx.Placeholder.Next(buf)
}

type aliasedVariable struct {
	V     interface{}
	Alias interface{}
}

func (v *aliasedVariable) String() string               { return expressionToString(v) }
func (v *aliasedVariable) C(aliasName ...string) Column { return columnExpr(v, aliasName...) }
func (v *aliasedVariable) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	ctx.ArgsMap[v.Alias] = len(ctx.Args)
	ctx.Args = append(ctx.Args, v.V)
	return ctx.Placeholder.Next(buf)
}

// InV creates Variable from slice.
// It can be used with IN operator.
func InV(slice interface{}) Variable {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		return inVariable{slice}
	}
	return valueToInV(s)
}

type inVariable []interface{}

func (v inVariable) String() string               { return expressionToString(v) }
func (v inVariable) C(aliasName ...string) Column { return columnExpr(v, aliasName...) }
func (v inVariable) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	ln := len(v)
	if ln == 0 {
		return append(buf, "()"...)
	}

	buf = append(buf, '(')
	buf = ctx.Placeholder.Next(buf)
	for i := 1; i < ln; i++ {
		buf = append(buf, ',')
		buf = ctx.Placeholder.Next(buf)
	}
	buf = append(buf, ')')
	ctx.Args = append(ctx.Args, v...)
	return buf
}
