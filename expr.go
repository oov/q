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

// Expressions represents combination of an expression.
// It can use as Expression because Expressions implements Expression interface.
type Expressions interface {
	Expression
	// Add adds conds and returns myself.
	Add(conds ...Expression) Expressions
	// Len returns number of expressions.
	Len() int
}

type nullExpr struct{}

func (e nullExpr) C(aliasName ...string) Column { panic("not implemeneted") }
func (e nullExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	return append(buf, "NULL"...)
}

type simpleExpr struct {
	Op    string
	Left  interface{}
	Right interface{}
}

func (e *simpleExpr) String() string               { return expressionToString(e) }
func (e *simpleExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *simpleExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, e.Op...)
	buf = writeIntf(e.Right, ctx, buf)
	return buf
}

type eqExpr struct {
	Eq    bool
	Left  interface{}
	Right interface{}
}

func (e *eqExpr) String() string               { return expressionToString(e) }
func (e *eqExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *eqExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	lv, rv := e.Left, e.Right
	if lv == nil {
		lv, rv = rv, lv
	}
	if rv == nil {
		buf = writeIntf(lv, ctx, buf)
		if e.Eq {
			return append(buf, " IS NULL"...)
		}
		return append(buf, " IS NOT NULL"...)
	}

	buf = writeIntf(lv, ctx, buf)
	if e.Eq {
		buf = append(buf, " = "...)
	} else {
		buf = append(buf, " != "...)
	}
	buf = writeIntf(rv, ctx, buf)
	return buf
}

type inExpr struct {
	Eq    bool
	Left  interface{}
	Right inVariable
}

func (e *inExpr) String() string               { return expressionToString(e) }
func (e *inExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *inExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	if len(e.Right) == 0 {
		// x IN () is invaild syntax.
		// But at the same time, a result is a obvious expression.
		// So replace the alternative valid expression which is the same result.
		if e.Eq {
			return append(buf, "'IN' = '()'"...)
		}
		return append(buf, "'IN' != '()'"...)
	}

	buf = writeIntf(e.Left, ctx, buf)
	if e.Eq {
		buf = append(buf, " IN "...)
	} else {
		buf = append(buf, " NOT IN "...)
	}
	buf = e.Right.WriteExpression(ctx, buf)
	return buf
}

type logicalExpr struct {
	Exprs []Expression
	Op    string
}

func (e *logicalExpr) String() string               { return expressionToString(e) }
func (e *logicalExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *logicalExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	switch len(e.Exprs) {
	case 0:
		buf = append(buf, "('empty' = '"...)
		buf = append(buf, e.Op...)
		buf = append(buf, "')"...)
		return buf
	case 1:
		return e.Exprs[0].WriteExpression(ctx, buf)
	}
	buf = append(buf, '(')
	buf = e.Exprs[0].WriteExpression(ctx, buf)
	buf = append(buf, ')')
	for _, cd := range e.Exprs[1:] {
		buf = append(buf, e.Op...)
		buf = append(buf, '(')
		buf = cd.WriteExpression(ctx, buf)
		buf = append(buf, ')')
	}
	return buf
}

func (e *logicalExpr) Add(exprs ...Expression) Expressions {
	e.Exprs = append(e.Exprs, exprs...)
	return e
}

func (e *logicalExpr) Len() int {
	return len(e.Exprs)
}

func newIn(l interface{}, v reflect.Value, eq bool) Expression {
	ln := v.Len()
	r := make(inVariable, ln)
	for i := 0; i < ln; i++ {
		r[i] = v.Index(i).Interface()
	}
	return &inExpr{Eq: eq, Left: l, Right: r}
}

// Eq creates Expression such as "l = r".
// But when you pass nil to one of a pair, Eq creates "x IS NULL" instead.
// In the same way, when you pass slice of any type to r, Eq creates "x IN (?)".
func Eq(l, r interface{}) Expression {
	if rv := reflect.ValueOf(r); rv.Kind() == reflect.Slice {
		if _, ok := r.(Expression); !ok {
			return newIn(l, rv, true)
		}
	}
	return &eqExpr{
		Eq:    true,
		Left:  l,
		Right: r,
	}
}

// Neq creates Expression such as "l != r".
// But when you pass nil to one of a pair, Neq creates "x IS NOT NULL" instead.
// In the same way, when you pass slice of any type to r, Neq creates "x NOT IN (?)".
func Neq(l, r interface{}) Expression {
	if rv := reflect.ValueOf(r); rv.Kind() == reflect.Slice {
		if _, ok := r.(Expression); !ok {
			return newIn(l, rv, false)
		}
	}
	return &eqExpr{
		Eq:    false,
		Left:  l,
		Right: r,
	}
}

// In creates Expression such as "l IN r".
func In(l, r interface{}) Expression {
	if rv := reflect.ValueOf(r); rv.Kind() == reflect.Slice {
		if _, ok := r.(Expression); !ok {
			return newIn(l, rv, true)
		}
	}
	return &simpleExpr{
		Op:    " IN ",
		Left:  l,
		Right: r,
	}
}

// NotIn creates Expression such as "l NOT IN r".
func NotIn(l, r interface{}) Expression {
	if rv := reflect.ValueOf(r); rv.Kind() == reflect.Slice {
		if _, ok := r.(Expression); !ok {
			return newIn(l, rv, false)
		}
	}
	return &simpleExpr{
		Op:    " NOT IN ",
		Left:  l,
		Right: r,
	}
}

// Gt creates Expression such as "l > r".
func Gt(l, r interface{}) Expression {
	return &simpleExpr{
		Op:    " > ",
		Left:  l,
		Right: r,
	}
}

// Gte creates Expression such as "l >= r".
func Gte(l, r interface{}) Expression {
	return &simpleExpr{
		Op:    " >= ",
		Left:  l,
		Right: r,
	}
}

// Lt creates Expression such as "l < r".
func Lt(l, r interface{}) Expression {
	return &simpleExpr{
		Op:    " < ",
		Left:  l,
		Right: r,
	}
}

// Lte creates Expression such as "l <= r".
func Lte(l, r interface{}) Expression {
	return &simpleExpr{
		Op:    " <= ",
		Left:  l,
		Right: r,
	}
}

// And creates Expression such as "(exprs[0])AND(exprs[1])AND(exprs[2])".
//
// If you output expression which isn't adding Expression at all,
// it generates "('empty' = 'AND')".
func And(exprs ...Expression) Expressions {
	return &logicalExpr{Op: "AND", Exprs: exprs}
}

// Or creates Expression such as "(exprs[0])OR(exprs[1])OR(exprs[2])".
//
// If you output expression which isn't adding Expression at all,
// it generates "('empty' = 'OR')".
func Or(exprs ...Expression) Expressions {
	return &logicalExpr{Op: "OR", Exprs: exprs}
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
func V(v interface{}) Variable {
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

// InV creates Variable from slice.
// It can be used with IN operator.
func InV(slice interface{}) Variable {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		return inVariable{slice}
	}
	ln := s.Len()
	if ln == 0 {
		panic("q: need at least one value to create Variable.")
	}
	v := make(inVariable, ln)
	for i := 0; i < ln; i++ {
		v[i] = s.Index(i).Interface()
	}
	return v
}

type inVariable []interface{}

func (v inVariable) String() string               { return expressionToString(v) }
func (v inVariable) C(aliasName ...string) Column { return columnExpr(v, aliasName...) }
func (v inVariable) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = append(buf, '(')
	buf = ctx.Placeholder.Next(buf)
	for i, l := 1, len(v); i < l; i++ {
		buf = append(buf, ',')
		buf = ctx.Placeholder.Next(buf)
	}
	buf = append(buf, ')')
	ctx.Args = append(ctx.Args, v...)
	return buf
}
