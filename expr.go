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
	return fmt.Sprint(string(buf), " ", ctx.Args)
}

func interfaceToExpression(x interface{}) Expression {
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

func writeValue(v interface{}, ctx *qutil.Context, buf []byte) []byte {
	switch vv := v.(type) {
	case Expression:
		return vv.WriteExpression(ctx, buf)
	case nil:
		return append(buf, "NULL"...)
	}
	ctx.Args = append(ctx.Args, v)
	return ctx.Placeholder.Next(buf)
}

type simpleExpr struct {
	Op    string
	Left  interface{}
	Right interface{}
}

func (e *simpleExpr) String() string {
	return expressionToString(e)
}

func (e *simpleExpr) C(aliasName ...string) Column {
	return columnExpr(e, aliasName...)
}

func (e *simpleExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = writeValue(e.Left, ctx, buf)
	buf = append(buf, e.Op...)
	buf = writeValue(e.Right, ctx, buf)
	return buf
}

type eqExpr struct {
	Eq    bool
	Left  interface{}
	Right interface{}
}

func (e *eqExpr) String() string {
	return expressionToString(e)
}

func (e *eqExpr) C(aliasName ...string) Column {
	return columnExpr(e, aliasName...)
}

func (e *eqExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	lv, rv, eq := e.Left, e.Right, e.Eq
	if lv == nil {
		lv, rv = rv, nil
	}
	if rv == nil {
		buf = writeValue(lv, ctx, buf)
		if eq {
			return append(buf, " IS NULL"...)
		}
		return append(buf, " IS NOT NULL"...)
	}

	if v := reflect.ValueOf(rv); v.Kind() == reflect.Slice {
		if v.Len() == 0 {
			// x IN () is invaild syntax.
			// But at the same time, a result is a obvious expression.
			// So replace the alternative valid expression which is the same result.
			if eq {
				return append(buf, "'IN' == '()'"...)
			}
			return append(buf, "'IN' != '()'"...)
		}
		buf = writeValue(lv, ctx, buf)
		if eq {
			buf = append(buf, " IN ("...)
		} else {
			buf = append(buf, " NOT IN ("...)
		}
		args := ctx.Args
		buf = ctx.Placeholder.Next(buf)
		args = append(args, v.Index(0).Interface())
		for i, l := 1, v.Len(); i < l; i++ {
			buf = append(buf, ',')
			buf = ctx.Placeholder.Next(buf)
			args = append(args, v.Index(i).Interface())
		}
		buf = append(buf, ')')
		ctx.Args = args
		return buf
	}

	buf = writeValue(lv, ctx, buf)
	if eq {
		buf = append(buf, " = "...)
	} else {
		buf = append(buf, " != "...)
	}
	buf = writeValue(rv, ctx, buf)
	return buf
}

type logicalExpr struct {
	Exprs []Expression
	Op    string
}

func (e *logicalExpr) String() string {
	return expressionToString(e)
}

func (e *logicalExpr) C(aliasName ...string) Column {
	return columnExpr(e, aliasName...)
}

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

// Eq creates Expression such as "l = r".
// But when you pass nil to one of a pair, Eq creates "x IS NULL" instead.
// In the same way, when you pass slice of any type to r, Eq creates "x IN (?)".
func Eq(l, r interface{}) Expression {
	if v := reflect.ValueOf(l); v.Kind() == reflect.Slice {
		panic("q: cannot use slice in l.")
	}
	return &eqExpr{Eq: true, Left: l, Right: r}
}

// Neq creates Expression such as "l != r".
// But when you pass nil to one of a pair, Neq creates "x IS NOT NULL" instead.
// In the same way, when you pass slice of any type to r, Neq creates "x NOT IN (?)".
func Neq(l, r interface{}) Expression {
	if v := reflect.ValueOf(l); v.Kind() == reflect.Slice {
		panic("q: cannot use slice in l.")
	}
	return &eqExpr{Eq: false, Left: l, Right: r}
}

// Gt creates Expression such as "l > r".
func Gt(l, r interface{}) Expression {
	return &simpleExpr{Op: " > ", Left: l, Right: r}
}

// Gte creates Expression such as "l >= r".
func Gte(l, r interface{}) Expression {
	return &simpleExpr{Op: " >= ", Left: l, Right: r}
}

// Lt creates Expression such as "l < r".
func Lt(l, r interface{}) Expression {
	return &simpleExpr{Op: " < ", Left: l, Right: r}
}

// Lte creates Expression such as "l <= r".
func Lte(l, r interface{}) Expression {
	return &simpleExpr{Op: " <= ", Left: l, Right: r}
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

func (e unsafeExpr) String() string {
	return expressionToString(e)
}

func (e unsafeExpr) C(aliasName ...string) Column {
	return columnExpr(e, aliasName...)
}

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

func (v *variable) C(aliasName ...string) Column {
	return columnExpr(v, aliasName...)
}

func (v *variable) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	ctx.Args = append(ctx.Args, v.V)
	return ctx.Placeholder.Next(buf)
}

// InV creates Variable from multiple inputs.
// It can be used with IN operator.
func InV(v ...interface{}) Variable {
	if len(v) == 0 {
		panic("q: need at least one value to create Variable.")
	}
	return &inVariable{v}
}

type inVariable struct {
	V []interface{}
}

func (v *inVariable) C(aliasName ...string) Column {
	return columnExpr(v, aliasName...)
}

func (v *inVariable) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = append(buf, '(')
	buf = ctx.Placeholder.Next(buf)
	for i, l := 1, len(v.V); i < l; i++ {
		buf = append(buf, ',')
		buf = ctx.Placeholder.Next(buf)
	}
	buf = append(buf, ')')
	ctx.Args = append(ctx.Args, v.V...)
	return buf
}
