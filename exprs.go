package q

import "github.com/oov/q/qutil"

type gtExpr struct {
	Left  interface{}
	Right interface{}
}

func (e *gtExpr) String() string               { return expressionToString(e) }
func (e *gtExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *gtExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " > "...)
	buf = writeIntf(e.Right, ctx, buf)
	return buf
}

type gteExpr struct {
	Left  interface{}
	Right interface{}
}

func (e *gteExpr) String() string               { return expressionToString(e) }
func (e *gteExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *gteExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " >= "...)
	buf = writeIntf(e.Right, ctx, buf)
	return buf
}

type ltExpr struct {
	Left  interface{}
	Right interface{}
}

func (e *ltExpr) String() string               { return expressionToString(e) }
func (e *ltExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *ltExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " < "...)
	buf = writeIntf(e.Right, ctx, buf)
	return buf
}

type lteExpr struct {
	Left  interface{}
	Right interface{}
}

func (e *lteExpr) String() string               { return expressionToString(e) }
func (e *lteExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *lteExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " <= "...)
	buf = writeIntf(e.Right, ctx, buf)
	return buf
}

type simpleInExpr struct {
	Left  interface{}
	Right interface{}
}

func (e *simpleInExpr) String() string               { return expressionToString(e) }
func (e *simpleInExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *simpleInExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " IN "...)
	buf = writeIntf(e.Right, ctx, buf)
	return buf
}

type simpleNotInExpr struct {
	Left  interface{}
	Right interface{}
}

func (e *simpleNotInExpr) String() string               { return expressionToString(e) }
func (e *simpleNotInExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *simpleNotInExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " NOT IN "...)
	buf = writeIntf(e.Right, ctx, buf)
	return buf
}

type eqExpr struct {
	Left  interface{}
	Right interface{}
}

func (e eqExpr) String() string               { return expressionToString(e) }
func (e eqExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e eqExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	lv, rv := e.Left, e.Right
	if lv == nil {
		lv, rv = rv, lv
	}
	if rv == nil {
		buf = writeIntf(lv, ctx, buf)
		return append(buf, " IS NULL"...)
	}

	buf = writeIntf(lv, ctx, buf)
	buf = append(buf, " = "...)
	buf = writeIntf(rv, ctx, buf)
	return buf
}

type neqExpr struct {
	Left  interface{}
	Right interface{}
}

func (e neqExpr) String() string               { return expressionToString(e) }
func (e neqExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e neqExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	lv, rv := e.Left, e.Right
	if lv == nil {
		lv, rv = rv, lv
	}
	if rv == nil {
		buf = writeIntf(lv, ctx, buf)
		return append(buf, " IS NOT NULL"...)
	}

	buf = writeIntf(lv, ctx, buf)
	buf = append(buf, " != "...)
	buf = writeIntf(rv, ctx, buf)
	return buf
}

type inExpr struct {
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
		return append(buf, "'IN' = '()'"...)
	}

	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " IN "...)
	buf = e.Right.WriteExpression(ctx, buf)
	return buf
}

type notInExpr struct {
	Left  interface{}
	Right inVariable
}

func (e *notInExpr) String() string               { return expressionToString(e) }
func (e *notInExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }
func (e *notInExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	if len(e.Right) == 0 {
		// x NOT IN () is invaild syntax.
		// But at the same time, a result is a obvious expression.
		// So replace the alternative valid expression which is the same result.
		return append(buf, "'IN' != '()'"...)
	}

	buf = writeIntf(e.Left, ctx, buf)
	buf = append(buf, " NOT IN "...)
	buf = e.Right.WriteExpression(ctx, buf)
	return buf
}

// ZAndExpr represents AND Expression.
type ZAndExpr []Expression

// String implements fmt.Stringer interface method.
func (e ZAndExpr) String() string { return expressionToString(e) }

// C implements Expression interface method.
func (e ZAndExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }

// WriteExpression implements Expression interface method.
func (e ZAndExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	switch len(e) {
	case 0:
		buf = append(buf, "('empty' = 'AND')"...)
		return buf
	case 1:
		return e[0].WriteExpression(ctx, buf)
	}
	buf = append(buf, '(')
	buf = e[0].WriteExpression(ctx, buf)
	buf = append(buf, ')')
	for _, cd := range e[1:] {
		buf = append(buf, "AND("...)
		buf = cd.WriteExpression(ctx, buf)
		buf = append(buf, ')')
	}
	return buf
}

// ZOrExpr represents OR Expression.
type ZOrExpr []Expression

// String implements fmt.Stringer interface method.
func (e ZOrExpr) String() string { return expressionToString(e) }

// C implements Expression interface method.
func (e ZOrExpr) C(aliasName ...string) Column { return columnExpr(e, aliasName...) }

// WriteExpression implements Expression interface method.
func (e ZOrExpr) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	switch len(e) {
	case 0:
		buf = append(buf, "('empty' = 'OR')"...)
		return buf
	case 1:
		return e[0].WriteExpression(ctx, buf)
	}
	buf = append(buf, '(')
	buf = e[0].WriteExpression(ctx, buf)
	buf = append(buf, ')')
	for _, cd := range e[1:] {
		buf = append(buf, "OR("...)
		buf = cd.WriteExpression(ctx, buf)
		buf = append(buf, ')')
	}
	return buf
}
