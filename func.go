package q

import "github.com/oov/q/qutil"

// Function represents functions.
type Function Expression

type function struct {
	Name string
	V    interface{}
}

func (f *function) String() string               { return expressionToString(f) }
func (f *function) C(aliasName ...string) Column { return columnExpr(f, aliasName...) }
func (f *function) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = append(buf, f.Name...)
	buf = append(buf, '(')
	buf = writeIntf(f.V, ctx, buf)
	buf = append(buf, ')')
	return buf
}

// Count creates Function such as "COUNT(v)".
func Count(v interface{}) Function {
	return &function{"COUNT", v}
}

// CountAll creates Function "COUNT(*)".
func CountAll() Function {
	return &function{"COUNT", Unsafe("*")}
}

// Avg creates Function such as "AVG(v)".
func Avg(v interface{}) Function {
	return &function{"AVG", v}
}

// Max creates Function such as "MAX(v)".
func Max(v interface{}) Function {
	return &function{"MAX", v}
}

// Min creates Function such as "MIN(v)".
func Min(v interface{}) Function {
	return &function{"MIN", v}
}

// Sum creates Function such as "SUM(v)".
func Sum(v interface{}) Function {
	return &function{"SUM", v}
}

type charLengthFunc struct {
	V interface{}
}

func (f *charLengthFunc) String() string               { return expressionToString(f) }
func (f *charLengthFunc) C(aliasName ...string) Column { return columnExpr(f, aliasName...) }
func (f *charLengthFunc) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = append(buf, ctx.Dialect.CharLengthName()...)
	buf = append(buf, '(')
	buf = writeIntf(f.V, ctx, buf)
	buf = append(buf, ')')
	return buf
}

// CharLength creates Function such as "CHAR_LENGTH(v)".
func CharLength(v interface{}) Function {
	return &charLengthFunc{v}
}
