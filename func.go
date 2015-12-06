package q

// Function represents functions.
type Function interface {
	Expression
}

type function struct {
	Name string
	V    interface{}
}

func (f *function) writeExpression(ctx *ctx, buf []byte) []byte {
	buf = append(buf, f.Name...)
	buf = append(buf, '(')
	buf = writeValue(f.V, ctx, buf)
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

func (f *charLengthFunc) writeExpression(ctx *ctx, buf []byte) []byte {
	buf = append(buf, ctx.Dialect.CharLengthName()...)
	buf = append(buf, '(')
	buf = writeValue(f.V, ctx, buf)
	buf = append(buf, ')')
	return buf
}

// CharLength creates Function such as "CHAR_LENGTH(v)".
func CharLength(v interface{}) Function {
	return &charLengthFunc{v}
}
