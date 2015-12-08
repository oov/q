package q

import (
	"fmt"

	"github.com/oov/q/qutil"
)

// Column represents database table column.
// You can create it from C or Table.C.
type Column interface {
	Expression

	// for internal use.
	WriteColumn(ctx *qutil.Context, buf []byte) []byte
	WriteDefinition(ctx *qutil.Context, buf []byte) []byte
}

func columnToString(c Column) string {
	buf, ctx := qutil.NewContext(c, 32, 0, nil)
	buf = c.WriteDefinition(ctx, buf)
	buf = append(buf, ' ')
	return fmt.Sprint(string(buf), ctx.Args)
}

type columnAlias struct {
	Column
	Alias string
}

func (c *columnAlias) String() string {
	return columnToString(c)
}

func (c *columnAlias) WriteColumn(ctx *qutil.Context, buf []byte) []byte {
	return ctx.Dialect.Quote(buf, c.Alias)
}

func (c *columnAlias) WriteDefinition(ctx *qutil.Context, buf []byte) []byte {
	buf = c.Column.WriteColumn(ctx, buf)
	buf = append(buf, " AS "...)
	return c.WriteColumn(ctx, buf)
}

type column string

func (c column) String() string {
	return columnToString(c)
}

func (c column) WriteColumn(ctx *qutil.Context, buf []byte) []byte {
	return ctx.Dialect.Quote(buf, string(c))
}

func (c column) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	return c.WriteColumn(ctx, buf)
}

func (c column) WriteDefinition(ctx *qutil.Context, buf []byte) []byte {
	return c.WriteColumn(ctx, buf)
}

type columnWithTable struct {
	Table
	column
}

func (c *columnWithTable) String() string {
	return columnToString(c)
}

func (c *columnWithTable) WriteColumn(ctx *qutil.Context, buf []byte) []byte {
	buf = c.Table.WriteTable(ctx, buf)
	buf = append(buf, '.')
	buf = ctx.Dialect.Quote(buf, string(c.column))
	return buf
}

func (c *columnWithTable) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	return c.WriteColumn(ctx, buf)
}

func (c *columnWithTable) WriteDefinition(ctx *qutil.Context, buf []byte) []byte {
	return c.WriteColumn(ctx, buf)
}

type exprAsColumn struct {
	Expression
}

func (c *exprAsColumn) String() string {
	return columnToString(c)
}

func (c *exprAsColumn) WriteColumn(ctx *qutil.Context, buf []byte) []byte {
	return c.Expression.WriteExpression(ctx, buf)
}

func (c *exprAsColumn) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	return c.Expression.WriteExpression(ctx, buf)
}

func (c *exprAsColumn) WriteDefinition(ctx *qutil.Context, buf []byte) []byte {
	return c.Expression.WriteExpression(ctx, buf)
}

// C creates database table column.
// You can pass the following types:
//	string
//	q.Expression
func C(c interface{}, aliasName ...string) Column {
	var r Column
	switch v := c.(type) {
	case *columnAlias:
		return v // prevents double wrapping
	case Column:
		r = v
	case Expression:
		r = &exprAsColumn{v}
	case string:
		r = column(v)
	default:
		panic(fmt.Sprintf("q: %T is not a valid column type", c))
	}
	if len(aliasName) == 0 {
		return r
	}
	return &columnAlias{Column: r, Alias: aliasName[0]}
}
