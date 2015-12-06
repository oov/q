package q

import (
	"fmt"
)

// Column represents database table column.
// You can create it from C or Table.C.
type Column interface {
	Expression
	writeColumn(ctx *ctx, buf []byte) []byte
	writeDefinition(ctx *ctx, buf []byte) []byte
}

func columnToString(c Column) string {
	buf, ctx := newDummyCtx(32, 0)
	buf = c.writeDefinition(ctx, buf)
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

func (c *columnAlias) writeColumn(ctx *ctx, buf []byte) []byte {
	return ctx.Dialect.Quote(buf, c.Alias)
}

func (c *columnAlias) writeDefinition(ctx *ctx, buf []byte) []byte {
	buf = c.Column.writeColumn(ctx, buf)
	buf = append(buf, " AS "...)
	return c.writeColumn(ctx, buf)
}

type column string

func (c column) String() string {
	return columnToString(c)
}

func (c column) writeColumn(ctx *ctx, buf []byte) []byte {
	return ctx.Dialect.Quote(buf, string(c))
}

func (c column) writeExpression(ctx *ctx, buf []byte) []byte {
	return c.writeColumn(ctx, buf)
}

func (c column) writeDefinition(ctx *ctx, buf []byte) []byte {
	return c.writeColumn(ctx, buf)
}

type columnWithTable struct {
	Table
	column
}

func (c *columnWithTable) String() string {
	return columnToString(c)
}

func (c *columnWithTable) writeColumn(ctx *ctx, buf []byte) []byte {
	buf = c.Table.writeTable(ctx, buf)
	buf = append(buf, '.')
	buf = ctx.Dialect.Quote(buf, string(c.column))
	return buf
}

func (c *columnWithTable) writeExpression(ctx *ctx, buf []byte) []byte {
	return c.writeColumn(ctx, buf)
}

func (c *columnWithTable) writeDefinition(ctx *ctx, buf []byte) []byte {
	return c.writeColumn(ctx, buf)
}

type exprAsColumn struct {
	Expression
}

func (c *exprAsColumn) String() string {
	return columnToString(c)
}

func (c *exprAsColumn) writeColumn(ctx *ctx, buf []byte) []byte {
	return c.Expression.writeExpression(ctx, buf)
}

func (c *exprAsColumn) writeExpression(ctx *ctx, buf []byte) []byte {
	return c.Expression.writeExpression(ctx, buf)
}

func (c *exprAsColumn) writeDefinition(ctx *ctx, buf []byte) []byte {
	return c.Expression.writeExpression(ctx, buf)
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
