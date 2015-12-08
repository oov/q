package q

import (
	"fmt"

	"github.com/oov/q/qutil"
)

// Column represents database table column.
// You can create it from C or Table.C or Expression.C.
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
	if ctx.CUD {
		return c.Column.WriteColumn(ctx, buf)
	}
	return ctx.Dialect.Quote(buf, c.Alias)
}

func (c *columnAlias) WriteDefinition(ctx *qutil.Context, buf []byte) []byte {
	if ctx.CUD {
		return c.Column.WriteColumn(ctx, buf)
	}
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

func (c column) C(aliasName ...string) Column {
	if len(aliasName) > 0 {
		return &columnAlias{c, aliasName[0]}
	}
	return c
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
	if ctx.CUD {
		return ctx.Dialect.Quote(buf, string(c.column))
	}
	buf = c.Table.WriteTable(ctx, buf)
	buf = append(buf, '.')
	buf = ctx.Dialect.Quote(buf, string(c.column))
	return buf
}

func (c *columnWithTable) C(aliasName ...string) Column {
	if len(aliasName) > 0 {
		return &columnAlias{c, aliasName[0]}
	}
	return c
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

// C creates Column.
func C(columnName string, aliasName ...string) Column {
	if len(aliasName) == 0 {
		return column(columnName)
	}
	return &columnAlias{column(columnName), aliasName[0]}
}
