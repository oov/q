// Package q implements a SQL builder.
package q

import (
	"fmt"
)

// SQL represents a executable SQL statement.
// In other words, this doesn't represents a piece of SQL statement.
type SQL interface {
	Expression
	fmt.Stringer
}

type sqlBytes []byte

func (s sqlBytes) writeExpression(ctx *ctx, buf []byte) []byte {
	buf = append(buf, '(')
	buf = append(buf, s...)
	buf = append(buf, ')')
	return buf
}

func (s sqlBytes) String() string {
	return string(s)
}

type order struct {
	Expression
	Ascending bool
}

func (o *order) writeDefinition(ctx *ctx, buf []byte) []byte {
	buf = o.Expression.writeExpression(ctx, buf)
	if o.Ascending {
		return append(buf, " ASC"...)
	}
	return append(buf, " DESC"...)
}

func (o *order) String() string {
	buf, ctx := newDummyCtx(32, 1)
	buf = o.writeDefinition(ctx, buf)
	buf = append(buf, ' ')
	return fmt.Sprint(string(buf), ctx.Args)
}

// SelectBuilder implemenets a SELECT builder.
// This also implements Expression interface, so it can use in many place.
type SelectBuilder struct {
	Beginning   string
	Columns     []Column
	Tables      []Table
	Wheres      Expressions
	Groups      []Expression
	Havings     Expressions
	Orders      []order
	LimitCount  Expression
	StartOffset Expression
}

// Select creates SelectBuilder.
// If not needing an additional keyword around "SELECT", the argument can be omitted.
func Select(beginning ...string) *SelectBuilder {
	var b string
	if len(beginning) > 0 {
		b = beginning[0]
	} else {
		b = "SELECT"
	}
	return &SelectBuilder{
		Beginning: b,
		Wheres:    And(),
		Havings:   And(),
	}
}

// Column appends a column to the column list.
func (b *SelectBuilder) Column(columns ...Column) *SelectBuilder {
	b.Columns = append(b.Columns, columns...)
	return b
}

// From appends a table to the FROM clause.
func (b *SelectBuilder) From(tables ...Table) *SelectBuilder {
	b.Tables = append(b.Tables, tables...)
	return b
}

// Where adds condition to the WHERE clause.
// More than one condition is connected by AND.
func (b *SelectBuilder) Where(conds ...Expression) *SelectBuilder {
	b.Wheres.Add(conds...)
	return b
}

// Limit sets LIMIT clause to the builder.
func (b *SelectBuilder) Limit(count interface{}) *SelectBuilder {
	b.LimitCount = interfaceToExpression(count)
	return b
}

// Offset sets OFFSET clause to the builder.
func (b *SelectBuilder) Offset(start interface{}) *SelectBuilder {
	b.StartOffset = interfaceToExpression(start)
	return b
}

// GroupBy adds condition to the GROUP BY clause.
func (b *SelectBuilder) GroupBy(e ...Expression) *SelectBuilder {
	b.Groups = append(b.Groups, e...)
	return b
}

// Having adds HAVING condition to the GROUP BY clause.
// More than one condition is connected by AND.
func (b *SelectBuilder) Having(conds ...Expression) *SelectBuilder {
	b.Havings.Add(conds...)
	return b
}

// OrderBy adds condition to the ORDER BY clause.
func (b *SelectBuilder) OrderBy(e Expression, asc bool) *SelectBuilder {
	b.Orders = append(b.Orders, order{Expression: e, Ascending: asc})
	return b
}

func (b *SelectBuilder) write(ctx *ctx, buf []byte) []byte {
	buf = append(buf, b.Beginning...)

	if len(b.Columns) == 0 {
		buf = append(buf, " *"...)
	} else {
		buf = append(buf, ' ')
		buf = b.Columns[0].writeDefinition(ctx, buf)
		for _, c := range b.Columns[1:] {
			buf = append(buf, ", "...)
			buf = c.writeDefinition(ctx, buf)
		}
	}

	if len(b.Tables) == 0 {
		// FROM DUAL?
	} else {
		buf = append(buf, " FROM "...)
		buf = b.Tables[0].writeDefinition(ctx, buf)
		for _, t := range b.Tables[1:] {
			buf = append(buf, ", "...)
			buf = t.writeDefinition(ctx, buf)
		}
	}

	if b.Wheres.Len() > 0 {
		buf = append(buf, " WHERE "...)
		buf = b.Wheres.writeExpression(ctx, buf)
	}

	if len(b.Groups) > 0 {
		buf = append(buf, " GROUP BY "...)
		buf = b.Groups[0].writeExpression(ctx, buf)
		for _, g := range b.Groups[1:] {
			buf = append(buf, ", "...)
			buf = g.writeExpression(ctx, buf)
		}
	}

	if b.Havings.Len() > 0 {
		buf = append(buf, " HAVING "...)
		buf = b.Havings.writeExpression(ctx, buf)
	}

	if len(b.Orders) > 0 {
		buf = append(buf, " ORDER BY "...)
		buf = b.Orders[0].writeDefinition(ctx, buf)
		for _, o := range b.Orders[1:] {
			buf = append(buf, ", "...)
			buf = o.writeDefinition(ctx, buf)
		}
	}

	if b.LimitCount != nil {
		buf = append(buf, " LIMIT "...)
		buf = b.LimitCount.writeExpression(ctx, buf)
	}

	if b.StartOffset != nil {
		buf = append(buf, " OFFSET "...)
		buf = b.StartOffset.writeExpression(ctx, buf)
	}

	return buf
}

// ToSQL builds SQL and arguments.
func (b *SelectBuilder) ToSQL(d dialect) (SQL, []interface{}) {
	buf, ctx := newCtx(128, 8, d)
	buf = b.write(ctx, buf)
	return sqlBytes(buf), ctx.Args
}

// String implemenets fmt.Stringer interface.
func (b *SelectBuilder) String() string {
	buf, ctx := newDummyCtx(128, 8)
	buf = b.write(ctx, buf)
	buf = append(buf, ' ')
	return fmt.Sprint(string(buf), ctx.Args)
}

func (b *SelectBuilder) writeExpression(ctx *ctx, buf []byte) []byte {
	buf = append(buf, '(')
	buf = b.write(ctx, buf)
	buf = append(buf, ')')
	return buf
}
