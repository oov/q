// Package q implements a SQL builder.
package q

import "github.com/oov/q/qutil"

// ZSelectBuilder implemenets a SELECT builder.
// This also implements Expression interface, so it can use in many place.
type ZSelectBuilder struct {
	Dialect   qutil.Dialect
	Beginning string
	Columns   []Column
	Tables    []Table
	Wheres    Expressions
	Groups    []Expression
	Havings   Expressions
	Orders    []struct {
		Expression
		Ascending bool
	}
	LimitCount  Expression
	StartOffset Expression
}

// Select creates ZSelectBuilder.
// If not needing an additional keyword around "SELECT", the argument can be omitted.
func Select(beginning ...string) *ZSelectBuilder {
	var b string
	if len(beginning) > 0 {
		b = beginning[0]
	} else {
		b = "SELECT"
	}
	return &ZSelectBuilder{
		Beginning: b,
		Wheres:    And(),
		Havings:   And(),
	}
}

// SetDialect sets a Dialect to the builder.
func (b *ZSelectBuilder) SetDialect(d qutil.Dialect) *ZSelectBuilder {
	b.Dialect = d
	return b
}

// Column appends a column to the column list.
func (b *ZSelectBuilder) Column(columns ...Column) *ZSelectBuilder {
	b.Columns = append(b.Columns, columns...)
	return b
}

// From appends a table to the FROM clause.
func (b *ZSelectBuilder) From(tables ...Table) *ZSelectBuilder {
	b.Tables = append(b.Tables, tables...)
	return b
}

// Where adds condition to the WHERE clause.
// More than one condition is connected by AND.
func (b *ZSelectBuilder) Where(conds ...Expression) *ZSelectBuilder {
	b.Wheres.Add(conds...)
	return b
}

// Limit sets LIMIT clause to the builder.
func (b *ZSelectBuilder) Limit(count interface{}) *ZSelectBuilder {
	b.LimitCount = interfaceToExpression(count)
	return b
}

// Offset sets OFFSET clause to the builder.
func (b *ZSelectBuilder) Offset(start interface{}) *ZSelectBuilder {
	b.StartOffset = interfaceToExpression(start)
	return b
}

// GroupBy adds condition to the GROUP BY clause.
func (b *ZSelectBuilder) GroupBy(e ...Expression) *ZSelectBuilder {
	b.Groups = append(b.Groups, e...)
	return b
}

// Having adds HAVING condition to the GROUP BY clause.
// More than one condition is connected by AND.
func (b *ZSelectBuilder) Having(conds ...Expression) *ZSelectBuilder {
	b.Havings.Add(conds...)
	return b
}

// OrderBy adds condition to the ORDER BY clause.
func (b *ZSelectBuilder) OrderBy(e Expression, asc bool) *ZSelectBuilder {
	b.Orders = append(b.Orders, struct {
		Expression
		Ascending bool
	}{e, asc})
	return b
}

func (b *ZSelectBuilder) write(ctx *qutil.Context, buf []byte) []byte {
	buf = append(buf, b.Beginning...)

	if len(b.Columns) == 0 {
		buf = append(buf, " *"...)
	} else {
		buf = append(buf, ' ')
		buf = b.Columns[0].WriteDefinition(ctx, buf)
		for _, c := range b.Columns[1:] {
			buf = append(buf, ", "...)
			buf = c.WriteDefinition(ctx, buf)
		}
	}

	if len(b.Tables) == 0 {
		// FROM DUAL?
	} else {
		buf = append(buf, " FROM "...)
		buf = b.Tables[0].WriteDefinition(ctx, buf)
		for _, t := range b.Tables[1:] {
			buf = append(buf, ", "...)
			buf = t.WriteDefinition(ctx, buf)
		}
	}

	if b.Wheres.Len() > 0 {
		buf = append(buf, " WHERE "...)
		buf = b.Wheres.WriteExpression(ctx, buf)
	}

	if len(b.Groups) > 0 {
		buf = append(buf, " GROUP BY "...)
		buf = b.Groups[0].WriteExpression(ctx, buf)
		for _, g := range b.Groups[1:] {
			buf = append(buf, ", "...)
			buf = g.WriteExpression(ctx, buf)
		}
	}

	if b.Havings.Len() > 0 {
		buf = append(buf, " HAVING "...)
		buf = b.Havings.WriteExpression(ctx, buf)
	}

	if len(b.Orders) > 0 {
		buf = append(buf, " ORDER BY "...)
		for i, o := range b.Orders {
			if i > 0 {
				buf = append(buf, ", "...)
			}
			buf = o.Expression.WriteExpression(ctx, buf)
			if o.Ascending {
				buf = append(buf, " ASC"...)
			} else {
				buf = append(buf, " DESC"...)
			}
		}
	}

	if b.LimitCount != nil {
		buf = append(buf, " LIMIT "...)
		buf = b.LimitCount.WriteExpression(ctx, buf)
	}

	if b.StartOffset != nil {
		buf = append(buf, " OFFSET "...)
		buf = b.StartOffset.WriteExpression(ctx, buf)
	}

	return buf
}

// ToSQL returns generated SQL and arguments.
func (b *ZSelectBuilder) ToSQL() (string, []interface{}) {
	return builderToSQL(b, b.Dialect, 128, 8, false)
}

// String implements fmt.Stringer interface.
func (b *ZSelectBuilder) String() string {
	return builderToString(b, b.Dialect, 128, 8, false)
}

// T creates Table from this builder.
func (b *ZSelectBuilder) T(aliasName string) Table {
	return &selectBuilderAsTable{ZSelectBuilder: b, Alias: aliasName}
}

// C implements Expression interface.
func (b *ZSelectBuilder) C(aliasName ...string) Column {
	return columnExpr(b, aliasName...)
}

// WriteExpression implements Expression interface.
func (b *ZSelectBuilder) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	buf = append(buf, '(')
	buf = b.write(ctx, buf)
	buf = append(buf, ')')
	return buf
}
