package q

import (
	"fmt"

	"github.com/oov/q/qutil"
)

// Update implements a DELETE builder.
type UpdateBuilder struct {
	Dialect   qutil.Dialect
	Beginning string
	Table     Table
	Sets      []struct {
		Name string
		Column
		Expression
	}
	Wheres Expressions
}

// Update creates UpdateBuilder.
func Update(table Table, beginning ...string) *UpdateBuilder {
	var b string
	if len(beginning) > 0 {
		b = beginning[0]
	} else {
		b = "UPDATE"
	}
	return &UpdateBuilder{
		Beginning: b,
		Table:     table,
		Wheres:    And(),
	}
}

// SetDialect sets a Dialect to the builder.
func (b *UpdateBuilder) SetDialect(d qutil.Dialect) *UpdateBuilder {
	b.Dialect = d
	return b
}

func (b *UpdateBuilder) find(c Column) (int, string) {
	buf, ctx := qutil.NewContext(b, 32, 0, nil)
	ctx.CUD = true
	name := string(c.WriteColumn(ctx, buf))
	for i, s := range b.Sets {
		if name == s.Name {
			return i, name
		}
	}
	return -1, name
}

// Set adds assignment expression to the SET clause.
func (b *UpdateBuilder) Set(c Column, v interface{}) *UpdateBuilder {
	i, name := b.find(c)
	if i != -1 {
		b.Sets[i].Column = c
		b.Sets[i].Expression = interfaceToExpression(v)
		return b
	}
	b.Sets = append(b.Sets, struct {
		Name string
		Column
		Expression
	}{name, c, interfaceToExpression(v)})
	return b
}

// Unset removes assignment expression from the SET clause.
func (b *UpdateBuilder) Unset(c Column) *UpdateBuilder {
	i, _ := b.find(c)
	if i == -1 {
		return b
	}
	if i == 0 {
		b.Sets = b.Sets[1:]
		return b
	}
	if i == len(b.Sets)-1 {
		b.Sets = b.Sets[:len(b.Sets)-1]
		return b
	}
	b.Sets = append(b.Sets[:i], b.Sets[i+1:]...)
	return b
}

// Where adds condition to the WHERE clause.
// More than one condition is connected by AND.
func (b *UpdateBuilder) Where(conds ...Expression) *UpdateBuilder {
	b.Wheres.Add(conds...)
	return b
}

func (b *UpdateBuilder) write(ctx *qutil.Context, buf []byte) []byte {
	if len(b.Sets) == 0 {
		panic("q: need at least one assignment expression to generate UPDATE statements.")
	}

	buf = append(buf, b.Beginning...)
	buf = append(buf, ' ')

	buf = b.Table.WriteDefinition(ctx, buf)

	buf = append(buf, " SET "...)
	for i, s := range b.Sets {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = s.Column.WriteColumn(ctx, buf)
		buf = append(buf, " = "...)
		buf = s.Expression.WriteExpression(ctx, buf)
	}

	if b.Wheres.Len() > 0 {
		buf = append(buf, " WHERE "...)
		buf = b.Wheres.WriteExpression(ctx, buf)
	}
	return buf
}

// ToSQL builds SQL and arguments.
func (b *UpdateBuilder) ToSQL() (string, []interface{}) {
	var d qutil.Dialect
	if b.Dialect != nil {
		d = b.Dialect
	} else {
		d = DefaultDialect
	}
	buf, ctx := qutil.NewContext(b, 128, 8, d)
	ctx.CUD = true
	buf = b.write(ctx, buf)
	return string(buf), ctx.Args
}

// String implemenets fmt.Stringer interface.
func (b *UpdateBuilder) String() string {
	buf, ctx := qutil.NewContext(b, 128, 8, nil)
	ctx.CUD = true
	buf = b.write(ctx, buf)
	buf = append(buf, ' ')
	return fmt.Sprint(string(buf), ctx.Args)
}
