package q

import (
	"fmt"

	"github.com/oov/q/qutil"
)

// DeleteBuilder implements a DELETE builder.
type DeleteBuilder struct {
	Dialect qutil.Dialect
	Table   Table
	Wheres  Expressions
}

// Delete creates DeleteBuilder.
func Delete(table ...Table) *DeleteBuilder {
	var t Table
	if len(table) > 0 {
		t = table[0]
	}
	return &DeleteBuilder{
		Table:  t,
		Wheres: And(),
	}
}

// SetDialect sets a Dialect to the builder.
func (b *DeleteBuilder) SetDialect(d qutil.Dialect) *DeleteBuilder {
	b.Dialect = d
	return b
}

// From sets a table to the FROM clause.
func (b *DeleteBuilder) From(table Table) *DeleteBuilder {
	b.Table = table
	return b
}

// Where adds condition to the WHERE clause.
// More than one condition is connected by AND.
func (b *DeleteBuilder) Where(conds ...Expression) *DeleteBuilder {
	b.Wheres.Add(conds...)
	return b
}

func (b *DeleteBuilder) write(ctx *qutil.Context, buf []byte) []byte {
	if b.Table == nil {
		panic("q: must set table to generate DELETE statement.")
	}
	buf = append(buf, "DELETE FROM "...)
	buf = b.Table.WriteTable(ctx, buf)
	if b.Wheres.Len() > 0 {
		buf = append(buf, " WHERE "...)
		buf = b.Wheres.WriteExpression(ctx, buf)
	}
	return buf
}

// ToSQL builds SQL and arguments.
func (b *DeleteBuilder) ToSQL() (string, []interface{}) {
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
func (b *DeleteBuilder) String() string {
	buf, ctx := qutil.NewContext(b, 128, 8, nil)
	ctx.CUD = true
	buf = b.write(ctx, buf)
	buf = append(buf, ' ')
	return fmt.Sprint(string(buf), ctx.Args)
}
