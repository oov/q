package q

import "github.com/oov/q/qutil"

// ZDeleteBuilder implements a DELETE builder.
type ZDeleteBuilder struct {
	Dialect qutil.Dialect
	Table   Table
	Wheres  ZAndExpr
}

// Delete creates ZDeleteBuilder.
func Delete(table ...Table) *ZDeleteBuilder {
	var t Table
	if len(table) > 0 {
		t = table[0]
	}
	return &ZDeleteBuilder{
		Table:  t,
		Wheres: ZAndExpr(make([]Expression, 0, 4)),
	}
}

// SetDialect sets a Dialect to the builder.
func (b *ZDeleteBuilder) SetDialect(d qutil.Dialect) *ZDeleteBuilder {
	b.Dialect = d
	return b
}

// From sets a table to the FROM clause.
func (b *ZDeleteBuilder) From(table Table) *ZDeleteBuilder {
	b.Table = table
	return b
}

// Where adds condition to the WHERE clause.
// More than one condition is connected by AND.
func (b *ZDeleteBuilder) Where(conds ...Expression) *ZDeleteBuilder {
	b.Wheres = append(b.Wheres, conds...)
	return b
}

func (b *ZDeleteBuilder) write(ctx *qutil.Context, buf []byte) []byte {
	if b.Table == nil {
		panic("q: must set table to generate DELETE statement.")
	}
	buf = append(buf, "DELETE FROM "...)
	buf = b.Table.WriteTable(ctx, buf)
	if len(b.Wheres) > 0 {
		buf = append(buf, " WHERE "...)
		buf = b.Wheres.WriteExpression(ctx, buf)
	}
	return buf
}

// ToSQL builds SQL and arguments.
func (b *ZDeleteBuilder) ToSQL() (string, []interface{}) {
	return builderToSQL(b, b.Dialect, 128, 8, true)
}

// String implemenets fmt.Stringer interface.
func (b *ZDeleteBuilder) String() string {
	return builderToString(b, b.Dialect, 128, 8, true)
}
