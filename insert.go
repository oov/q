package q

import "github.com/oov/q/qutil"

// ZInsertBuilder implements a INSERT builder.
type ZInsertBuilder struct {
	Dialect   qutil.Dialect
	Beginning string
	Table     Table
	Sets      []struct {
		Name string
		Column
		Expression
	}
	Returnings []Column
}

// Insert creates ZInsertBuilder.
func Insert(beginning ...string) *ZInsertBuilder {
	var b string
	if len(beginning) > 0 {
		b = beginning[0]
	} else {
		b = "INSERT"
	}
	return &ZInsertBuilder{
		Beginning: b,
	}
}

// SetDialect sets a Dialect to the builder.
func (b *ZInsertBuilder) SetDialect(d qutil.Dialect) *ZInsertBuilder {
	b.Dialect = d
	return b
}

// Into sets a table to the builder.
func (b *ZInsertBuilder) Into(table Table) *ZInsertBuilder {
	b.Table = table
	return b
}

func (b *ZInsertBuilder) find(c Column) (int, string) {
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

// Set adds assignment expression to the builder.
func (b *ZInsertBuilder) Set(c Column, v interface{}) *ZInsertBuilder {
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

// Unset removes assignment expression from the builder.
func (b *ZInsertBuilder) Unset(c Column) *ZInsertBuilder {
	i, _ := b.find(c)
	if i == -1 {
		return b
	}
	b.Sets[i].Column = nil
	b.Sets[i].Expression = nil
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

// Returning appends a column to RETURNING clause.
// This feature is available for PostgreSQL only.
func (b *ZInsertBuilder) Returning(columns ...Column) *ZInsertBuilder {
	b.Returnings = append(b.Returnings, columns...)
	return b
}

func (b *ZInsertBuilder) write(ctx *qutil.Context, buf []byte) []byte {
	if len(b.Sets) == 0 {
		panic("q: need at least one assignment expression to generate INSERT statements.")
	}

	buf = append(buf, b.Beginning...)
	buf = append(buf, " INTO "...)

	buf = b.Table.WriteTable(ctx, buf)
	buf = append(buf, '(')
	buf = b.Sets[0].Column.WriteColumn(ctx, buf)
	for _, s := range b.Sets[1:] {
		buf = append(buf, ", "...)
		buf = s.Column.WriteColumn(ctx, buf)
	}
	buf = append(buf, ") VALUES ("...)
	buf = b.Sets[0].Expression.WriteExpression(ctx, buf)
	for _, s := range b.Sets[1:] {
		buf = append(buf, ", "...)
		buf = s.Expression.WriteExpression(ctx, buf)
	}
	buf = append(buf, ')')

	if len(b.Returnings) > 0 && ctx.Dialect.CanUseReturning() {
		buf = append(buf, " RETURNING "...)
		buf = b.Returnings[0].WriteDefinition(ctx, buf)
		for _, c := range b.Returnings[1:] {
			buf = append(buf, ", "...)
			buf = c.WriteDefinition(ctx, buf)
		}
	}
	return buf
}

// ToSQL builds SQL and arguments.
func (b *ZInsertBuilder) ToSQL() (string, []interface{}) {
	return builderToSQL(b, b.Dialect, 128, 8, true)
}

// String implemenets fmt.Stringer interface.
func (b *ZInsertBuilder) String() string {
	return builderToString(b, b.Dialect, 128, 8, true)
}
