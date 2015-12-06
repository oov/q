package q

import (
	"fmt"
)

// Table represents database table.
// You can create it from T.
type Table interface {
	C(columnName string, aliasName ...string) Column

	InnerJoin(table Table, conds ...Expression) Table
	LeftJoin(table Table, conds ...Expression) Table
	CrossJoin(table Table, conds ...Expression) Table

	JoinIndex(i int) (string, Table, Expressions)
	JoinLen() int

	writeTable(ctx *ctx, buf []byte) []byte
	writeJoins(ctx *ctx, buf []byte) []byte
	writeDefinition(ctx *ctx, buf []byte) []byte
}

func tablerToString(t Table) string {
	buf, ctx := newDummyCtx(32, 0)
	buf = t.writeDefinition(ctx, buf)
	buf = append(buf, ' ')
	return fmt.Sprint(string(buf), ctx.Args)
}

type join struct {
	Type  string
	Table Table
	Conds Expressions
}

type joinable struct {
	Joins []join
}

func (j *joinable) InnerJoin(table Table, conds ...Expression) {
	j.Joins = append(j.Joins, join{
		Type:  "INNER",
		Table: table,
		Conds: And(conds...),
	})
}

func (j *joinable) LeftJoin(table Table, conds ...Expression) {
	j.Joins = append(j.Joins, join{
		Type:  "LEFT",
		Table: table,
		Conds: And(conds...),
	})
}

func (j *joinable) CrossJoin(table Table, conds ...Expression) {
	j.Joins = append(j.Joins, join{
		Type:  "CROSS",
		Table: table,
		Conds: And(conds...),
	})
}

func (j *joinable) JoinIndex(i int) (string, Table, Expressions) {
	jd := j.Joins[i]
	return jd.Type, jd.Table, jd.Conds
}

func (j *joinable) JoinLen() int {
	return len(j.Joins)
}

func (j *joinable) writeJoins(ctx *ctx, buf []byte) []byte {
	for _, v := range j.Joins {
		buf = append(buf, ' ')
		buf = append(buf, v.Type...)
		buf = append(buf, " JOIN "...)
		hasJoins := v.Table.JoinLen() > 0
		if hasJoins {
			buf = append(buf, '(')
		}
		buf = v.Table.writeDefinition(ctx, buf)
		if hasJoins {
			buf = append(buf, ')')
		}
		if v.Conds.Len() > 0 {
			buf = append(buf, " ON "...)
			buf = v.Conds.writeExpression(ctx, buf)
		}
	}
	return buf
}

func columnTable(table Table, columnName string, aliasName ...string) Column {
	r := &columnWithTable{Table: table, column: column(columnName)}
	if len(aliasName) == 0 {
		return r
	}
	return &columnAlias{Column: r, Alias: aliasName[0]}
}

type tableAlias struct {
	Table
	Alias string
}

func (t *tableAlias) String() string {
	return tablerToString(t)
}

func (t *tableAlias) C(columnName string, aliasName ...string) Column {
	return columnTable(t, columnName, aliasName...)
}

func (t *tableAlias) writeTable(ctx *ctx, buf []byte) []byte {
	return ctx.Dialect.Quote(buf, t.Alias)
}

func (t *tableAlias) writeDefinition(c *ctx, buf []byte) []byte {
	buf = t.Table.writeTable(c, buf)
	buf = append(buf, " AS "...)
	buf = t.writeTable(c, buf)
	buf = t.writeJoins(c, buf)
	return buf
}

func (t *tableAlias) InnerJoin(table Table, conds ...Expression) Table {
	t.Table.InnerJoin(table, conds...)
	return t
}

func (t *tableAlias) LeftJoin(table Table, conds ...Expression) Table {
	t.Table.LeftJoin(table, conds...)
	return t
}

func (t *tableAlias) CrossJoin(table Table, conds ...Expression) Table {
	t.Table.CrossJoin(table, conds...)
	return t
}

type table struct {
	Table string
	joinable
}

func (t *table) String() string {
	return tablerToString(t)
}

func (t *table) writeTable(ctx *ctx, buf []byte) []byte {
	return ctx.Dialect.Quote(buf, t.Table)
}

func (t *table) writeDefinition(ctx *ctx, buf []byte) []byte {
	buf = t.writeTable(ctx, buf)
	buf = t.writeJoins(ctx, buf)
	return buf
}

func (t *table) C(columnName string, aliasName ...string) Column {
	return columnTable(t, columnName, aliasName...)
}

func (t *table) InnerJoin(table Table, conds ...Expression) Table {
	t.joinable.InnerJoin(table, conds...)
	return t
}

func (t *table) LeftJoin(table Table, conds ...Expression) Table {
	t.joinable.LeftJoin(table, conds...)
	return t
}

func (t *table) CrossJoin(table Table, conds ...Expression) Table {
	t.joinable.CrossJoin(table, conds...)
	return t
}

type selectBuilderAsTable struct {
	*SelectBuilder
	joinable
}

func (t *selectBuilderAsTable) String() string {
	return tablerToString(t)
}

func (t *selectBuilderAsTable) writeTable(ctx *ctx, buf []byte) []byte {
	buf = append(buf, '(')
	buf = t.SelectBuilder.write(ctx, buf)
	buf = append(buf, ')')
	return buf
}

func (t *selectBuilderAsTable) writeDefinition(ctx *ctx, buf []byte) []byte {
	buf = t.writeTable(ctx, buf)
	buf = t.writeJoins(ctx, buf)
	return buf
}

func (t *selectBuilderAsTable) C(columnName string, aliasName ...string) Column {
	return columnTable(t, columnName, aliasName...)
}

func (t *selectBuilderAsTable) InnerJoin(table Table, conds ...Expression) Table {
	t.joinable.InnerJoin(table, conds...)
	return t
}

func (t *selectBuilderAsTable) LeftJoin(table Table, conds ...Expression) Table {
	t.joinable.LeftJoin(table, conds...)
	return t
}

func (t *selectBuilderAsTable) CrossJoin(table Table, conds ...Expression) Table {
	t.joinable.CrossJoin(table, conds...)
	return t
}

// T creates Table. You can pass the following types:
//	string
//	*q.SelectBuilder
func T(t interface{}, aliasName ...string) Table {
	var r Table
	switch v := t.(type) {
	case *SelectBuilder:
		r = &selectBuilderAsTable{SelectBuilder: v}
	case *tableAlias:
		// prevents double wrapping
		if len(aliasName) > 0 {
			v.Alias = aliasName[0]
		}
		return v
	case Table:
		r = v
	case string:
		r = &table{Table: v}
	default:
		panic(fmt.Sprintf("q: %T is not a valid table type", t))
	}
	if len(aliasName) == 0 {
		return r
	}
	return &tableAlias{Table: r, Alias: aliasName[0]}
}
