package q

import (
	"fmt"

	"github.com/oov/q/qutil"
)

// Table represents database table.
// You can create it from T or *SelectBuilder.T.
type Table interface {
	C(columnName string, aliasName ...string) Column

	InnerJoin(table Table, conds ...Expression) Table
	LeftJoin(table Table, conds ...Expression) Table
	CrossJoin(table Table, conds ...Expression) Table

	JoinIndex(i int) (string, Table, Expressions)
	JoinLen() int

	// for internal use.
	WriteTable(ctx *qutil.Context, buf []byte) []byte
	WriteJoins(ctx *qutil.Context, buf []byte) []byte
	WriteDefinition(ctx *qutil.Context, buf []byte) []byte
}

func tableToString(t Table) string {
	buf, ctx := qutil.NewContext(t, 32, 0, nil)
	buf = t.WriteDefinition(ctx, buf)
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

func (j *joinable) WriteJoins(ctx *qutil.Context, buf []byte) []byte {
	for _, v := range j.Joins {
		buf = append(buf, ' ')
		buf = append(buf, v.Type...)
		buf = append(buf, " JOIN "...)
		hasJoins := v.Table.JoinLen() > 0
		if hasJoins {
			buf = append(buf, '(')
		}
		buf = v.Table.WriteDefinition(ctx, buf)
		if hasJoins {
			buf = append(buf, ')')
		}
		if v.Conds.Len() > 0 {
			buf = append(buf, " ON "...)
			buf = v.Conds.WriteExpression(ctx, buf)
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
	return tableToString(t)
}

func (t *tableAlias) C(columnName string, aliasName ...string) Column {
	return columnTable(t, columnName, aliasName...)
}

func (t *tableAlias) WriteTable(ctx *qutil.Context, buf []byte) []byte {
	if ctx.CUD {
		return t.Table.WriteTable(ctx, buf)
	}
	return ctx.Dialect.Quote(buf, t.Alias)
}

func (t *tableAlias) WriteDefinition(ctx *qutil.Context, buf []byte) []byte {
	if ctx.CUD {
		return t.Table.WriteTable(ctx, buf)
	}
	buf = t.Table.WriteTable(ctx, buf)
	buf = append(buf, " AS "...)
	buf = t.WriteTable(ctx, buf)
	buf = t.WriteJoins(ctx, buf)
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
	return tableToString(t)
}

func (t *table) WriteTable(ctx *qutil.Context, buf []byte) []byte {
	return ctx.Dialect.Quote(buf, t.Table)
}

func (t *table) WriteDefinition(ctx *qutil.Context, buf []byte) []byte {
	if ctx.CUD {
		return t.WriteTable(ctx, buf)
	}
	buf = t.WriteTable(ctx, buf)
	buf = t.WriteJoins(ctx, buf)
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
	return tableToString(t)
}

func (t *selectBuilderAsTable) WriteTable(ctx *qutil.Context, buf []byte) []byte {
	buf = append(buf, '(')
	buf = t.SelectBuilder.write(ctx, buf)
	buf = append(buf, ')')
	return buf
}

func (t *selectBuilderAsTable) WriteDefinition(ctx *qutil.Context, buf []byte) []byte {
	buf = t.WriteTable(ctx, buf)
	buf = t.WriteJoins(ctx, buf)
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

// T creates Table.
func T(tableName string, aliasName ...string) Table {
	r := &table{Table: tableName}
	if len(aliasName) == 0 {
		return r
	}
	return &tableAlias{Table: r, Alias: aliasName[0]}
}
