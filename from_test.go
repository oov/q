package q

import (
	"testing"
)

func TestSelectFrom(t *testing.T) {
	type params struct {
		first interface{}
		other []string
	}
	tests := []struct {
		params []params
		q      SQL
	}{
		{
			params: []params{{first: "table"}},
			q:      sqlBytes("SELECT * FROM `table`"),
		},
		{
			params: []params{{first: "table", other: []string{"alias"}}},
			q:      sqlBytes("SELECT * FROM `table` AS `alias`"),
		},
		{
			params: []params{{first: "table"}, {first: "table2"}},
			q:      sqlBytes("SELECT * FROM `table`, `table2`"),
		},
		{
			params: []params{{first: "table", other: []string{"alias"}}, {first: "table2", other: []string{"alias2"}}},
			q:      sqlBytes("SELECT * FROM `table` AS `alias`, `table2` AS `alias2`"),
		},
		{
			params: []params{{first: "table", other: []string{"alias"}}, {first: "table2"}},
			q:      sqlBytes("SELECT * FROM `table` AS `alias`, `table2`"),
		},
		{
			params: []params{{first: Select().From(T("table2"))}},
			q:      sqlBytes("SELECT * FROM (SELECT * FROM `table2`)"),
		},
		{
			params: []params{{first: Select().From(T("table2")), other: []string{"sql"}}},
			q:      sqlBytes("SELECT * FROM (SELECT * FROM `table2`) AS `sql`"),
		},
	}
	for i, test := range tests {
		sel := Select()
		for _, ps := range test.params {
			sel.From(T(ps.first, ps.other...))
		}
		if q, _ := sel.ToSQL(MySQL); q.String() != test.q.String() {
			t.Errorf("tests[%d]: want %q got %q", i, test.q, q)
		}
	}
}
