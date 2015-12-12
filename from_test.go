package q

import (
	"testing"
)

var fromTests = []struct {
	params [][]string
	q      string
}{
	{
		params: [][]string{{"table"}},
		q:      "SELECT * FROM `table`",
	},
	{
		params: [][]string{{"table", "alias"}},
		q:      "SELECT * FROM `table` AS `alias`",
	},
	{
		params: [][]string{{"table"}, {"table2"}},
		q:      "SELECT * FROM `table`, `table2`",
	},
	{
		params: [][]string{{"table", "alias"}, {"table2", "alias2"}},
		q:      "SELECT * FROM `table` AS `alias`, `table2` AS `alias2`",
	},
	{
		params: [][]string{{"table", "alias"}, {"table2"}},
		q:      "SELECT * FROM `table` AS `alias`, `table2`",
	},
}

func TestSelectFrom(t *testing.T) {
	for i, test := range fromTests {
		sel := Select()
		for _, ps := range test.params {
			sel.From(T(ps[0], ps[1:]...))
		}
		if q, _ := sel.SetDialect(MySQL).ToSQL(); q != test.q {
			t.Errorf("tests[%d]: want %q got %q", i, test.q, q)
		}
	}
}
