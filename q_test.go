package q

import (
	"testing"
)

func TestSelect(t *testing.T) {
	rq, rv := "SELECT * FROM `table` WHERE `test` = ?", []interface{}{1}
	query, values := Select().From(T("table")).Where(Eq(C("test"), 1)).SetDialect(MySQL).ToSQL()
	if query != rq {
		t.Errorf("query: want %q got %q", rq, query)
	}
	if len(values) != len(rv) {
		t.Errorf("len(values): want %d got %d", len(rv), len(values))
	}
}
