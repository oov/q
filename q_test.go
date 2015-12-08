package q

import (
	"testing"
)

func TestSelect(t *testing.T) {
	rq, rv := sqlBytes("SELECT * FROM `table` WHERE `test` = ?"), []interface{}{1}
	query, values := Select().From(T("table")).Where(Eq(C("test"), 1)).SetDialect(MySQL).SQL()
	if query.String() != rq.String() {
		t.Errorf("query: want %q got %q", rq, query)
	}
	if len(values) != len(rv) {
		t.Errorf("len(values): want %d got %d", len(rv), len(values))
	}
}
