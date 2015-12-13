package q

import (
	"fmt"
	"testing"
)

func TestTable(t *testing.T) {
	tests := []struct {
		Name string
		T    Table
		Want string
	}{
		{
			Name: "T(name)",
			T:    T("tbl"),
			Want: `"tbl" []`,
		},
		{
			Name: "T(name).InnerJoin(T(name))",
			T:    T("tbl").InnerJoin(T("tbl2")),
			Want: `"tbl" INNER JOIN "tbl2" []`,
		},
		{
			Name: "T(name).InnerJoin(T(name, alias))",
			T:    T("tbl").InnerJoin(T("tbl2", "t2")),
			Want: `"tbl" INNER JOIN "tbl2" AS "t2" []`,
		},
		{
			Name: "T(name).InnerJoin(T(name), expr)",
			T:    T("tbl").InnerJoin(T("tbl2"), Eq(C("a"), C("b"))),
			Want: `"tbl" INNER JOIN "tbl2" ON "a" = "b" []`,
		},
		{
			Name: "T(name).InnerJoin(T(name, alias), expr)",
			T:    T("tbl").InnerJoin(T("tbl2", "t2"), Eq(C("a"), C("b"))),
			Want: `"tbl" INNER JOIN "tbl2" AS "t2" ON "a" = "b" []`,
		},
		{
			Name: "T(name).LeftJoin(T(name))",
			T:    T("tbl").LeftJoin(T("tbl2")),
			Want: `"tbl" LEFT JOIN "tbl2" []`,
		},
		{
			Name: "T(name).LeftJoin(T(name, alias))",
			T:    T("tbl").LeftJoin(T("tbl2", "t2")),
			Want: `"tbl" LEFT JOIN "tbl2" AS "t2" []`,
		},
		{
			Name: "T(name).LeftJoin(T(name), expr)",
			T:    T("tbl").LeftJoin(T("tbl2"), Eq(C("a"), C("b"))),
			Want: `"tbl" LEFT JOIN "tbl2" ON "a" = "b" []`,
		},
		{
			Name: "T(name).LeftJoin(T(name, alias), expr)",
			T:    T("tbl").LeftJoin(T("tbl2", "t2"), Eq(C("a"), C("b"))),
			Want: `"tbl" LEFT JOIN "tbl2" AS "t2" ON "a" = "b" []`,
		},
		{
			Name: "T(name).CrossJoin(T(name))",
			T:    T("tbl").CrossJoin(T("tbl2")),
			Want: `"tbl" CROSS JOIN "tbl2" []`,
		},
		{
			Name: "T(name).CrossJoin(T(name, alias))",
			T:    T("tbl").CrossJoin(T("tbl2", "t2")),
			Want: `"tbl" CROSS JOIN "tbl2" AS "t2" []`,
		},
		{
			Name: "T(name).CrossJoin(T(name), expr)",
			T:    T("tbl").CrossJoin(T("tbl2"), Eq(C("a"), C("b"))),
			Want: `"tbl" CROSS JOIN "tbl2" ON "a" = "b" []`,
		},
		{
			Name: "T(name).CrossJoin(T(name, alias), expr)",
			T:    T("tbl").CrossJoin(T("tbl2", "t2"), Eq(C("a"), C("b"))),
			Want: `"tbl" CROSS JOIN "tbl2" AS "t2" ON "a" = "b" []`,
		},
		{
			Name: "T(name, alias)",
			T:    T("tbl", "t"),
			Want: `"tbl" AS "t" []`,
		},
		{
			Name: "T(name, alias).InnerJoin(T(name))",
			T:    T("tbl", "t").InnerJoin(T("tbl2")),
			Want: `"tbl" AS "t" INNER JOIN "tbl2" []`,
		},
		{
			Name: "T(name, alias).LeftJoin(T(name))",
			T:    T("tbl", "t").LeftJoin(T("tbl2")),
			Want: `"tbl" AS "t" LEFT JOIN "tbl2" []`,
		},
		{
			Name: "T(name, alias).CrossJoin(T(name))",
			T:    T("tbl", "t").CrossJoin(T("tbl2")),
			Want: `"tbl" AS "t" CROSS JOIN "tbl2" []`,
		},
		{
			Name: "*SelectBuilder.T()",
			T:    Select().From(T("tbl")).T(),
			Want: `(SELECT * FROM "tbl") []`,
		},
		{
			Name: "*SelectBuilder.T().InnerJoin(T(name))",
			T:    Select().From(T("tbl")).T().InnerJoin(T("tbl2")),
			Want: `(SELECT * FROM "tbl") INNER JOIN "tbl2" []`,
		},
		{
			Name: "*SelectBuilder.T().LeftJoin(T(name))",
			T:    Select().From(T("tbl")).T().LeftJoin(T("tbl2")),
			Want: `(SELECT * FROM "tbl") LEFT JOIN "tbl2" []`,
		},
		{
			Name: "*SelectBuilder.T().CrossJoin(T(name))",
			T:    Select().From(T("tbl")).T().CrossJoin(T("tbl2")),
			Want: `(SELECT * FROM "tbl") CROSS JOIN "tbl2" []`,
		},
		{
			Name: "*SelectBuilder.T(alias)",
			T:    Select().From(T("tbl")).T("t"),
			Want: `(SELECT * FROM "tbl") AS "t" []`,
		},
	}
	for i, test := range tests {
		if r := fmt.Sprint(test.T); r != test.Want {
			t.Errorf("tests[%d] %s: want %q got %q", i, test.Name, test.Want, r)
		}
	}
}
