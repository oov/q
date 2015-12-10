package q

import (
	"fmt"
	"testing"
)

func TestColumn(t *testing.T) {
	tests := []struct {
		Name string
		C    Column
		Want string
	}{
		{
			Name: "C(name)",
			C:    C("col"),
			Want: `"col" []`,
		},
		{
			Name: "C(name).C()",
			C:    C("col").C(),
			Want: `"col" []`,
		},
		{
			Name: "C(name).C(alias)",
			C:    C("col").C("c"),
			Want: `"col" AS "c" []`,
		},
		{
			Name: "C(name, alias)",
			C:    C("col", "c"),
			Want: `"col" AS "c" []`,
		},
		{
			Name: "C(name, alias).C()",
			C:    C("col", "c").C(),
			Want: `"col" AS "c" []`,
		},
		{
			Name: "C(name, alias).C(alias2)",
			C:    C("col", "c").C("co"),
			Want: `"col" AS "co" []`,
		},
		{
			Name: "T(name).C(name)",
			C:    T("test").C("col"),
			Want: `"test"."col" []`,
		},
		{
			Name: "T(name).C(name, alias)",
			C:    T("test").C("col", "c"),
			Want: `"test"."col" AS "c" []`,
		},
		{
			Name: "T(name).C(name).C()",
			C:    T("test").C("col").C(),
			Want: `"test"."col" []`,
		},
		{
			Name: "T(name).C(name).C(alias)",
			C:    T("test").C("col").C("c"),
			Want: `"test"."col" AS "c" []`,
		},
		{
			Name: "T(name).C(name, alias).C(alias2)",
			C:    T("test").C("col", "c").C("co"),
			Want: `"test"."col" AS "co" []`,
		},
		{
			Name: "T(name, alias).C(name)",
			C:    T("test", "t").C("col"),
			Want: `"t"."col" []`,
		},
		{
			Name: "T(name, alias).C(name, alias)",
			C:    T("test", "t").C("col", "c"),
			Want: `"t"."col" AS "c" []`,
		},
		{
			Name: "T(name, alias).C(name).C()",
			C:    T("test", "t").C("col").C(),
			Want: `"t"."col" []`,
		},
		{
			Name: "T(name, alias).C(name, alias).C(alias2)",
			C:    T("test", "t").C("col", "c").C("co"),
			Want: `"t"."col" AS "co" []`,
		},
		{
			Name: "Case().C()",
			C:    Case().When(Eq(C("col"), 0), 1).Else(2).C(),
			Want: `CASE WHEN "col" = ? THEN ? ELSE ? END [0 1 2]`,
		},
		{
			Name: "Case().C().C()",
			C:    Case().When(Eq(C("col"), 0), 1).Else(2).C().C(),
			Want: `CASE WHEN "col" = ? THEN ? ELSE ? END [0 1 2]`,
		},
		{
			Name: "Case().C().C(alias)",
			C:    Case().When(Eq(C("col"), 0), 1).Else(2).C().C("c"),
			Want: `CASE WHEN "col" = ? THEN ? ELSE ? END AS "c" [0 1 2]`,
		},
		{
			Name: "Case().C(alias)",
			C:    Case().When(Eq(C("col"), 0), 1).Else(2).C("col"),
			Want: `CASE WHEN "col" = ? THEN ? ELSE ? END AS "col" [0 1 2]`,
		},
		{
			Name: "Case().C(alias).C()",
			C:    Case().When(Eq(C("col"), 0), 1).Else(2).C("col").C(),
			Want: `CASE WHEN "col" = ? THEN ? ELSE ? END AS "col" [0 1 2]`,
		},
		{
			Name: "Case().C(alias).C(alias2)",
			C:    Case().When(Eq(C("col"), 0), 1).Else(2).C("col").C("c"),
			Want: `CASE WHEN "col" = ? THEN ? ELSE ? END AS "c" [0 1 2]`,
		},
	}
	for i, test := range tests {
		if r := fmt.Sprint(test.C); r != test.Want {
			t.Errorf("tests[%d] %s: want %q got %q", i, test.Name, test.Want, r)
		}
	}
}
