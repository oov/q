package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var columnTests = []struct {
	Name string
	T    Table
	C    Column
	Want string
	V    string
}{
	{
		Name: "C(name)",
		T:    T("user"),
		C:    C("id"),
		Want: `1`,
		V:    `"id" []`,
	},
	{
		Name: "C(name).C()",
		T:    T("user"),
		C:    C("id").C(),
		Want: `1`,
		V:    `"id" []`,
	},
	{
		Name: "C(name).C(alias)",
		T:    T("user"),
		C:    C("id").C("i"),
		Want: `1`,
		V:    `"id" AS "i" []`,
	},
	{
		Name: "C(name, alias)",
		T:    T("user"),
		C:    C("id", "i"),
		Want: `1`,
		V:    `"id" AS "i" []`,
	},
	{
		Name: "C(name, alias).C()",
		T:    T("user"),
		C:    C("id", "i").C(),
		Want: `1`,
		V:    `"id" AS "i" []`,
	},
	{
		Name: "C(name, alias).C(alias2)",
		T:    T("user"),
		C:    C("id", "i").C("co"),
		Want: `1`,
		V:    `"id" AS "co" []`,
	},
	{
		Name: "T(name).C(name)",
		T:    T("user"),
		C:    T("user").C("id"),
		Want: `1`,
		V:    `"user"."id" []`,
	},
	{
		Name: "T(name).C(name, alias)",
		T:    T("user"),
		C:    T("user").C("id", "i"),
		Want: `1`,
		V:    `"user"."id" AS "i" []`,
	},
	{
		Name: "T(name).C(name).C()",
		T:    T("user"),
		C:    T("user").C("id").C(),
		Want: `1`,
		V:    `"user"."id" []`,
	},
	{
		Name: "T(name).C(name).C(alias)",
		T:    T("user"),
		C:    T("user").C("id").C("i"),
		Want: `1`,
		V:    `"user"."id" AS "i" []`,
	},
	{
		Name: "T(name).C(name, alias).C(alias2)",
		T:    T("user"),
		C:    T("user").C("id", "i").C("co"),
		Want: `1`,
		V:    `"user"."id" AS "co" []`,
	},
	{
		Name: "T(name, alias).C(name)",
		T:    T("user", "u"),
		C:    T("user", "u").C("id"),
		Want: `1`,
		V:    `"u"."id" []`,
	},
	{
		Name: "T(name, alias).C(name, alias)",
		T:    T("user", "u"),
		C:    T("user", "u").C("id", "i"),
		Want: `1`,
		V:    `"u"."id" AS "i" []`,
	},
	{
		Name: "T(name, alias).C(name).C()",
		T:    T("user", "u"),
		C:    T("user", "u").C("id").C(),
		Want: `1`,
		V:    `"u"."id" []`,
	},
	{
		Name: "T(name, alias).C(name, alias).C(alias2)",
		T:    T("user", "u"),
		C:    T("user", "u").C("id", "i").C("co"),
		Want: `1`,
		V:    `"u"."id" AS "co" []`,
	},
	{
		Name: "Case().C()",
		T:    T("user"),
		C:    Case().When(Eq(C("id"), 0), 1).Else(2).C(),
		Want: `2`,
		V:    `CASE WHEN "id" = ? THEN ? ELSE ? END [0 1 2]`,
	},
	{
		Name: "Case().C().C()",
		T:    T("user"),
		C:    Case().When(Eq(C("id"), 0), 1).Else(2).C().C(),
		Want: `2`,
		V:    `CASE WHEN "id" = ? THEN ? ELSE ? END [0 1 2]`,
	},
	{
		Name: "Case().C().C(alias)",
		T:    T("user"),
		C:    Case().When(Eq(C("id"), 0), 1).Else(2).C().C("i"),
		Want: `2`,
		V:    `CASE WHEN "id" = ? THEN ? ELSE ? END AS "i" [0 1 2]`,
	},
	{
		Name: "Case().C(alias)",
		T:    T("user"),
		C:    Case().When(Eq(C("id"), 0), 1).Else(2).C("id"),
		Want: `2`,
		V:    `CASE WHEN "id" = ? THEN ? ELSE ? END AS "id" [0 1 2]`,
	},
	{
		Name: "Case().C(alias).C()",
		T:    T("user"),
		C:    Case().When(Eq(C("id"), 0), 1).Else(2).C("id").C(),
		Want: `2`,
		V:    `CASE WHEN "id" = ? THEN ? ELSE ? END AS "id" [0 1 2]`,
	},
	{
		Name: "Case().C(alias).C(alias2)",
		T:    T("user"),
		C:    Case().When(Eq(C("id"), 0), 1).Else(2).C("id").C("i"),
		Want: `2`,
		V:    `CASE WHEN "id" = ? THEN ? ELSE ? END AS "i" [0 1 2]`,
	},
}

func TestColumn(t *testing.T) {
	for i, test := range columnTests {
		if r := fmt.Sprint(test.C); r != test.V {
			t.Errorf("tests[%d] %s: want %q got %q", i, test.Name, test.V, r)
		}
	}
}

func TestColumnOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)
			for i, test := range columnTests {
				var r string
				sql, args := Select().From(test.T).Column(test.C).Limit(1).OrderBy(C("id"), true).SetDialect(d).ToSQL()
				if err := db.QueryRow(sql, args...).Scan(&r); err != nil {
					t.Fatalf("%s tests[%d] %s Error: %v", d, i, test.Name, err)
				}
				if fmt.Sprint(r) != fmt.Sprint(test.Want) {
					t.Errorf("%s test[%d] %s want %v got %v", d, i, test.Name, test.Want, r)
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
