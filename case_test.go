package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var caseTests = []struct {
	Name string
	B    *ZCaseBuilder
	Want sql.NullInt64
	V    string
}{
	{
		Name: "empty simple case",
		B:    Case(C("id")),
		Want: sql.NullInt64{0, false},
		V:    "NULL []",
	},
	{
		Name: "else only simple case",
		B:    Case(C("id")).Else(Unsafe(0)),
		Want: sql.NullInt64{0, true},
		V:    "0 []",
	},
	{
		Name: "no else simple case unmatched",
		B:    Case(C("id")).When(0, 1),
		Want: sql.NullInt64{0, false},
		V:    `CASE "id" WHEN ? THEN ? END [0 1]`,
	},
	{
		Name: "no else simple case matched",
		B:    Case(C("id")).When(1, 2),
		Want: sql.NullInt64{2, true},
		V:    `CASE "id" WHEN ? THEN ? END [1 2]`,
	},
	{
		Name: "simple case matched",
		B:    Case(C("id")).When(1, 2).Else(1),
		Want: sql.NullInt64{2, true},
		V:    `CASE "id" WHEN ? THEN ? ELSE ? END [1 2 1]`,
	},
	{
		Name: "simple case unmatched",
		B:    Case(C("id")).When(0, 1).Else(2),
		Want: sql.NullInt64{2, true},
		V:    `CASE "id" WHEN ? THEN ? ELSE ? END [0 1 2]`,
	},
	{
		Name: "empty searched case",
		B:    Case(),
		Want: sql.NullInt64{0, false},
		V:    "NULL []",
	},
	{
		Name: "else only searched case",
		B:    Case().Else(Unsafe("0")),
		Want: sql.NullInt64{0, true},
		V:    "0 []",
	},
	{
		Name: "no else searched case unmatched",
		B:    Case().When(Eq(C("id"), 0), 1),
		Want: sql.NullInt64{0, false},
		V:    `CASE WHEN "id" = ? THEN ? END [0 1]`,
	},
	{
		Name: "no else searched case matched",
		B:    Case().When(Eq(C("id"), 1), 1),
		Want: sql.NullInt64{1, true},
		V:    `CASE WHEN "id" = ? THEN ? END [1 1]`,
	},
	{
		Name: "searched case unmatched",
		B:    Case().When(Eq(C("id"), 0), 1).Else(2),
		Want: sql.NullInt64{2, true},
		V:    `CASE WHEN "id" = ? THEN ? ELSE ? END [0 1 2]`,
	},
	{
		Name: "searched case matched",
		B:    Case().When(Eq(C("id"), 1), 2).Else(1),
		Want: sql.NullInt64{2, true},
		V:    `CASE WHEN "id" = ? THEN ? ELSE ? END [1 2 1]`,
	},
}

func TestCase(t *testing.T) {
	for i, test := range caseTests {
		if r := test.B.String(); r != test.V {
			t.Errorf("tests[%d] %s: want %q got %q", i, test.Name, test.V, r)
		}
	}
}

func TestCaseOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)
			for i, test := range caseTests {
				var r sql.NullInt64
				sql, args := Select().Column(test.B.C()).From(T("user")).Limit(1).OrderBy(C("id"), true).SetDialect(d).ToSQL()
				if err := db.QueryRow(sql, args...).Scan(&r); err != nil {
					t.Fatalf("%s tests[%d] %s Error: %v", d, i, test.Name, err)
				}
				if fmt.Sprint(r) != fmt.Sprint(test.Want) {
					t.Errorf("%s tests[%d] %s want %v got %v", d, i, test.Name, test.Want, r)
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
