package q

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/oov/q/qutil"
)

var intervalTests = []struct {
	Name string
	E    Expression
	Want string
	V    map[qutil.Dialect]string
}{
	{
		Name: `1 Year`,
		E:    AddInterval(C("at"), Years(1)),
		Want: `"at" + INTERVAL 1 YEAR []`,
		V:    resultMap("SELECT `at` + INTERVAL 1 YEAR AS `i` []", `SELECT "at" + INTERVAL '1 year' AS "i" []`, `SELECT DATETIME("at", '1 year') AS "i" []`),
	},
	{
		Name: `2 Years`,
		E:    AddInterval(C("at"), Years(2)),
		Want: `"at" + INTERVAL 2 YEAR []`,
		V:    resultMap("SELECT `at` + INTERVAL 2 YEAR AS `i` []", `SELECT "at" + INTERVAL '2 years' AS "i" []`, `SELECT DATETIME("at", '2 years') AS "i" []`),
	},
	{
		Name: `1 Month`,
		E:    AddInterval(C("at"), Months(1)),
		Want: `"at" + INTERVAL 1 MONTH []`,
		V:    resultMap("SELECT `at` + INTERVAL 1 MONTH AS `i` []", `SELECT "at" + INTERVAL '1 month' AS "i" []`, `SELECT DATETIME("at", '1 month') AS "i" []`),
	},
	{
		Name: `2 Months`,
		E:    AddInterval(C("at"), Months(2)),
		Want: `"at" + INTERVAL 2 MONTH []`,
		V:    resultMap("SELECT `at` + INTERVAL 2 MONTH AS `i` []", `SELECT "at" + INTERVAL '2 months' AS "i" []`, `SELECT DATETIME("at", '2 months') AS "i" []`),
	},
	{
		Name: `1 Day`,
		E:    AddInterval(C("at"), Days(1)),
		Want: `"at" + INTERVAL 1 DAY []`,
		V:    resultMap("SELECT `at` + INTERVAL 1 DAY AS `i` []", `SELECT "at" + INTERVAL '1 day' AS "i" []`, `SELECT DATETIME("at", '1 day') AS "i" []`),
	},
	{
		Name: `2 Days`,
		E:    AddInterval(C("at"), Days(2)),
		Want: `"at" + INTERVAL 2 DAY []`,
		V:    resultMap("SELECT `at` + INTERVAL 2 DAY AS `i` []", `SELECT "at" + INTERVAL '2 days' AS "i" []`, `SELECT DATETIME("at", '2 days') AS "i" []`),
	},
	{
		Name: `1 Hour`,
		E:    AddInterval(C("at"), Hours(1)),
		Want: `"at" + INTERVAL 1 HOUR []`,
		V:    resultMap("SELECT `at` + INTERVAL 1 HOUR AS `i` []", `SELECT "at" + INTERVAL '1 hour' AS "i" []`, `SELECT DATETIME("at", '1 hour') AS "i" []`),
	},
	{
		Name: `2 Hours`,
		E:    AddInterval(C("at"), Hours(2)),
		Want: `"at" + INTERVAL 2 HOUR []`,
		V:    resultMap("SELECT `at` + INTERVAL 2 HOUR AS `i` []", `SELECT "at" + INTERVAL '2 hours' AS "i" []`, `SELECT DATETIME("at", '2 hours') AS "i" []`),
	},
	{
		Name: `1 Minute`,
		E:    AddInterval(C("at"), Minutes(1)),
		Want: `"at" + INTERVAL 1 MINUTE []`,
		V:    resultMap("SELECT `at` + INTERVAL 1 MINUTE AS `i` []", `SELECT "at" + INTERVAL '1 minute' AS "i" []`, `SELECT DATETIME("at", '1 minute') AS "i" []`),
	},
	{
		Name: `2 Minutes`,
		E:    AddInterval(C("at"), Minutes(2)),
		Want: `"at" + INTERVAL 2 MINUTE []`,
		V:    resultMap("SELECT `at` + INTERVAL 2 MINUTE AS `i` []", `SELECT "at" + INTERVAL '2 minutes' AS "i" []`, `SELECT DATETIME("at", '2 minutes') AS "i" []`),
	},
	{
		Name: `1 Second`,
		E:    AddInterval(C("at"), Seconds(1)),
		Want: `"at" + INTERVAL 1 SECOND []`,
		V:    resultMap("SELECT `at` + INTERVAL 1 SECOND AS `i` []", `SELECT "at" + INTERVAL '1 second' AS "i" []`, `SELECT DATETIME("at", '1 second') AS "i" []`),
	},
	{
		Name: `2 Seconds`,
		E:    AddInterval(C("at"), Seconds(2)),
		Want: `"at" + INTERVAL 2 SECOND []`,
		V:    resultMap("SELECT `at` + INTERVAL 2 SECOND AS `i` []", `SELECT "at" + INTERVAL '2 seconds' AS "i" []`, `SELECT DATETIME("at", '2 seconds') AS "i" []`),
	},
}

func TestInterval(t *testing.T) {
	for i, test := range intervalTests {
		for d := range testModel {
			if r := fmt.Sprint(test.E); r != test.Want {
				t.Errorf("%s test[%d] %s Stringer want %s got %s", d, i, test.Name, test.Want, r)
			}
			if r := Select().SetDialect(d).Column(test.E.C("i")).String(); r != test.V[d] {
				t.Errorf("%s test[%d] %s want %s got %s", d, i, test.Name, test.V[d], r)
			}
		}
	}
}

func TestIntervalOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)
			for i, test := range intervalTests {
				var r int64
				sql, args := Select().SetDialect(d).Column(C("id")).From(T("post")).Where(Eq(C("id"), 1), Neq(C("at"), test.E)).ToSQL()
				if err := db.QueryRow(sql, args...).Scan(&r); err != nil {
					t.Fatalf("%s tests[%d] %s Error: %v\n%s", d, i, test.Name, err, sql)
				}
				if r != 1 {
					t.Errorf("%s test[%d] %s want 1 got %v", d, i, test.Name, r)
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestNowOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)
			post := T("post")
			sql, args := Insert().SetDialect(d).Into(post).
				Set(post.C("id"), 100).
				Set(post.C("user_id"), 1).
				Set(post.C("title"), "test").
				Set(post.C("at"), Now()).
				ToSQL()
			if _, err := db.Exec(sql, args...); err != nil {
				t.Fatalf("%s Error: %v\n%s", d, err, sql)
			}

			want := time.Now().In(time.UTC)
			var r time.Time
			sql, args = Select().SetDialect(d).Column(C("at")).From(T("post")).Where(Eq(C("id"), 100)).ToSQL()
			if err := db.QueryRow(sql, args...).Scan(&r); err != nil {
				t.Fatalf("%s Error: %v\n%s", d, err, sql)
			}
			if r.Before(want.Add(-5*time.Minute)) || r.After(want.Add(5*time.Minute)) {
				t.Errorf("%s want %s got %s", d, want.Format("2006-01-02 15:04:05"), r.Format("2006-01-02 15:04:05"))
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
