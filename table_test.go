package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var tableTests = []struct {
	Name string
	T    Table
	Col  func(sel *ZSelectBuilder, t Table) *ZSelectBuilder
	Want []string
	V    string
	Skip map[qutil.Dialect]string
}{
	{
		Name: "T(name)",
		T:    T("user"),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			return sel.Column(t.C("id"), t.C("name"), t.C("age"))
		},
		Want: []string{"1", "Shipon", "15"},
		V:    `"user" []`,
	},
	{
		Name: "T(name).InnerJoin(T(name))",
		T:    T("user").InnerJoin(T("post")),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" INNER JOIN "post" []`,
		Skip: map[qutil.Dialect]string{
		// supported by workaround.
		// PostgreSQL: `ERROR: syntax error at or near "LIMIT" (SQLSTATE 42601)`,
		},
	},
	{
		Name: "T(name).InnerJoin(T(name, alias))",
		T:    T("user").InnerJoin(T("post", "p")),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" INNER JOIN "post" AS "p" []`,
		Skip: map[qutil.Dialect]string{
		// supported by workaround.
		// PostgreSQL: `ERROR: syntax error at or near "LIMIT" (SQLSTATE 42601)`,
		},
	},
	{
		Name: "T(name).InnerJoin(T(name), expr)",
		T: func() Table {
			user := T("user")
			return user.InnerJoin(T("post"), Eq(user.C("id"), C("user_id")))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" INNER JOIN "post" ON "user"."id" = "user_id" []`,
	},
	{
		Name: "T(name).InnerJoin(T(name, alias), expr)",
		T: func() Table {
			user := T("user")
			return user.InnerJoin(T("post", "p"), Eq(user.C("id"), C("user_id")))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" INNER JOIN "post" AS "p" ON "user"."id" = "user_id" []`,
	},
	{
		Name: "T(name).LeftJoin(T(name))",
		T:    T("user").LeftJoin(T("post")),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" LEFT JOIN "post" []`,
		Skip: map[qutil.Dialect]string{
		// supported by workaround.
		// MySQL:      `Error 1064: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'LIMIT ?' at line 1`,
		// PostgreSQL: `ERROR: syntax error at or near "LIMIT" (SQLSTATE 42601)`,
		},
	},
	{
		Name: "T(name).LeftJoin(T(name, alias))",
		T:    T("user").LeftJoin(T("post", "p")),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" LEFT JOIN "post" AS "p" []`,
		Skip: map[qutil.Dialect]string{
		// supported by workaround.
		// MySQL:      `Error 1064: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'LIMIT ?' at line 1`,
		// PostgreSQL: `ERROR: syntax error at or near "LIMIT" (SQLSTATE 42601)`,
		},
	},
	{
		Name: "T(name).LeftJoin(T(name), expr)",
		T: func() Table {
			user := T("user")
			return user.LeftJoin(T("post"), Eq(user.C("id"), C("user_id")))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" LEFT JOIN "post" ON "user"."id" = "user_id" []`,
	},
	{
		Name: "T(name).LeftJoin(T(name, alias), expr)",
		T: func() Table {
			user := T("user")
			return user.LeftJoin(T("post", "p"), Eq(user.C("id"), C("user_id")))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" LEFT JOIN "post" AS "p" ON "user"."id" = "user_id" []`,
	},
	{
		Name: "T(name).CrossJoin(T(name))",
		T:    T("user").CrossJoin(T("post")),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" CROSS JOIN "post" []`,
	},
	{
		Name: "T(name).CrossJoin(T(name, alias))",
		T:    T("user").CrossJoin(T("post", "p")),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" CROSS JOIN "post" AS "p" []`,
	},
	{
		Name: "T(name, alias)",
		T:    T("user", "u"),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			return sel.Column(t.C("id"), t.C("name"), t.C("age"))
		},
		Want: []string{"1", "Shipon", "15"},
		V:    `"user" AS "u" []`,
	},
	{
		Name: "T(name, alias).InnerJoin(T(name))",
		T: func() Table {
			user := T("user", "u")
			return user.InnerJoin(T("post"), Eq(user.C("id"), C("user_id")))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" AS "u" INNER JOIN "post" ON "u"."id" = "user_id" []`,
	},
	{
		Name: "T(name, alias).LeftJoin(T(name))",
		T: func() Table {
			user := T("user", "u")
			return user.LeftJoin(T("post"), Eq(user.C("id"), C("user_id")))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" AS "u" LEFT JOIN "post" ON "u"."id" = "user_id" []`,
	},
	{
		Name: "T(name, alias).CrossJoin(T(name))",
		T: func() Table {
			user := T("user", "u")
			return user.CrossJoin(T("post"))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `"user" AS "u" CROSS JOIN "post" []`,
	},
	{
		Name: "*SelectBuilder.T(alias)",
		T:    Select().From(T("user")).T("u"),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			return sel.Column(t.C("id"), t.C("name"), t.C("age"))
		},
		Want: []string{"1", "Shipon", "15"},
		V:    `(SELECT * FROM "user") AS "u" []`,
	},
	{
		Name: "*SelectBuilder.T(alias).InnerJoin(T(name))",
		T: func() Table {
			user := Select().From(T("user")).T("u")
			return user.InnerJoin(T("post"), Eq(user.C("id"), C("user_id")))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `(SELECT * FROM "user") AS "u" INNER JOIN "post" ON "u"."id" = "user_id" []`,
	},
	{
		Name: "*SelectBuilder.T(alias).LeftJoin(T(name))",
		T: func() Table {
			user := Select().From(T("user")).T("u")
			return user.LeftJoin(T("post"), Eq(user.C("id"), C("user_id")))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `(SELECT * FROM "user") AS "u" LEFT JOIN "post" ON "u"."id" = "user_id" []`,
	},
	{
		Name: "*SelectBuilder.T(alias).CrossJoin(T(name))",
		T: func() Table {
			user := Select().From(T("user")).T("u")
			return user.CrossJoin(T("post"))
		}(),
		Col: func(sel *ZSelectBuilder, t Table) *ZSelectBuilder {
			_, t2, _ := t.JoinIndex(0)
			return sel.Column(t.C("id"), t.C("name"), t.C("age"), t2.C("id"), t2.C("user_id"), t2.C("title"))
		},
		Want: []string{"1", "Shipon", "15", "1", "1", "昨日見た夢の内容が凄い"},
		V:    `(SELECT * FROM "user") AS "u" CROSS JOIN "post" []`,
	},
}

func TestTableJoinIndex(t *testing.T) {
	user := T("user")
	want := `INNER "post" [] "user"."id" = "user_id" []`
	jt, tbl, conds := user.InnerJoin(T("post"), Eq(user.C("id"), C("user_id"))).JoinIndex(0)
	if r := fmt.Sprint(jt, " ", tbl, " ", conds); r != want {
		t.Errorf("want %q got %q", want, r)
	}
}

func TestTable(t *testing.T) {
	for i, test := range tableTests {
		if r := fmt.Sprint(test.T); r != test.V {
			t.Errorf("tests[%d] %s: want %s got %s", i, test.Name, test.V, r)
		}
	}
}

func TestTableOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)
			// deletes unnecessary record
			db.Exec("DELETE FROM user WHERE id != 1")
			db.Exec("DELETE FROM post WHERE id != 1")
			for i, test := range tableTests {
				r, rp := make([]string, len(test.Want)), make([]interface{}, len(test.Want))
				for i := range r {
					rp[i] = &r[i]
				}
				sql, args := test.Col(Select().From(test.T).Limit(1).SetDialect(d), test.T).ToSQL()
				if err := db.QueryRow(sql, args...).Scan(rp...); err != nil {
					if msg, skip := test.Skip[d]; skip && err.Error() == msg {
						continue
					}
					t.Fatalf("%s tests[%d] %s Error: %v\n%s", d, i, test.Name, err, sql)
				}
				if msg, skip := test.Skip[d]; skip {
					t.Fatalf("%s tests[%d] %s want %q error, got nothing", d, i, test.Name, msg)
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
