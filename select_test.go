package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var selectTests = []struct {
	Name string
	B    *ZSelectBuilder
	Cols []string
	Want [][]string
	V    string
}{
	{
		Name: "begnning",
		B:    Select("select").From(T("user")),
		Cols: []string{"id", "name", "age"},
		Want: [][]string{
			{"1", "Shipon", "15"},
			{"2", "Mr.TireMan", "44"},
		},
		V: `select * FROM "user" []`,
	},
	{
		Name: "Simple Select",
		B:    Select().From(T("user")),
		Cols: []string{"id", "name", "age"},
		Want: [][]string{
			{"1", "Shipon", "15"},
			{"2", "Mr.TireMan", "44"},
		},
		V: `SELECT * FROM "user" []`,
	},
	{
		Name: "Single Join",
		B: func() *ZSelectBuilder {
			user, post := T("user", "u"), T("post", "p")
			return Select().From(post.InnerJoin(
				user,
				Eq(post.C("user_id"), user.C("id")),
			))
		}(),
		Cols: []string{"id", "user_id", "title", "id", "name", "age"},
		Want: [][]string{
			{"1", "1", "昨日見た夢の内容が凄い", "1", "Shipon", "15"},
			{"2", "2", "氷の上で滑るタイヤの原因とは？", "2", "Mr.TireMan", "44"},
			{"3", "1", "嘘じゃないんです", "1", "Shipon", "15"},
			{"4", "2", "最近仕事が辛い", "2", "Mr.TireMan", "44"},
		},
		V: `SELECT * FROM "post" AS "p" INNER JOIN "user" AS "u" ON "p"."user_id" = "u"."id" []`,
	},
	{
		Name: "Multiple Join",
		B: func() *ZSelectBuilder {
			post, posttag, tag := T("post", "p"), T("posttag", "pt"), T("tag", "t")
			return Select().Column(
				post.C("id", "i"),
				tag.C("value", "v"),
			).From(
				post.InnerJoin(
					posttag.InnerJoin(
						tag,
						Eq(posttag.C("tag_id"), tag.C("id")),
					),
					Eq(post.C("id"), posttag.C("post_id")),
				),
			)
		}(),
		Cols: []string{"i", "v"},
		Want: [][]string{
			{"1", "Diary"},
			{"2", "Ad"},
			{"3", "Diary"},
			{"3", "ぼやき"},
			{"4", "ぼやき"},
		},
		V: `SELECT "p"."id" AS "i", "t"."value" AS "v" FROM "post" AS "p" INNER JOIN ("posttag" AS "pt" INNER JOIN "tag" AS "t" ON "pt"."tag_id" = "t"."id") ON "p"."id" = "pt"."post_id" []`,
	},
	{
		Name: "Limit",
		B:    Select().Column(C("id", "i")).From(T("post")).Limit(2).OrderBy(C("id", "i"), true),
		Cols: []string{"i"},
		Want: [][]string{{"1"}, {"2"}},
		V:    `SELECT "id" AS "i" FROM "post" ORDER BY "i" ASC LIMIT ? [2]`,
	},
	{
		Name: "Limit + Offset",
		B:    Select().Column(C("id", "i")).From(T("post")).Limit(2).Offset(1).OrderBy(C("id"), true),
		Cols: []string{"i"},
		Want: [][]string{{"2"}, {"3"}},
		V:    `SELECT "id" AS "i" FROM "post" ORDER BY "id" ASC LIMIT ? OFFSET ? [2 1]`,
	},
	{
		Name: "GroupBy",
		B:    Select().Column(C("user_id", "uid"), CountAll().C("c")).From(T("post")).GroupBy(C("user_id")),
		Cols: []string{"uid", "c"},
		Want: [][]string{{"1", "2"}, {"2", "2"}},
		V:    `SELECT "user_id" AS "uid", COUNT(*) AS "c" FROM "post" GROUP BY "user_id" []`,
	},
	{
		Name: "GroupBy Multiple",
		B:    Select().Column(CountAll().C("c")).From(T("post")).GroupBy(C("user_id"), C("id")),
		Cols: []string{"c"},
		Want: [][]string{{"1"}, {"1"}, {"1"}, {"1"}},
		V:    `SELECT COUNT(*) AS "c" FROM "post" GROUP BY "user_id", "id" []`,
	},
	{
		Name: "Having",
		B:    Select().Column(C("user_id", "uid"), CountAll().C("c")).From(T("post")).GroupBy(C("user_id")).Having(Eq(C("user_id"), 1)),
		Cols: []string{"uid", "c"},
		Want: [][]string{{"1", "2"}},
		V:    `SELECT "user_id" AS "uid", COUNT(*) AS "c" FROM "post" GROUP BY "user_id" HAVING "user_id" = ? [1]`,
	},
	{
		Name: "OrderBy",
		B:    Select().Column(C("user_id", "u"), C("id", "i")).From(T("post")).OrderBy(C("user_id"), true).OrderBy(C("id"), false),
		Cols: []string{"u", "i"},
		Want: [][]string{{"1", "3"}, {"1", "1"}, {"2", "4"}, {"2", "2"}},
		V:    `SELECT "user_id" AS "u", "id" AS "i" FROM "post" ORDER BY "user_id" ASC, "id" DESC []`,
	},
	{
		Name: "SubQuery(Table)",
		B:    Select().From(Select().Column(C("id", "i")).From(T("user")).T("sq")),
		Cols: []string{"i"},
		Want: [][]string{{"1"}, {"2"}},
		V:    `SELECT * FROM (SELECT "id" AS "i" FROM "user") AS "sq" []`,
	},
	{
		Name: "SubQuery(Expression)",
		B:    Select().Column(C("id", "i")).From(T("post")).Where(In(C("user_id"), Select().Column(C("id", "i")).From(T("user")))),
		Cols: []string{"i"},
		Want: [][]string{{"1"}, {"2"}, {"3"}, {"4"}},
		V:    `SELECT "id" AS "i" FROM "post" WHERE "user_id" IN (SELECT "id" AS "i" FROM "user") []`,
	},
	{
		Name: "SubQuery(Column)",
		B: func() *ZSelectBuilder {
			user, post := T("user"), T("post")
			return Select().Column(
				Select().Column(user.C("name")).From(user).Where(
					Eq(post.C("user_id"), user.C("id")),
				).C("n"),
			).From(post)
		}(),
		Cols: []string{"n"},
		Want: [][]string{{"Shipon"}, {"Mr.TireMan"}, {"Shipon"}, {"Mr.TireMan"}},
		V:    `SELECT (SELECT "user"."name" FROM "user" WHERE "post"."user_id" = "user"."id") AS "n" FROM "post" []`,
	},
	{
		Name: "Eq",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Eq(C("age"), 15)),
		Cols: []string{"id"},
		Want: [][]string{{"1"}},
		V:    `SELECT "id" AS "id" FROM "user" WHERE "age" = ? [15]`,
	},
	{
		Name: "Neq",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Neq(C("age"), 15)),
		Cols: []string{"id"},
		Want: [][]string{{"2"}},
		V:    `SELECT "id" AS "id" FROM "user" WHERE "age" != ? [15]`,
	},
	{
		Name: "Lt",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Lt(C("age"), 44)),
		Cols: []string{"id"},
		Want: [][]string{{"1"}},
		V:    `SELECT "id" AS "id" FROM "user" WHERE "age" < ? [44]`,
	},
	{
		Name: "Lte",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Lte(C("age"), 44)),
		Cols: []string{"id"},
		Want: [][]string{{"1"}, {"2"}},
		V:    `SELECT "id" AS "id" FROM "user" WHERE "age" <= ? [44]`,
	},
	{
		Name: "Gt",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Gt(C("age"), 15)),
		Cols: []string{"id"},
		Want: [][]string{{"2"}},
		V:    `SELECT "id" AS "id" FROM "user" WHERE "age" > ? [15]`,
	},
	{
		Name: "Gte",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Gte(C("age"), 15)),
		Cols: []string{"id"},
		Want: [][]string{{"1"}, {"2"}},
		V:    `SELECT "id" AS "id" FROM "user" WHERE "age" >= ? [15]`,
	},
	{
		Name: "And",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(And(Eq(C("id"), 1), Eq(C("age"), 15))),
		Cols: []string{"id"},
		Want: [][]string{{"1"}},
		V:    `SELECT "id" AS "id" FROM "user" WHERE ("id" = ?)AND("age" = ?) [1 15]`,
	},
	{
		Name: "Or",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Or(Eq(C("id"), 2), Eq(C("age"), 15))),
		Cols: []string{"id"},
		Want: [][]string{{"1"}, {"2"}},
		V:    `SELECT "id" AS "id" FROM "user" WHERE ("id" = ?)OR("age" = ?) [2 15]`,
	},
	{
		Name: "And(empty)",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(And()),
		Want: [][]string{},
		V:    `SELECT "id" AS "id" FROM "user" WHERE ('empty' = 'AND') []`,
	},
	{
		Name: "Or(empty)",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Or()),
		Want: [][]string{},
		V:    `SELECT "id" AS "id" FROM "user" WHERE ('empty' = 'OR') []`,
	},
	{
		Name: "CountAll",
		B:    Select().Column(CountAll().C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"2"}},
		V:    `SELECT COUNT(*) AS "a" FROM "user" []`,
	},
	{
		Name: "Count",
		B:    Select().Column(Count(C("id")).C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"2"}},
		V:    `SELECT COUNT("id") AS "a" FROM "user" []`,
	},
	{
		Name: "Max",
		B:    Select().Column(Max(C("age")).C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"44"}},
		V:    `SELECT MAX("age") AS "a" FROM "user" []`,
	},
	{
		Name: "Min",
		B:    Select().Column(Min(C("age")).C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"15"}},
		V:    `SELECT MIN("age") AS "a" FROM "user" []`,
	},
	{
		Name: "Sum",
		B:    Select().Column(Sum(C("age")).C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"59"}},
		V:    `SELECT SUM("age") AS "a" FROM "user" []`,
	},
	{
		Name: "Simple CASE",
		B:    Select().Column(Case(C("age")).When(44, 10).When(15, 1).Else(0).C("r")).From(T("user")),
		Cols: []string{"r"},
		Want: [][]string{{"1"}, {"10"}},
		V:    `SELECT CASE "age" WHEN ? THEN ? WHEN ? THEN ? ELSE ? END AS "r" FROM "user" [44 10 15 1 0]`,
	},
	{
		Name: "Searched CASE",
		B:    Select().Column(Case().When(Eq(C("age"), 44), 10).When(Eq(C("age"), 15), 1).Else(0).C("r")).From(T("user")),
		Cols: []string{"r"},
		Want: [][]string{{"1"}, {"10"}},
		V:    `SELECT CASE WHEN "age" = ? THEN ? WHEN "age" = ? THEN ? ELSE ? END AS "r" FROM "user" [44 10 15 1 0]`,
	},
	{
		Name: "Simple CASE + Sum",
		// Unsafe is workaround for PostgreSQL "function sum(text) does not exist" error.
		// When all expressions are a placeholder, It seems PostgreSQL can not guess at the type.
		B:    Select().Column(Sum(Case(C("age")).When(44, 10).When(15, (1)).Else(Unsafe(0))).C("r")).From(T("user")),
		Cols: []string{"r"},
		Want: [][]string{{"11"}},
		V:    `SELECT SUM(CASE "age" WHEN ? THEN ? WHEN ? THEN ? ELSE 0 END) AS "r" FROM "user" [44 10 15 1]`,
	},
	{
		Name: "Searched CASE + Sum",
		B:    Select().Column(Sum(Case().When(Eq(C("age"), 44), 10).When(Eq(C("age"), 15), 1).Else(Unsafe(0))).C("r")).From(T("user")),
		Cols: []string{"r"},
		Want: [][]string{{"11"}},
		V:    `SELECT SUM(CASE WHEN "age" = ? THEN ? WHEN "age" = ? THEN ? ELSE 0 END) AS "r" FROM "user" [44 10 15 1]`,
	},
}

func TestSelect(t *testing.T) {
	for i, test := range selectTests {
		if r := fmt.Sprint(test.B); r != test.V {
			t.Errorf("tests[%d] %s: want %s got %s", i, test.Name, test.V, r)
		}
	}
}

func exec(t *testing.T, name string, db *sql.DB, d qutil.Dialect, sqls []string) {
	for i, sql := range sqls {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s %s[%d] %v", d, name, i, err)
		}
	}
}

func scan(rows *sql.Rows) (vals []string, err error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vars := make([]string, len(cols))
	ps := make([]interface{}, len(cols))
	for i := range vars {
		ps[i] = &vars[i]
	}
	if err = rows.Scan(ps...); err != nil {
		return nil, err
	}
	return vars, nil
}

func TestSelectOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)

			for i, test := range selectTests {
				func() {
					sql, args := test.B.SetDialect(d).ToSQL()
					rows, err := db.Query(sql, args...)
					if err != nil {
						t.Fatalf("%s test[%d] %s Error: %v\n%s", d, i, test.Name, err, sql)
					}
					defer rows.Close()

					if test.Cols != nil {
						// verify columns
						// https://www.sqlite.org/c3ref/column_name.html
						// In SQLite3, if there is no AS clause then the name of the column is unspecified.
						cols, err := rows.Columns()
						if err = rows.Err(); err != nil {
							t.Fatalf("%s test[%d] %s Error: %v", d, i, test.Name, err)
						}
						if fmt.Sprint(cols) != fmt.Sprint(test.Cols) {
							t.Errorf("%s test[%d] %s cols want %v got %v", d, i, test.Name, test.Cols, cols)
							return
						}
					}

					j := 0
					for ; rows.Next(); j++ {
						vars, err := scan(rows)
						if err != nil {
							t.Fatal(err)
						}
						if fmt.Sprint(vars) != fmt.Sprint(test.Want[j]) {
							t.Errorf("%s test[%d] %s vals[[j] want %v got %v", d, i, test.Name, test.Want[j], vars)
							return
						}
					}
					if err = rows.Err(); err != nil {
						t.Fatal(err)
					}
					if j != len(test.Want) {
						t.Errorf("%s test[%d] %s vals length want %d got %d", d, i, test.Name, len(test.Want), j)
						return
					}
				}()
			}

			// A return value of AVG can't be compared by string because
			// AVG returns decimal value which the number of decimal places is different on each platform.
			sql, args := Select().Column(Avg(C("id")).C("a")).From(T("post")).Where(Eq(C("user_id"), 1)).SetDialect(d).ToSQL()
			var v float64
			if err := db.QueryRow(sql, args...).Scan(&v); err != nil {
				t.Errorf("%s AVG func Error: %v", d, err)
			}
			if v != 2.0 {
				t.Errorf("%s AVG func want %d got %d", d, 2.0, v)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkSelectOverall(b *testing.B) {
	for i := 0; i < b.N; i++ {
		user, post := T("user"), T("post")
		Select().Column(
			user.C("id"),
			user.C("age"),
			user.C("name"),
			Select().Column(user.C("name")).From(user).Where(
				Eq(post.C("user_id"), user.C("id")),
			).C("n"),
		).From(post).Where(
			Eq(user.C("id"), []int{1, 2, 3, 4, 5}),
			Lt(user.C("age"), 18),
		).GroupBy(
			user.C("age"),
		).Having(
			Eq(user.C("id"), nil),
		).OrderBy(user.C("id"), true).Limit(10).Offset(20).ToSQL()
	}
}
