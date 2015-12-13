package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var testModel = map[qutil.Dialect]struct {
	tester  Tester
	creates []string
	inserts []string
	drops   []string
}{
	MySQL: {
		tester: mySQLTest,
		creates: []string{
			`CREATE TABLE IF NOT EXISTS user(id INTEGER PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), age int) DEFAULT CHARSET=utf8mb4`,
			`CREATE TABLE IF NOT EXISTS post(id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER REFERENCES user(id) ON DELETE CASCADE, title TEXT NOT NULL) DEFAULT CHARSET=utf8mb4`,
			`CREATE TABLE IF NOT EXISTS tag(id INTEGER PRIMARY KEY AUTO_INCREMENT, value VARCHAR(255)) DEFAULT CHARSET=utf8mb4`,
			`CREATE TABLE IF NOT EXISTS posttag(post_id INTEGER NOT NULL REFERENCES post(id) ON DELETE CASCADE, tag_id INTEGER NOT NULL REFERENCES tag(id) ON DELETE CASCADE, PRIMARY KEY (post_id, tag_id)) DEFAULT CHARSET=utf8mb4`,
		},
		inserts: []string{
			`INSERT INTO user(id, name, age) VALUES (1, 'Shipon', 15)`,
			`INSERT INTO user(id, name, age) VALUES (2, 'Mr.TireMan', 44)`,
			`INSERT INTO post(id, user_id, title) VALUES (1, 1, '昨日見た夢の内容が凄い')`,
			`INSERT INTO post(id, user_id, title) VALUES (2, 2, '氷の上で滑るタイヤの原因とは？')`,
			`INSERT INTO post(id, user_id, title) VALUES (3, 1, '嘘じゃないんです')`,
			`INSERT INTO post(id, user_id, title) VALUES (4, 2, '最近仕事が辛い')`,
			`INSERT INTO tag(id, value) VALUES (1, 'Diary')`,
			`INSERT INTO tag(id, value) VALUES (2, 'Ad')`,
			`INSERT INTO tag(id, value) VALUES (3, 'ぼやき')`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (1, 1)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (2, 2)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (3, 1)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (3, 3)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (4, 3)`,
		},
		drops: []string{
			`DROP TABLE IF EXISTS posttag`,
			`DROP TABLE IF EXISTS post`,
			`DROP TABLE IF EXISTS user`,
			`DROP TABLE IF EXISTS tag`,
		},
	},
	PostgreSQL: {
		tester: postgreSQLTest,
		creates: []string{
			`CREATE TABLE IF NOT EXISTS "user"(id SERIAL PRIMARY KEY, name VARCHAR(255), age int)`,
			`CREATE TABLE IF NOT EXISTS post(id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES "user"(id) ON DELETE CASCADE, title TEXT NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS tag(id SERIAL PRIMARY KEY, value VARCHAR(255))`,
			`CREATE TABLE IF NOT EXISTS posttag(post_id INTEGER NOT NULL REFERENCES post(id) ON DELETE CASCADE, tag_id INTEGER NOT NULL REFERENCES tag(id) ON DELETE CASCADE, PRIMARY KEY (post_id, tag_id))`,
		},
		inserts: []string{
			`INSERT INTO "user"(id, name, age) VALUES (1, 'Shipon', 15)`,
			`INSERT INTO "user"(id, name, age) VALUES (2, 'Mr.TireMan', 44)`,
			`INSERT INTO post(id, user_id, title) VALUES (1, 1, '昨日見た夢の内容が凄い')`,
			`INSERT INTO post(id, user_id, title) VALUES (2, 2, '氷の上で滑るタイヤの原因とは？')`,
			`INSERT INTO post(id, user_id, title) VALUES (3, 1, '嘘じゃないんです')`,
			`INSERT INTO post(id, user_id, title) VALUES (4, 2, '最近仕事が辛い')`,
			`INSERT INTO tag(id, value) VALUES (1, 'Diary')`,
			`INSERT INTO tag(id, value) VALUES (2, 'Ad')`,
			`INSERT INTO tag(id, value) VALUES (3, 'ぼやき')`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (1, 1)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (2, 2)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (3, 1)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (3, 3)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (4, 3)`,
		},
		drops: []string{
			`DROP TABLE IF EXISTS posttag`,
			`DROP TABLE IF EXISTS post`,
			`DROP TABLE IF EXISTS "user"`,
			`DROP TABLE IF EXISTS tag`,
		},
	},
	SQLite: {
		tester: sqliteTest,
		creates: []string{
			`CREATE TABLE IF NOT EXISTS user(id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255), age int)`,
			`CREATE TABLE IF NOT EXISTS post(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER REFERENCES user(id) ON DELETE CASCADE, title TEXT NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS tag(id INTEGER PRIMARY KEY AUTOINCREMENT, value VARCHAR(255))`,
			`CREATE TABLE IF NOT EXISTS posttag(post_id INTEGER NOT NULL REFERENCES post(id) ON DELETE CASCADE, tag_id INTEGER NOT NULL REFERENCES tag(id) ON DELETE CASCADE, PRIMARY KEY (post_id, tag_id))`,
		},
		inserts: []string{
			`INSERT INTO user(id, name, age) VALUES (1, 'Shipon', 15)`,
			`INSERT INTO user(id, name, age) VALUES (2, 'Mr.TireMan', 44)`,
			`INSERT INTO post(id, user_id, title) VALUES (1, 1, '昨日見た夢の内容が凄い')`,
			`INSERT INTO post(id, user_id, title) VALUES (2, 2, '氷の上で滑るタイヤの原因とは？')`,
			`INSERT INTO post(id, user_id, title) VALUES (3, 1, '嘘じゃないんです')`,
			`INSERT INTO post(id, user_id, title) VALUES (4, 2, '最近仕事が辛い')`,
			`INSERT INTO tag(id, value) VALUES (1, 'Diary')`,
			`INSERT INTO tag(id, value) VALUES (2, 'Ad')`,
			`INSERT INTO tag(id, value) VALUES (3, 'ぼやき')`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (1, 1)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (2, 2)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (3, 1)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (3, 3)`,
			`INSERT INTO posttag(post_id, tag_id) VALUES (4, 3)`,
		},
		drops: []string{
			`DROP TABLE IF EXISTS posttag`,
			`DROP TABLE IF EXISTS post`,
			`DROP TABLE IF EXISTS user`,
			`DROP TABLE IF EXISTS tag`,
		},
	},
}

var selectTests = []struct {
	Name string
	B    *ZSelectBuilder
	Cols []string
	Want [][]string
}{
	{
		Name: "Simple Select",
		B:    Select().From(T("user")),
		Cols: []string{"id", "name", "age"},
		Want: [][]string{
			{"1", "Shipon", "15"},
			{"2", "Mr.TireMan", "44"},
		},
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
	},
	{
		Name: "GroupBy",
		B:    Select().Column(C("user_id", "uid"), CountAll().C("c")).From(T("post")).GroupBy(C("user_id")),
		Cols: []string{"uid", "c"},
		Want: [][]string{{"1", "2"}, {"2", "2"}},
	},
	{
		Name: "Having",
		B:    Select().Column(C("user_id", "uid"), CountAll().C("c")).From(T("post")).GroupBy(C("user_id")).Having(Eq(C("user_id"), 1)),
		Cols: []string{"uid", "c"},
		Want: [][]string{{"1", "2"}},
	},
	{
		Name: "OrderBy",
		B:    Select().Column(C("user_id", "u"), C("id", "i")).From(T("post")).OrderBy(C("user_id"), true).OrderBy(C("id"), false),
		Cols: []string{"u", "i"},
		Want: [][]string{{"1", "3"}, {"1", "1"}, {"2", "4"}, {"2", "2"}},
	},
	{
		Name: "SubQuery(Table)",
		B:    Select().From(Select().Column(C("id", "i")).From(T("user")).T("sq")),
		Cols: []string{"i"},
		Want: [][]string{{"1"}, {"2"}},
	},
	{
		Name: "SubQuery(Expression)",
		B:    Select().Column(C("id", "i")).From(T("post")).Where(In(C("user_id"), Select().Column(C("id", "i")).From(T("user")))),
		Cols: []string{"i"},
		Want: [][]string{{"1"}, {"2"}, {"3"}, {"4"}},
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
	},
	{
		Name: "Eq",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Eq(C("age"), 15)),
		Cols: []string{"id"},
		Want: [][]string{{"1"}},
	},
	{
		Name: "Neq",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Neq(C("age"), 15)),
		Cols: []string{"id"},
		Want: [][]string{{"2"}},
	},
	{
		Name: "Lt",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Lt(C("age"), 44)),
		Cols: []string{"id"},
		Want: [][]string{{"1"}},
	},
	{
		Name: "Lte",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Lte(C("age"), 44)),
		Cols: []string{"id"},
		Want: [][]string{{"1"}, {"2"}},
	},
	{
		Name: "Gt",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Gt(C("age"), 15)),
		Cols: []string{"id"},
		Want: [][]string{{"2"}},
	},
	{
		Name: "Gte",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Gte(C("age"), 15)),
		Cols: []string{"id"},
		Want: [][]string{{"1"}, {"2"}},
	},
	{
		Name: "And",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(And(Eq(C("id"), 1), Eq(C("age"), 15))),
		Cols: []string{"id"},
		Want: [][]string{{"1"}},
	},
	{
		Name: "Or",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Or(Eq(C("id"), 2), Eq(C("age"), 15))),
		Cols: []string{"id"},
		Want: [][]string{{"1"}, {"2"}},
	},
	{
		Name: "And(empty)",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(And()),
		Want: [][]string{},
	},
	{
		Name: "Or(empty)",
		B:    Select().Column(C("id", "id")).From(T("user")).Where(Or()),
		Want: [][]string{},
	},
	{
		Name: "CountAll",
		B:    Select().Column(CountAll().C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"2"}},
	},
	{
		Name: "Count",
		B:    Select().Column(Count(C("id")).C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"2"}},
	},
	{
		Name: "Max",
		B:    Select().Column(Max(C("age")).C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"44"}},
	},
	{
		Name: "Min",
		B:    Select().Column(Min(C("age")).C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"15"}},
	},
	{
		Name: "Sum",
		B:    Select().Column(Sum(C("age")).C("a")).From(T("user")),
		Cols: []string{"a"},
		Want: [][]string{{"59"}},
	},
	{
		Name: "Simple CASE",
		B:    Select().Column(Case(C("age")).When(44, 10).When(15, 1).Else(0).C("r")).From(T("user")),
		Cols: []string{"r"},
		Want: [][]string{{"1"}, {"10"}},
	},
	{
		Name: "Searched CASE",
		B:    Select().Column(Case().When(Eq(C("age"), 44), 10).When(Eq(C("age"), 15), 1).Else(0).C("r")).From(T("user")),
		Cols: []string{"r"},
		Want: [][]string{{"1"}, {"10"}},
	},
	{
		Name: "Simple CASE + Sum",
		// Unsafe is workaround for PostgreSQL "function sum(text) does not exist" error.
		// When all expressions are a placeholder, It seems PostgreSQL can not guess at the type.
		B:    Select().Column(Sum(Case(C("age")).When(44, 10).When(15, (1)).Else(Unsafe(0))).C("r")).From(T("user")),
		Cols: []string{"r"},
		Want: [][]string{{"11"}},
	},
	{
		Name: "Searched CASE + Sum",
		B:    Select().Column(Sum(Case().When(Eq(C("age"), 44), 10).When(Eq(C("age"), 15), 1).Else(Unsafe(0))).C("r")).From(T("user")),
		Cols: []string{"r"},
		Want: [][]string{{"11"}},
	},
}

func TestSelect(t *testing.T) {
	tests := []struct {
		Name string
		B    *ZSelectBuilder
		Want string
	}{
		{
			Name: "empty",
			B:    Select(),
			Want: `SELECT * []`,
		},
		{
			Name: "empty+beginning",
			B:    Select("SELECT SQL_NO_CACHE"),
			Want: `SELECT SQL_NO_CACHE * []`,
		},
		{
			Name: "Limit Offset",
			B:    Select().From(T("test")).Limit(20).Offset(10),
			Want: `SELECT * FROM "test" LIMIT ? OFFSET ? [20 10]`,
		},
		{
			Name: "GroupBy",
			B:    Select().From(T("test")).GroupBy(C("a"), C("b")),
			Want: `SELECT * FROM "test" GROUP BY "a", "b" []`,
		},
		{
			Name: "OrderBy",
			B:    Select().From(T("test")).OrderBy(C("a"), true).OrderBy(C("b"), false),
			Want: `SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC []`,
		},
		{
			Name: "SubQuery(Table)",
			B:    Select().From(Select().Column(C("id", "i")).From(T("test")).T()),
			Want: `SELECT * FROM (SELECT "id" AS "i" FROM "test") []`,
		},
		{
			Name: "SubQuery(Table alias)",
			B:    Select().From(Select().Column(C("id", "i")).From(T("test")).T("sq")),
			Want: `SELECT * FROM (SELECT "id" AS "i" FROM "test") AS "sq" []`,
		},
		{
			Name: "SubQuery(Expression)",
			B:    Select().From(T("t2")).Where(In(C("id"), Select().Column(C("id", "i")).From(T("test")))),
			Want: `SELECT * FROM "t2" WHERE "id" IN (SELECT "id" AS "i" FROM "test") []`,
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
			Want: `SELECT (SELECT "user"."name" FROM "user" WHERE "post"."user_id" = "user"."id") AS "n" FROM "post" []`,
		},
	}
	for i, test := range tests {
		if r := fmt.Sprint(test.B); r != test.Want {
			t.Errorf("tests[%d] %s: want %q got %q", i, test.Name, test.Want, r)
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
