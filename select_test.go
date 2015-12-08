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
			`CREATE TABLE IF NOT EXISTS post(id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER REFERENCES user(id), title TEXT NOT NULL) DEFAULT CHARSET=utf8mb4`,
			`CREATE TABLE IF NOT EXISTS tag(id INTEGER PRIMARY KEY AUTO_INCREMENT, value VARCHAR(255)) DEFAULT CHARSET=utf8mb4`,
			`CREATE TABLE IF NOT EXISTS posttag(post_id INTEGER NOT NULL REFERENCES post(id), tag_id INTEGER NOT NULL REFERENCES tag(id), PRIMARY KEY (post_id, tag_id)) DEFAULT CHARSET=utf8mb4`,
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
			`CREATE TABLE IF NOT EXISTS post(id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES "user"(id), title TEXT NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS tag(id SERIAL PRIMARY KEY, value VARCHAR(255))`,
			`CREATE TABLE IF NOT EXISTS posttag(post_id INTEGER NOT NULL REFERENCES post(id), tag_id INTEGER NOT NULL REFERENCES tag(id), PRIMARY KEY (post_id, tag_id))`,
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
			`CREATE TABLE IF NOT EXISTS post(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER REFERENCES user(id), title TEXT NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS tag(id INTEGER PRIMARY KEY AUTOINCREMENT, value VARCHAR(255))`,
			`CREATE TABLE IF NOT EXISTS posttag(post_id INTEGER NOT NULL REFERENCES post(id), tag_id INTEGER NOT NULL REFERENCES tag(id), PRIMARY KEY (post_id, tag_id))`,
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

func TestRealDB(t *testing.T) {

	tests := []struct {
		name string
		s    *SelectBuilder
		cols []string
		vals [][]string
	}{
		{
			name: "Simple Select",
			s:    Select().From(T("user")),
			cols: []string{"id", "name", "age"},
			vals: [][]string{
				{"1", "Shipon", "15"},
				{"2", "Mr.TireMan", "44"},
			},
		},
		{
			name: "Single Join",
			s: func() *SelectBuilder {
				user, post := T("user", "u"), T("post", "p")
				return Select().From(post.InnerJoin(
					user,
					Eq(post.C("user_id"), user.C("id")),
				))
			}(),
			cols: []string{"id", "user_id", "title", "id", "name", "age"},
			vals: [][]string{
				{"1", "1", "昨日見た夢の内容が凄い", "1", "Shipon", "15"},
				{"2", "2", "氷の上で滑るタイヤの原因とは？", "2", "Mr.TireMan", "44"},
				{"3", "1", "嘘じゃないんです", "1", "Shipon", "15"},
				{"4", "2", "最近仕事が辛い", "2", "Mr.TireMan", "44"},
			},
		},
		{
			name: "Multiple Join",
			s: func() *SelectBuilder {
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
			cols: []string{"i", "v"},
			vals: [][]string{
				{"1", "Diary"},
				{"2", "Ad"},
				{"3", "Diary"},
				{"3", "ぼやき"},
				{"4", "ぼやき"},
			},
		},
		{
			name: "Eq",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(Eq(C("age"), 15)),
			cols: []string{"id"},
			vals: [][]string{{"1"}},
		},
		{
			name: "Neq",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(Neq(C("age"), 15)),
			cols: []string{"id"},
			vals: [][]string{{"2"}},
		},
		{
			name: "Lt",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(Lt(C("age"), 44)),
			cols: []string{"id"},
			vals: [][]string{{"1"}},
		},
		{
			name: "Lte",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(Lte(C("age"), 44)),
			cols: []string{"id"},
			vals: [][]string{{"1"}, {"2"}},
		},
		{
			name: "Gt",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(Gt(C("age"), 15)),
			cols: []string{"id"},
			vals: [][]string{{"2"}},
		},
		{
			name: "Gte",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(Gte(C("age"), 15)),
			cols: []string{"id"},
			vals: [][]string{{"1"}, {"2"}},
		},
		{
			name: "And",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(And(Eq(C("id"), 1), Eq(C("age"), 15))),
			cols: []string{"id"},
			vals: [][]string{{"1"}},
		},
		{
			name: "Or",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(Or(Eq(C("id"), 2), Eq(C("age"), 15))),
			cols: []string{"id"},
			vals: [][]string{{"1"}, {"2"}},
		},
		{
			name: "And(empty)",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(And()),
			vals: [][]string{},
		},
		{
			name: "Or(empty)",
			s:    Select().Column(C("id", "id")).From(T("user")).Where(Or()),
			vals: [][]string{},
		},
		{
			name: "CountAll",
			s:    Select().Column(C(CountAll(), "a")).From(T("user")),
			cols: []string{"a"},
			vals: [][]string{{"2"}},
		},
		{
			name: "Count",
			s:    Select().Column(C(Count(C("id")), "a")).From(T("user")),
			cols: []string{"a"},
			vals: [][]string{{"2"}},
		},
		{
			name: "Avg",
			s:    Select().Column(C(Max(C("id")), "a")).From(T("post")).Where(Eq(C("user_id"), 1)),
			cols: []string{"a"},
			vals: [][]string{{"3"}},
		},
		{
			name: "Max",
			s:    Select().Column(C(Max(C("age")), "a")).From(T("user")),
			cols: []string{"a"},
			vals: [][]string{{"44"}},
		},
		{
			name: "Min",
			s:    Select().Column(C(Min(C("age")), "a")).From(T("user")),
			cols: []string{"a"},
			vals: [][]string{{"15"}},
		},
		{
			name: "Sum",
			s:    Select().Column(C(Sum(C("age")), "a")).From(T("user")),
			cols: []string{"a"},
			vals: [][]string{{"59"}},
		},
		{
			name: "Simple CASE",
			s:    Select().Column(C(Case(C("age")).When(44, 10).When(15, 1).Else(0), "r")).From(T("user")),
			cols: []string{"r"},
			vals: [][]string{{"1"}, {"10"}},
		},
		{
			name: "Searched CASE",
			s:    Select().Column(C(Case().When(Eq(C("age"), 44), 10).When(Eq(C("age"), 15), 1).Else(0), "r")).From(T("user")),
			cols: []string{"r"},
			vals: [][]string{{"1"}, {"10"}},
		},
		{
			name: "Simple CASE + Sum",
			// Unsafe is workaround for PostgreSQL "function sum(text) does not exist" error.
			// When all expressions are a placeholder, It seems PostgreSQL can not guess at the type.
			s:    Select().Column(C(Sum(Case(C("age")).When(44, 10).When(15, (1)).Else(Unsafe(0))), "r")).From(T("user")),
			cols: []string{"r"},
			vals: [][]string{{"11"}},
		},
		{
			name: "Searched CASE + Sum",
			s:    Select().Column(C(Sum(Case().When(Eq(C("age"), 44), 10).When(Eq(C("age"), 15), 1).Else(Unsafe(0))), "r")).From(T("user")),
			cols: []string{"r"},
			vals: [][]string{{"11"}},
		},
	}

	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)

			for i, test := range tests {
				func() {
					sql, args := test.s.SetDialect(d).SQL()
					rows, err := db.Query(sql.String(), args...)
					if err != nil {
						t.Fatalf("%s test[%d] %s Error: %v\n%s", d, i, test.name, err, sql)
					}
					defer rows.Close()

					if test.cols != nil {
						// verify columns
						// https://www.sqlite.org/c3ref/column_name.html
						// In SQLite3, if there is no AS clause then the name of the column is unspecified.
						cols, err := rows.Columns()
						if err = rows.Err(); err != nil {
							t.Fatalf("%s test[%d] %s Error: %v", d, i, test.name, err)
						}
						if fmt.Sprint(cols) != fmt.Sprint(test.cols) {
							t.Errorf("%s test[%d] %s cols want %v got %v", d, i, test.name, test.cols, cols)
							return
						}
					}

					j := 0
					for ; rows.Next(); j++ {
						vars, err := scan(rows)
						if err != nil {
							t.Fatal(err)
						}
						if fmt.Sprint(vars) != fmt.Sprint(test.vals[j]) {
							t.Errorf("%s test[%d] %s vals[[j] want %v got %v", d, i, test.name, test.vals[j], vars)
							return
						}
					}
					if err = rows.Err(); err != nil {
						t.Fatal(err)
					}
					if j != len(test.vals) {
						t.Errorf("%s test[%d] %s vals length want %d got %d", d, i, test.name, len(test.vals), j)
						return
					}
				}()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
