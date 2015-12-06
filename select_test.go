package q

import (
	"database/sql"
	"fmt"
	"testing"
)

var testModel = map[dialect]struct {
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

func TestRealDB(t *testing.T) {
	exec := func(name string, db *sql.DB, d dialect, sqls []string) {
		for i, sql := range sqls {
			if _, err := db.Exec(sql); err != nil {
				t.Fatalf("%s %s[%d] %v", d, name, i, err)
			}
		}
	}
	scan := func(rows *sql.Rows) (vals []string, err error) {
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

	tests := []struct {
		s    *SelectBuilder
		cols []string
		vals [][]string
	}{
		{
			s:    Select().From(T("user")),
			cols: []string{"id", "name", "age"},
			vals: [][]string{
				[]string{"1", "Shipon", "15"},
				[]string{"2", "Mr.TireMan", "44"},
			},
		},
		{
			s: func() *SelectBuilder {
				user, post := T("user", "u"), T("post", "p")
				return Select().From(post.InnerJoin(
					user,
					Eq(post.C("user_id"), user.C("id")),
				))
			}(),
			cols: []string{"id", "user_id", "title", "id", "name", "age"},
			vals: [][]string{
				[]string{"1", "1", "昨日見た夢の内容が凄い", "1", "Shipon", "15"},
				[]string{"2", "2", "氷の上で滑るタイヤの原因とは？", "2", "Mr.TireMan", "44"},
				[]string{"3", "1", "嘘じゃないんです", "1", "Shipon", "15"},
				[]string{"4", "2", "最近仕事が辛い", "2", "Mr.TireMan", "44"},
			},
		},
		{
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
				[]string{"1", "Diary"},
				[]string{"2", "Ad"},
				[]string{"3", "Diary"},
				[]string{"3", "ぼやき"},
				[]string{"4", "ぼやき"},
			},
		},
	}

	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d dialect) {
			defer exec("drops", db, d, testData.drops)
			exec("drops", db, d, testData.drops)
			exec("creates", db, d, testData.creates)
			exec("inserts", db, d, testData.inserts)

			for i, test := range tests {
				func() {
					sql, args := test.s.ToSQL(d)
					t.Log(d, " | ", sql, args)
					rows, err := db.Query(sql.String(), args...)
					if err != nil {
						t.Fatalf("%s test[%d] Error: %v", d, i, err)
					}
					defer rows.Close()

					// verify columns
					// https://www.sqlite.org/c3ref/column_name.html
					// In SQLite3, if there is no AS clause then the name of the column is unspecified.
					cols, err := rows.Columns()
					if err = rows.Err(); err != nil {
						t.Fatalf("%s test[%d] Error: %v", d, i, err)
					}
					if fmt.Sprint(cols) != fmt.Sprint(test.cols) {
						t.Errorf("%s test[%d].cols want %v got %v", d, i, test.cols, cols)
						return
					}
					j := 0
					for ; rows.Next(); j++ {
						vars, err := scan(rows)
						if err != nil {
							t.Fatal(err)
						}
						if fmt.Sprint(vars) != fmt.Sprint(test.vals[j]) {
							t.Errorf("%s test[%d].vals[[j] want %v got %v", d, i, test.vals[j], vars)
							return
						}
					}
					if err = rows.Err(); err != nil {
						t.Fatal(err)
					}
					if j != len(test.vals) {
						t.Errorf("%s test[%d].vals length want %d got %d", d, i, len(test.vals), j)
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
