package q

import "github.com/oov/q/qutil"

var testModel = map[qutil.Dialect]struct {
	tester  Tester
	creates []string
	inserts []string
	drops   []string
}{
	MySQL: {
		tester: mySQLTest,
		creates: []string{
			`CREATE TABLE IF NOT EXISTS user(id INTEGER PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), age INT) DEFAULT CHARSET=utf8mb4`,
			`CREATE TABLE IF NOT EXISTS post(id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER REFERENCES user(id) ON DELETE CASCADE, title TEXT NOT NULL, at DATETIME NOT NULL) DEFAULT CHARSET=utf8mb4`,
			`CREATE TABLE IF NOT EXISTS tag(id INTEGER PRIMARY KEY AUTO_INCREMENT, value VARCHAR(255)) DEFAULT CHARSET=utf8mb4`,
			`CREATE TABLE IF NOT EXISTS posttag(post_id INTEGER NOT NULL REFERENCES post(id) ON DELETE CASCADE, tag_id INTEGER NOT NULL REFERENCES tag(id) ON DELETE CASCADE, PRIMARY KEY (post_id, tag_id)) DEFAULT CHARSET=utf8mb4`,
		},
		inserts: []string{
			`INSERT INTO user(id, name, age) VALUES (1, 'Shipon', 15)`,
			`INSERT INTO user(id, name, age) VALUES (2, 'Mr.TireMan', 44)`,
			`INSERT INTO post(id, user_id, title, at) VALUES (1, 1, '昨日見た夢の内容が凄い', '2015-12-12 20:19:18')`,
			`INSERT INTO post(id, user_id, title, at) VALUES (2, 2, '氷の上で滑るタイヤの原因とは？', '2015-12-12 21:19:18')`,
			`INSERT INTO post(id, user_id, title, at) VALUES (3, 1, '嘘じゃないんです', '2015-12-13 20:19:18')`,
			`INSERT INTO post(id, user_id, title, at) VALUES (4, 2, '最近仕事が辛い', '2015-12-13 21:19:18')`,
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
			`CREATE TABLE IF NOT EXISTS "user"(id SERIAL PRIMARY KEY, name VARCHAR(255), age INT)`,
			`CREATE TABLE IF NOT EXISTS post(id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES "user"(id) ON DELETE CASCADE, title TEXT NOT NULL, at TIMESTAMP NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS tag(id SERIAL PRIMARY KEY, value VARCHAR(255))`,
			`CREATE TABLE IF NOT EXISTS posttag(post_id INTEGER NOT NULL REFERENCES post(id) ON DELETE CASCADE, tag_id INTEGER NOT NULL REFERENCES tag(id) ON DELETE CASCADE, PRIMARY KEY (post_id, tag_id))`,
		},
		inserts: []string{
			`INSERT INTO "user"(id, name, age) VALUES (1, 'Shipon', 15)`,
			`INSERT INTO "user"(id, name, age) VALUES (2, 'Mr.TireMan', 44)`,
			`INSERT INTO post(id, user_id, title, at) VALUES (1, 1, '昨日見た夢の内容が凄い', '2015-12-12 20:19:18')`,
			`INSERT INTO post(id, user_id, title, at) VALUES (2, 2, '氷の上で滑るタイヤの原因とは？', '2015-12-12 21:19:18')`,
			`INSERT INTO post(id, user_id, title, at) VALUES (3, 1, '嘘じゃないんです', '2015-12-13 20:19:18')`,
			`INSERT INTO post(id, user_id, title, at) VALUES (4, 2, '最近仕事が辛い', '2015-12-13 21:19:18')`,
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
			`CREATE TABLE IF NOT EXISTS user(id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255), age INT)`,
			`CREATE TABLE IF NOT EXISTS post(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER REFERENCES user(id) ON DELETE CASCADE, title TEXT NOT NULL, at TIMESTAMP NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS tag(id INTEGER PRIMARY KEY AUTOINCREMENT, value VARCHAR(255))`,
			`CREATE TABLE IF NOT EXISTS posttag(post_id INTEGER NOT NULL REFERENCES post(id) ON DELETE CASCADE, tag_id INTEGER NOT NULL REFERENCES tag(id) ON DELETE CASCADE, PRIMARY KEY (post_id, tag_id))`,
		},
		inserts: []string{
			`INSERT INTO user(id, name, age) VALUES (1, 'Shipon', 15)`,
			`INSERT INTO user(id, name, age) VALUES (2, 'Mr.TireMan', 44)`,
			`INSERT INTO post(id, user_id, title, at) VALUES (1, 1, '昨日見た夢の内容が凄い', '2015-12-12 20:19:18')`,
			`INSERT INTO post(id, user_id, title, at) VALUES (2, 2, '氷の上で滑るタイヤの原因とは？', '2015-12-12 21:19:18')`,
			`INSERT INTO post(id, user_id, title, at) VALUES (3, 1, '嘘じゃないんです', '2015-12-13 20:19:18')`,
			`INSERT INTO post(id, user_id, title, at) VALUES (4, 2, '最近仕事が辛い', '2015-12-13 21:19:18')`,
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
