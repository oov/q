package q_test

import (
	"fmt"

	"github.com/oov/q"
)

func Example() {
	user := q.T("user")
	sel := q.Select().From(
		user,
	).Column(
		user.C("id"),
		user.C("name"),
	).Where(
		q.Eq(user.C("age"), 18),
	)
	// You can use sel by performing the following steps.
	// sql, args := sel.SQL()
	// rows, err := db.Query(sql.String(), args...)
	// ...
	fmt.Println(sel)
	// Output:
	// SELECT "user"."id", "user"."name" FROM "user" WHERE "user"."age" = ? [18]
}

func Example_complicated() {
	user := q.T("user", "u")
	age := user.C("age")
	sel := q.Select().From(user).Column(
		q.Sum(
			q.Case().When(
				q.And(
					q.Gte(age, 13),
					q.Lte(age, 19),
				),
				1,
			).Else(0),
		).C("teen"),
		q.Sum(q.Case().When(q.Gte(age, 20), 1).Else(0)).C("adult"),
	)
	fmt.Println(sel)
	// Output:
	// SELECT SUM(CASE WHEN ("u"."age" >= ?)AND("u"."age" <= ?) THEN ? ELSE ? END) AS "teen", SUM(CASE WHEN "u"."age" >= ? THEN ? ELSE ? END) AS "adult" FROM "user" AS "u" [13 19 1 0 20 1 0]
}

func Example_unsafe() {
	user := q.T("user", "u")
	age := user.C("age")
	sel := q.Select().From(user).Column(
		q.Unsafe(`SUM(CASE WHEN (`, age, ` >= 13)AND(`, age, ` <= 19) THEN 1 ELSE 0 END)`).C("teen"),
		q.Unsafe(`SUM(CASE WHEN `, age, ` >= 20 THEN 1 ELSE 0 END)`).C("adult"),
	)
	fmt.Println(sel)

	lastName := user.C("last_name")
	maliciousUserInput := "' OR '' = '"
	sel = q.Select().From(user).Where(
		// Safe
		q.Eq(lastName, maliciousUserInput),
		// Safe
		q.Unsafe(lastName, ` = `, q.V(maliciousUserInput)),
		// Unsafe - DO NOT THIS
		q.Unsafe(lastName, ` = '`, maliciousUserInput, `'`),
	)
	fmt.Println(sel)
	// Output:
	// SELECT SUM(CASE WHEN ("u"."age" >= 13)AND("u"."age" <= 19) THEN 1 ELSE 0 END) AS "teen", SUM(CASE WHEN "u"."age" >= 20 THEN 1 ELSE 0 END) AS "adult" FROM "user" AS "u" []
	// SELECT * FROM "user" AS "u" WHERE ("u"."last_name" = ?)AND("u"."last_name" = ?)AND("u"."last_name" = '' OR '' = '') [' OR '' = ' ' OR '' = ']
}

func ExampleColumn() {
	fmt.Println("q.C(name):               ", q.C("id"))
	fmt.Println("q.C(name, alias):        ", q.C("age", "ag"))
	fmt.Println("Table.C(name):           ", q.T("user").C("age"))
	fmt.Println("Table.C(name, alias):    ", q.T("user").C("age", "ag"))
	fmt.Println("Expression.C():          ", q.CountAll().C())
	fmt.Println("Expression.C(alias):     ", q.CountAll().C("cnt"))

	country := q.T("country")
	sel := q.Select().Column(
		country.C("name"),
	).From(
		country,
	).Where(
		q.Eq(country.C("id"), 100),
	)
	fmt.Println("*ZSelectBuilder.C():     ", sel.C())
	fmt.Println("*ZSelectBuilder.C(alias):", sel.C("cname"))
	// Output:
	// q.C(name):                "id" []
	// q.C(name, alias):         "age" AS "ag" []
	// Table.C(name):            "user"."age" []
	// Table.C(name, alias):     "user"."age" AS "ag" []
	// Expression.C():           COUNT(*) []
	// Expression.C(alias):      COUNT(*) AS "cnt" []
	// *ZSelectBuilder.C():      (SELECT "country"."name" FROM "country" WHERE "country"."id" = ?) [100]
	// *ZSelectBuilder.C(alias): (SELECT "country"."name" FROM "country" WHERE "country"."id" = ?) AS "cname" [100]
}

// This is an example of how to use C.
// Actually, using Table.C is more useful in many cases than using C directly because Table.C adds the table name before column name.
func ExampleC() {
	fmt.Println("name:        ", q.C("id"))
	fmt.Println("name + alias:", q.C("age", "ag"))
	// Output:
	// name:         "id" []
	// name + alias: "age" AS "ag" []
}

// This is an example of how to use Table.InnerJoin.
func ExampleTable() {
	user := q.T("user", "usr")
	post := q.T("post", "pst")
	// user.id -> post.user_id
	user.InnerJoin(post, q.Eq(user.C("id"), post.C("user_id")))
	fmt.Println("Short:", user)

	postTag := q.T("posttag", "rel")
	tag := q.T("tag", "tg")
	// post.id -> posttag.post_id
	post.InnerJoin(postTag, q.Eq(post.C("id"), postTag.C("post_id")))
	// posttag.tag_id -> tag.id
	postTag.InnerJoin(tag, q.Eq(postTag.C("tag_id"), tag.C("id")))
	fmt.Println("Long: ", user)
	// Output:
	// Short: "user" AS "usr" INNER JOIN "post" AS "pst" ON "usr"."id" = "pst"."user_id" []
	// Long:  "user" AS "usr" INNER JOIN ("post" AS "pst" INNER JOIN ("posttag" AS "rel" INNER JOIN "tag" AS "tg" ON "rel"."tag_id" = "tg"."id") ON "pst"."id" = "rel"."post_id") ON "usr"."id" = "pst"."user_id" []
}

// This is an example of how to use T.
func ExampleT() {
	fmt.Println("name:        ", q.T("user"))
	fmt.Println("name + alias:", q.T("user", "usr"))
	// Output:
	// name:         "user" []
	// name + alias: "user" AS "usr" []
}

// This is an example of how to use Expression.
func ExampleExpression() {
	id := q.C("id")
	fmt.Println("Eq(id, 100):     ", q.Eq(id, 100))
	fmt.Println("Eq(id, nil):     ", q.Eq(id, nil))
	fmt.Println("Eq(id, []int):   ", q.Eq(id, []int{1, 2}))
	fmt.Println("In(id, []int):   ", q.In(id, []int{1, 2}))
	fmt.Println("Neq(id, 100):    ", q.Neq(id, 100))
	fmt.Println("Neq(id, nil):    ", q.Neq(id, nil))
	fmt.Println("Neq(id, []int):  ", q.Neq(id, []int{1, 2}))
	fmt.Println("NotIn(id, []int):", q.NotIn(id, []int{1, 2}))
	fmt.Println("Gt(id, 100):     ", q.Gt(id, 100))
	fmt.Println("Gte(id, 100):    ", q.Gte(id, 100))
	fmt.Println("Lt(id, 100):     ", q.Lt(id, 100))
	fmt.Println("Lte(id, 100):    ", q.Lte(id, 100))
	// Output:
	// Eq(id, 100):      "id" = ? [100]
	// Eq(id, nil):      "id" IS NULL []
	// Eq(id, []int):    "id" IN (?,?) [1 2]
	// In(id, []int):    "id" IN (?,?) [1 2]
	// Neq(id, 100):     "id" != ? [100]
	// Neq(id, nil):     "id" IS NOT NULL []
	// Neq(id, []int):   "id" NOT IN (?,?) [1 2]
	// NotIn(id, []int): "id" NOT IN (?,?) [1 2]
	// Gt(id, 100):      "id" > ? [100]
	// Gte(id, 100):     "id" >= ? [100]
	// Lt(id, 100):      "id" < ? [100]
	// Lte(id, 100):     "id" <= ? [100]
}

// This is an example of how to use Expressions.
func ExampleExpressions() {
	user := q.T("user")
	fmt.Println("And:       ", q.And(
		q.Eq(user.C("age"), 15),
		q.Eq(user.C("gender"), "female"),
		q.Eq(user.C("nickname"), "Shipon"),
	))
	fmt.Println("Or:        ", q.Or(
		q.Neq(user.C("name"), nil),
		q.Neq(user.C("nickname"), nil),
	))
	fmt.Println("And(empty):", q.And())
	fmt.Println("Or(empty): ", q.Or())
	// Output:
	// And:        ("user"."age" = ?)AND("user"."gender" = ?)AND("user"."nickname" = ?) [15 female Shipon]
	// Or:         ("user"."name" IS NOT NULL)OR("user"."nickname" IS NOT NULL) []
	// And(empty): ('empty' = 'AND') []
	// Or(empty):  ('empty' = 'OR') []
}

// This is an example of how to use Unsafe, V and InV.
func ExampleUnsafe() {
	user := q.T("user")
	id, name, age := user.C("id"), user.C("name"), user.C("age")
	expr := q.Unsafe(
		"(", id, " % 2 = 1)AND",
		"(", name, " != ", q.V("yourname"), ")AND",
		"(", age, " IN ", q.InV([]int{16, 17, 18}), ")",
	)
	fmt.Println(expr)
	// Output:
	// ("user"."id" % 2 = 1)AND("user"."name" != ?)AND("user"."age" IN (?,?,?)) [yourname 16 17 18]
}

// This is an example of how to use the beginning argument.
func ExampleSelect_beginning() {
	user := q.T("user")
	fmt.Println("Default:     ", q.Select().From(user))
	fmt.Println("SQL_NO_CACHE:", q.Select("SELECT SQL_NO_CACHE").From(user))
	fmt.Println("EXPLAIN:     ", q.Select("EXPLAIN SELECT").From(user))
	// Output:
	// Default:      SELECT * FROM "user" []
	// SQL_NO_CACHE: SELECT SQL_NO_CACHE * FROM "user" []
	// EXPLAIN:      EXPLAIN SELECT * FROM "user" []
}

// This is an example of how to use Select.
func ExampleSelect() {
	post, user := q.T("post"), q.T("user")
	sel := q.Select().From(
		post.InnerJoin(
			user,
			q.Eq(post.C("user_id"), user.C("id")),
		),
	).Column(
		user.C("name"),
		post.C("message"),
	).Where(
		q.Eq(post.C("id"), 100),
	)
	// You can also use `q.DefaultDialect = q.MySQL` instead of SetDialect.
	fmt.Println(sel.SetDialect(q.MySQL).ToSQL())
	// Output:
	// SELECT `user`.`name`, `post`.`message` FROM `post` INNER JOIN `user` ON `post`.`user_id` = `user`.`id` WHERE `post`.`id` = ? [100]
}

// This is an example of how to use ZSelectBuilder.Column.
func ExampleZSelectBuilder_Column() {
	user := q.T("user")
	fmt.Println("Default:  ", q.Select().From(user))
	fmt.Println("Append:   ", q.Select().Column(user.C("id")).From(user))
	fmt.Println("Aggregate:", q.Select().Column(q.CountAll().C("count")).From(user))
	// Output:
	// Default:   SELECT * FROM "user" []
	// Append:    SELECT "user"."id" FROM "user" []
	// Aggregate: SELECT COUNT(*) AS "count" FROM "user" []
}

// This is an example of how to use ZSelectBuilder.From.
func ExampleZSelectBuilder_From() {
	user := q.T("user")
	fmt.Println("Simple: ", q.Select().From(user))
	post := q.T("post")
	fmt.Println("Complex:", q.Select().From(user, post).Where(
		q.Eq(user.C("id"), post.C("user_id")),
	))
	fmt.Println("Builder:", q.Select().From(q.Select().From(q.T("post")).T("p")))
	// Output:
	// Simple:  SELECT * FROM "user" []
	// Complex: SELECT * FROM "user", "post" WHERE "user"."id" = "post"."user_id" []
	// Builder: SELECT * FROM (SELECT * FROM "post") AS "p" []
}

// This is an example of how to use ZSelectBuilder.SQL.
func ExampleZSelectBuilder_SQL() {
	fmt.Println(q.Select().From(q.T("user")).Where(q.Lte(q.C("age"), 18)).ToSQL())
	// Output:
	// SELECT * FROM "user" WHERE "age" <= ? [18]
}

// This is an example of how to use ZSelectBuilder.Where.
func ExampleZSelectBuilder_Where() {
	user := q.T("user")
	fmt.Println("Simple: ", q.Select().From(user).Where(q.Neq(user.C("id"), nil)))
	post := q.T("post")
	fmt.Println("Complex:", q.Select().From(user, post).Where(
		q.Neq(user.C("id"), nil),
		q.Gt(user.C("id"), 100),
	))
	// Output:
	// Simple:  SELECT * FROM "user" WHERE "user"."id" IS NOT NULL []
	// Complex: SELECT * FROM "user", "post" WHERE ("user"."id" IS NOT NULL)AND("user"."id" > ?) [100]
}

// This is an example of how to use ZSelectBuilder.Limit.
func ExampleZSelectBuilder_Limit() {
	user := q.T("user")
	fmt.Println("int:     ", q.Select().From(user).Limit(10))
	fmt.Println("q.Unsafe:", q.Select().From(user).Limit(q.Unsafe(10, "*", 20)))
	// Output:
	// int:      SELECT * FROM "user" LIMIT ? [10]
	// q.Unsafe: SELECT * FROM "user" LIMIT 10*20 []
}

// This is an example of how to use ZSelectBuilder.Offset.
func ExampleZSelectBuilder_Offset() {
	user := q.T("user")
	fmt.Println("int:     ", q.Select().From(user).Limit(10).Offset(10))
	fmt.Println("q.Unsafe:", q.Select().From(user).Limit(10).Offset(q.Unsafe(10, "*", 20)))
	// Output:
	// int:      SELECT * FROM "user" LIMIT ? OFFSET ? [10 10]
	// q.Unsafe: SELECT * FROM "user" LIMIT ? OFFSET 10*20 [10]
}

// This is an example of how to use ZSelectBuilder.OrderBy.
func ExampleZSelectBuilder_OrderBy() {
	user := q.T("user")
	fmt.Println(
		"Single order:  ",
		q.Select().From(user).OrderBy(user.C("age"), true),
	)
	fmt.Println(
		"Multiple order:",
		q.Select().From(user).OrderBy(user.C("age"), true).OrderBy(
			q.CharLength(user.C("name")), false),
	)
	// Output:
	// Single order:   SELECT * FROM "user" ORDER BY "user"."age" ASC []
	// Multiple order: SELECT * FROM "user" ORDER BY "user"."age" ASC, CHAR_LENGTH("user"."name") DESC []
}

// This is an example of how to use ZSelectBuilder.GroupBy.
func ExampleZSelectBuilder_GroupBy() {
	user := q.T("user")
	fmt.Println(
		q.Select().Column(q.CountAll().C("count")).From(user).GroupBy(user.C("age")),
	)
	// Output:
	// SELECT COUNT(*) AS "count" FROM "user" GROUP BY "user"."age" []
}

// This is an example of how to use Case.
func ExampleCase() {
	user := q.T("user")
	cs := q.Case().When(
		q.Eq(user.C("id"), 100),
		10,
	).Else(
		0,
	)
	fmt.Println(cs)
	fmt.Println(q.Select().From(user).Column(cs.C("bonus")))
	// Output:
	// CASE WHEN "user"."id" = ? THEN ? ELSE ? END [100 10 0]
	// SELECT CASE WHEN "user"."id" = ? THEN ? ELSE ? END AS "bonus" FROM "user" [100 10 0]
}

// This is an example of how to use Delete.
func ExampleDelete() {
	user := q.T("user")
	del := q.Delete(user).Where(q.Eq(user.C("id"), 1))
	// del := q.Delete().From(user).Where(q.Eq(user.C("id"), 1)) // same
	fmt.Println(del)

	// Even in this case, the original name is used as a table and a column name
	// because Insert, Delete and Update aren't supporting "AS" syntax.
	u := q.T("user", "u")
	fmt.Println(q.Delete(u).Where(q.Eq(u.C("id", "i"), 1)))
	// Output:
	// DELETE FROM "user" WHERE "id" = ? [1]
	// DELETE FROM "user" WHERE "id" = ? [1]
}

// This is an example of how to use Update.
func ExampleUpdate() {
	upd := q.Update(q.T("user")).Set(q.C("name"), "hackme").Where(q.Eq(q.C("id"), 1))
	fmt.Println(upd)
	// Even in this case, the original name is used as a table and a column name
	// because Insert, Delete and Update aren't supporting "AS" syntax.
	u := q.T("user", "u")
	fmt.Println(q.Update(u).Set(u.C("name"), "hackme").Where(q.Eq(u.C("id"), 1)))
	// When overwriting in the same name, the last one is effective.
	fmt.Println(q.Update(u).Set(u.C("name"), "hackyou").Set(u.C("name"), "hackme").Where(q.Eq(u.C("id"), 1)))
	// Output:
	// UPDATE "user" SET "name" = ? WHERE "id" = ? [hackme 1]
	// UPDATE "user" SET "name" = ? WHERE "id" = ? [hackme 1]
	// UPDATE "user" SET "name" = ? WHERE "id" = ? [hackme 1]
}

// This is an example of how to use Insert.
func ExampleInsert() {
	user := q.T("user")
	ins := q.Insert().Into(user).Set(user.C("name"), "hackme")
	fmt.Println(ins)
	// Output:
	// INSERT INTO "user"("name") VALUES (?) [hackme]
}
