package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var insertTests = []struct {
	name string
	b    *ZInsertBuilder
	val  []string
}{
	{
		name: "T and C",
		b: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		val: []string{"1", "TestMan", "100"},
	},
	{
		name: "T and C(alias)",
		b: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		val: []string{"1", "TestMan", "100"},
	},
	{
		name: "T(alias) and C",
		b: func() *ZInsertBuilder {
			user := T("user", "u")
			id, name, age := C("id"), C("name"), C("age")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		val: []string{"1", "TestMan", "100"},
	},
	{
		name: "T(alias) and C(alias)",
		b: func() *ZInsertBuilder {
			user := T("user", "u")
			id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		val: []string{"1", "TestMan", "100"},
	},
	{
		name: "T and Table.C",
		b: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := user.C("id"), user.C("name"), user.C("age")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		val: []string{"1", "TestMan", "100"},
	},
	{
		name: "T and Table.C(alias)",
		b: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		val: []string{"1", "TestMan", "100"},
	},
	{
		name: "T(alias) and Table.C",
		b: func() *ZInsertBuilder {
			user := T("user", "u")
			id, name, age := user.C("id"), user.C("name"), user.C("age")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		val: []string{"1", "TestMan", "100"},
	},
	{
		name: "T(alias) and Table.C(alias)",
		b: func() *ZInsertBuilder {
			user := T("user", "u")
			id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		val: []string{"1", "TestMan", "100"},
	},
}

func TestRealDBInsert(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			for i, test := range insertTests {
				func() {
					tx, err := db.Begin()
					if err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v", d, i, test.name, err)
					}
					defer func() {
						if err = tx.Rollback(); err != nil {
							t.Fatalf("%s tests[%d] %s Error: %v", d, i, test.name, err)
						}
					}()

					sql, args := test.b.SetDialect(d).ToSQL()
					if _, err = tx.Exec(sql, args...); err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v\n%s", d, i, test.name, err, sql)
					}

					r := []string{"", "", ""}
					sql, _ = Select().From(T("user")).SetDialect(d).ToSQL()
					if err = tx.QueryRow(sql).Scan(&r[0], &r[1], &r[2]); err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v", d, i, test.name, err)
					}
					if fmt.Sprint(r) != fmt.Sprint(test.val) {
						t.Errorf("%s test[%d] %s want %v got %v", d, i, test.name, test.val, r)
					}
				}()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
