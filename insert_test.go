package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var insertTests = []struct {
	Name string
	B    *ZInsertBuilder
	Want []sql.NullString
	V    string
}{
	{
		Name: "beginning",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Insert("insert").Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `insert INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 100]`,
	},
	{
		Name: "Set overwrite",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Insert("insert").Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100).Set(age, 200)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"200", true}},
		V:    `insert INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 200]`,
	},
	{
		Name: "Unset first",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Insert("insert").Into(user).Set(name, "TestMan").Set(id, "1").Set(age, 100).Unset(name)
		}(),
		Want: []sql.NullString{{"1", true}, {}, {"100", true}},
		V:    `insert INTO "user"("id", "age") VALUES (?, ?) [1 100]`,
	},
	{
		Name: "Unset middle",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Insert("insert").Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100).Unset(name)
		}(),
		Want: []sql.NullString{{"1", true}, {"", false}, {"100", true}},
		V:    `insert INTO "user"("id", "age") VALUES (?, ?) [1 100]`,
	},
	{
		Name: "Unset last",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Insert("insert").Into(user).Set(id, "1").Set(age, 100).Set(name, "TestMan").Unset(name)
		}(),
		Want: []sql.NullString{{"1", true}, {"", false}, {"100", true}},
		V:    `insert INTO "user"("id", "age") VALUES (?, ?) [1 100]`,
	},
	{
		Name: "Unset unmatched",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Insert("insert").Into(user).Set(id, "1").Set(age, 100).Set(name, "TestMan").Unset(C("unmatched"))
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `insert INTO "user"("id", "age", "name") VALUES (?, ?, ?) [1 100 TestMan]`,
	},
	{
		Name: "T and C",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `INSERT INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 100]`,
	},
	{
		Name: "T and C(alias)",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `INSERT INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 100]`,
	},
	{
		Name: "T(alias) and C",
		B: func() *ZInsertBuilder {
			user := T("user", "u")
			id, name, age := C("id"), C("name"), C("age")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `INSERT INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 100]`,
	},
	{
		Name: "T(alias) and C(alias)",
		B: func() *ZInsertBuilder {
			user := T("user", "u")
			id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `INSERT INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 100]`,
	},
	{
		Name: "T and Table.C",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := user.C("id"), user.C("name"), user.C("age")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `INSERT INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 100]`,
	},
	{
		Name: "T and Table.C(alias)",
		B: func() *ZInsertBuilder {
			user := T("user")
			id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `INSERT INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 100]`,
	},
	{
		Name: "T(alias) and Table.C",
		B: func() *ZInsertBuilder {
			user := T("user", "u")
			id, name, age := user.C("id"), user.C("name"), user.C("age")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `INSERT INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 100]`,
	},
	{
		Name: "T(alias) and Table.C(alias)",
		B: func() *ZInsertBuilder {
			user := T("user", "u")
			id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
			return Insert().Into(user).Set(id, "1").Set(name, "TestMan").Set(age, 100)
		}(),
		Want: []sql.NullString{{"1", true}, {"TestMan", true}, {"100", true}},
		V:    `INSERT INTO "user"("id", "name", "age") VALUES (?, ?, ?) [1 TestMan 100]`,
	},
}

func TestInsertPanic(t *testing.T) {
	defer func() {
		if e := recover(); e == nil {
			t.Error("want Panic got Nothing")
		}
	}()
	Insert().String()
}

func TestInsert(t *testing.T) {
	for i, test := range insertTests {
		if r := fmt.Sprint(test.B); r != fmt.Sprint(test.V) {
			t.Errorf("test[%d] %s want %v got %v", i, test.Name, test.V, r)
		}
	}
}

func TestInsertOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			if d == PostgreSQL {
				func() {
					tx, err := db.Begin()
					if err != nil {
						t.Fatalf("%s Begin Error: %v", d, err)
					}
					defer func() {
						if err = tx.Rollback(); err != nil {
							t.Fatalf("%s Rollback Error: %v", d, err)
						}
					}()
					user := T("user")
					b := Insert().SetDialect(d).Into(user).
						Set(user.C("name"), "x").
						Set(user.C("age"), 100).
						Returning(user.C("id"))
					sql, args := b.ToSQL()
					var i int64
					if err = tx.QueryRow(sql, args...).Scan(&i); err != nil {
						t.Fatalf("%s Returning Error: %v\n%s", d, err, sql)
					}
					if i == 0 {
						t.Errorf("%s Returning want not 0 got 0", d)
					}

					sql, args = b.Returning(user.C("name", "n")).ToSQL()
					var n string
					if err = tx.QueryRow(sql, args...).Scan(&i, &n); err != nil {
						t.Fatalf("%s Returning Error: %v\n%s", d, err, sql)
					}
					if i == 0 {
						t.Errorf("%s Returning want not 0 got 0", d)
					}
					if n == "" {
						t.Errorf("%s Returning want not empty got empty", d)
					}
				}()
			}
			for i, test := range insertTests {
				func() {
					tx, err := db.Begin()
					if err != nil {
						t.Fatalf("%s Begin Error: %v", d, err)
					}
					defer func() {
						if err = tx.Rollback(); err != nil {
							t.Fatalf("%s Rollback Error: %v", d, err)
						}
					}()

					s, args := test.B.SetDialect(d).ToSQL()
					if _, err = tx.Exec(s, args...); err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v\n%s", d, i, test.Name, err, s)
					}

					r := []sql.NullString{{}, {}, {}}
					s, _ = Select().From(T("user")).SetDialect(d).ToSQL()
					if err = tx.QueryRow(s).Scan(&r[0], &r[1], &r[2]); err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v", d, i, test.Name, err)
					}
					if fmt.Sprint(r) != fmt.Sprint(test.Want) {
						t.Errorf("%s test[%d] %s want %v got %v", d, i, test.Name, test.Want, r)
					}
				}()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
