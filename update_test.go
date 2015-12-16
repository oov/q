package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var updateTests = []struct {
	Name string
	B    *ZUpdateBuilder
	Want []string
	V    string
}{
	{
		Name: "beginning",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Update(user, "update").Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `update "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
	{
		Name: "Set overwrite",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Update(user, "update").Set(name, "Shiponium").Set(age, 16).Set(age, 17).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "17"},
		V:    `update "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 17 1]`,
	},
	{
		Name: "Unset first",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Update(user, "update").Set(name, "Shiponium").Set(age, 16).Unset(name).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shipon", "16"},
		V:    `update "user" SET "age" = ? WHERE "id" = ? [16 1]`,
	},
	{
		Name: "Unset middle",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Update(user, "update").Set(id, 1).Set(name, "Shiponium").Set(age, 16).Unset(name).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shipon", "16"},
		V:    `update "user" SET "id" = ?, "age" = ? WHERE "id" = ? [1 16 1]`,
	},
	{
		Name: "Unset last",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Update(user, "update").Set(name, "Shiponium").Set(age, 16).Unset(age).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "15"},
		V:    `update "user" SET "name" = ? WHERE "id" = ? [Shiponium 1]`,
	},
	{
		Name: "Unset unmatched",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Update(user, "update").Set(name, "Shiponium").Set(age, 16).Unset(C("unmatched")).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `update "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
	{
		Name: "T and C",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `UPDATE "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
	{
		Name: "T and C(alias)",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
			return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `UPDATE "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
	{
		Name: "T(alias) and C",
		B: func() *ZUpdateBuilder {
			user := T("user", "u")
			id, name, age := C("id"), C("name"), C("age")
			return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `UPDATE "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
	{
		Name: "T(alias) and C(alias)",
		B: func() *ZUpdateBuilder {
			user := T("user", "u")
			id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
			return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `UPDATE "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
	{
		Name: "T and Table.C",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := user.C("id"), user.C("name"), user.C("age")
			return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `UPDATE "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
	{
		Name: "T and Table.C(alias)",
		B: func() *ZUpdateBuilder {
			user := T("user")
			id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
			return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `UPDATE "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
	{
		Name: "T(alias) and Table.C",
		B: func() *ZUpdateBuilder {
			user := T("user", "u")
			id, name, age := user.C("id"), user.C("name"), user.C("age")
			return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `UPDATE "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
	{
		Name: "T(alias) and Table.C(alias)",
		B: func() *ZUpdateBuilder {
			user := T("user", "u")
			id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
			return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
		}(),
		Want: []string{"1", "Shiponium", "16"},
		V:    `UPDATE "user" SET "name" = ?, "age" = ? WHERE "id" = ? [Shiponium 16 1]`,
	},
}

func TestUpdatePanic(t *testing.T) {
	defer func() {
		if e := recover(); e == nil {
			t.Error("want Panic got Nothing")
		}
	}()
	Update(T("test")).String()
}

func TestUpdate(t *testing.T) {
	for i, test := range updateTests {
		if r := test.B.String(); r != test.V {
			t.Errorf("tests[%d] %s: want %q got %q", i, test.Name, test.V, r)
		}
	}
}

func TestUpdateOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)
			for i, test := range updateTests {
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

					sql, args := test.B.SetDialect(d).ToSQL()
					if _, err = tx.Exec(sql, args...); err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v\n%s", d, i, test.Name, err, sql)
					}

					r := []string{"", "", ""}
					sql, args = Select().From(T("user")).Where(Eq(C("id"), 1)).SetDialect(d).ToSQL()
					if err = tx.QueryRow(sql, args...).Scan(&r[0], &r[1], &r[2]); err != nil {
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
