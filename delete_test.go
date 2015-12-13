package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var deleteTests = []struct {
	Name string
	B    *ZDeleteBuilder
	Want []string
	V    string
}{
	{
		Name: "Delete(T(name))",
		B: func() *ZDeleteBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Delete(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
		}(),
		Want: []string{"2", "Mr.TireMan", "44"},
		V:    `DELETE FROM "user" WHERE ("id" = ?)AND("name" = ?)AND("age" = ?) [1 Shipon 15]`,
	},
	{
		Name: "T and C",
		B: func() *ZDeleteBuilder {
			user := T("user")
			id, name, age := C("id"), C("name"), C("age")
			return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
		}(),
		Want: []string{"2", "Mr.TireMan", "44"},
		V:    `DELETE FROM "user" WHERE ("id" = ?)AND("name" = ?)AND("age" = ?) [1 Shipon 15]`,
	},
	{
		Name: "T and C(alias)",
		B: func() *ZDeleteBuilder {
			user := T("user")
			id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
			return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
		}(),
		Want: []string{"2", "Mr.TireMan", "44"},
		V:    `DELETE FROM "user" WHERE ("id" = ?)AND("name" = ?)AND("age" = ?) [1 Shipon 15]`,
	},
	{
		Name: "T(alias) and C",
		B: func() *ZDeleteBuilder {
			user := T("user", "u")
			id, name, age := C("id"), C("name"), C("age")
			return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
		}(),
		Want: []string{"2", "Mr.TireMan", "44"},
		V:    `DELETE FROM "user" WHERE ("id" = ?)AND("name" = ?)AND("age" = ?) [1 Shipon 15]`,
	},
	{
		Name: "T(alias) and C(alias)",
		B: func() *ZDeleteBuilder {
			user := T("user", "u")
			id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
			return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
		}(),
		Want: []string{"2", "Mr.TireMan", "44"},
		V:    `DELETE FROM "user" WHERE ("id" = ?)AND("name" = ?)AND("age" = ?) [1 Shipon 15]`,
	},
	{
		Name: "T and Table.C",
		B: func() *ZDeleteBuilder {
			user := T("user")
			id, name, age := user.C("id"), user.C("name"), user.C("age")
			return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
		}(),
		Want: []string{"2", "Mr.TireMan", "44"},
		V:    `DELETE FROM "user" WHERE ("id" = ?)AND("name" = ?)AND("age" = ?) [1 Shipon 15]`,
	},
	{
		Name: "T and Table.C(alias)",
		B: func() *ZDeleteBuilder {
			user := T("user")
			id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
			return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
		}(),
		Want: []string{"2", "Mr.TireMan", "44"},
		V:    `DELETE FROM "user" WHERE ("id" = ?)AND("name" = ?)AND("age" = ?) [1 Shipon 15]`,
	},
	{
		Name: "T(alias) and Table.C",
		B: func() *ZDeleteBuilder {
			user := T("user", "u")
			id, name, age := user.C("id"), user.C("name"), user.C("age")
			return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
		}(),
		Want: []string{"2", "Mr.TireMan", "44"},
		V:    `DELETE FROM "user" WHERE ("id" = ?)AND("name" = ?)AND("age" = ?) [1 Shipon 15]`,
	},
	{
		Name: "T(alias) and Table.C(alias)",
		B: func() *ZDeleteBuilder {
			user := T("user", "u")
			id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
			return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
		}(),
		Want: []string{"2", "Mr.TireMan", "44"},
		V:    `DELETE FROM "user" WHERE ("id" = ?)AND("name" = ?)AND("age" = ?) [1 Shipon 15]`,
	},
}

func TestDeletePanic(t *testing.T) {
	defer func() {
		if e := recover(); e == nil {
			t.Error("want Panic got Nothing")
		}
	}()
	Delete().String()
}

func TestDelete(t *testing.T) {
	for i, test := range deleteTests {
		if r := fmt.Sprint(test.B); r != test.V {
			t.Errorf("tests[%d] %s want %v got %v", i, test.Name, test.Want, r)
		}
	}
}

func TestDeleteOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)
			for i, test := range deleteTests {
				func() {
					tx, err := db.Begin()
					if err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v", d, i, test.Name, err)
					}
					defer func() {
						if err = tx.Rollback(); err != nil {
							t.Fatalf("%s tests[%d] %s Error: %v", d, i, test.Name, err)
						}
					}()

					sql, args := test.B.SetDialect(d).ToSQL()
					if _, err = tx.Exec(sql, args...); err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v\n%s", d, i, test.Name, err, sql)
					}

					r := []string{"", "", ""}
					sql, _ = Select().From(T("user")).SetDialect(d).ToSQL()
					if err = tx.QueryRow(sql).Scan(&r[0], &r[1], &r[2]); err != nil {
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
