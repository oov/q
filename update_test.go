package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

func TestRealDBUpdate(t *testing.T) {
	tests := []struct {
		name string
		b    *ZUpdateBuilder
		val  []string
	}{
		{
			name: "T and C",
			b: func() *ZUpdateBuilder {
				user := T("user")
				id, name, age := C("id"), C("name"), C("age")
				return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
			}(),
			val: []string{"1", "Shiponium", "16"},
		},
		{
			name: "T and C(alias)",
			b: func() *ZUpdateBuilder {
				user := T("user")
				id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
				return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
			}(),
			val: []string{"1", "Shiponium", "16"},
		},
		{
			name: "T(alias) and C",
			b: func() *ZUpdateBuilder {
				user := T("user", "u")
				id, name, age := C("id"), C("name"), C("age")
				return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
			}(),
			val: []string{"1", "Shiponium", "16"},
		},
		{
			name: "T(alias) and C(alias)",
			b: func() *ZUpdateBuilder {
				user := T("user", "u")
				id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
				return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
			}(),
			val: []string{"1", "Shiponium", "16"},
		},
		{
			name: "T and Table.C",
			b: func() *ZUpdateBuilder {
				user := T("user")
				id, name, age := user.C("id"), user.C("name"), user.C("age")
				return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
			}(),
			val: []string{"1", "Shiponium", "16"},
		},
		{
			name: "T and Table.C(alias)",
			b: func() *ZUpdateBuilder {
				user := T("user")
				id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
				return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
			}(),
			val: []string{"1", "Shiponium", "16"},
		},
		{
			name: "T(alias) and Table.C",
			b: func() *ZUpdateBuilder {
				user := T("user", "u")
				id, name, age := user.C("id"), user.C("name"), user.C("age")
				return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
			}(),
			val: []string{"1", "Shiponium", "16"},
		},
		{
			name: "T(alias) and Table.C(alias)",
			b: func() *ZUpdateBuilder {
				user := T("user", "u")
				id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
				return Update(user).Set(name, "Shiponium").Set(age, 16).Where(Eq(id, "1"))
			}(),
			val: []string{"1", "Shiponium", "16"},
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
					sql, args = Select().From(T("user")).Where(Eq(C("id"), 1)).SetDialect(d).ToSQL()
					if err = tx.QueryRow(sql, args...).Scan(&r[0], &r[1], &r[2]); err != nil {
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
