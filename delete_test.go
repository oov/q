package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

func TestRealDBDelete(t *testing.T) {
	tests := []struct {
		name string
		b    *ZDeleteBuilder
		val  []string
	}{
		{
			name: "T and C",
			b: func() *ZDeleteBuilder {
				user := T("user")
				id, name, age := C("id"), C("name"), C("age")
				return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
			}(),
			val: []string{"2", "Mr.TireMan", "44"},
		},
		{
			name: "T and C(alias)",
			b: func() *ZDeleteBuilder {
				user := T("user")
				id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
				return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
			}(),
			val: []string{"2", "Mr.TireMan", "44"},
		},
		{
			name: "T(alias) and C",
			b: func() *ZDeleteBuilder {
				user := T("user", "u")
				id, name, age := C("id"), C("name"), C("age")
				return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
			}(),
			val: []string{"2", "Mr.TireMan", "44"},
		},
		{
			name: "T(alias) and C(alias)",
			b: func() *ZDeleteBuilder {
				user := T("user", "u")
				id, name, age := C("id", "i"), C("name", "n"), C("age", "a")
				return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
			}(),
			val: []string{"2", "Mr.TireMan", "44"},
		},
		{
			name: "T and Table.C",
			b: func() *ZDeleteBuilder {
				user := T("user")
				id, name, age := user.C("id"), user.C("name"), user.C("age")
				return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
			}(),
			val: []string{"2", "Mr.TireMan", "44"},
		},
		{
			name: "T and Table.C(alias)",
			b: func() *ZDeleteBuilder {
				user := T("user")
				id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
				return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
			}(),
			val: []string{"2", "Mr.TireMan", "44"},
		},
		{
			name: "T(alias) and Table.C",
			b: func() *ZDeleteBuilder {
				user := T("user", "u")
				id, name, age := user.C("id"), user.C("name"), user.C("age")
				return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
			}(),
			val: []string{"2", "Mr.TireMan", "44"},
		},
		{
			name: "T(alias) and Table.C(alias)",
			b: func() *ZDeleteBuilder {
				user := T("user", "u")
				id, name, age := user.C("id", "i"), user.C("name", "n"), user.C("age", "a")
				return Delete().From(user).Where(Eq(id, "1"), Eq(name, "Shipon"), Eq(age, 15))
			}(),
			val: []string{"2", "Mr.TireMan", "44"},
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
