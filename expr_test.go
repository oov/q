package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

func TestRealDBExpression(t *testing.T) {
	resultMap := func(r ...string) map[qutil.Dialect]string {
		return map[qutil.Dialect]string{
			MySQL:      r[0],
			PostgreSQL: r[1],
			SQLite:     r[2],
		}
	}
	tests := []struct {
		Name  string
		B     *ZSelectBuilder
		Value map[qutil.Dialect]string
	}{
		{
			Name:  `=`,
			B:     Select().Column(Eq(0, 1).C()),
			Value: resultMap("0", "false", "0"),
		},
		{
			Name:  `=(IN)`,
			B:     Select().Column(Eq(0, []int{1, 2, 3}).C()),
			Value: resultMap("0", "false", "0"),
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
					var r string
					sql, args := test.B.SetDialect(d).ToSQL()
					if err := db.QueryRow(sql, args...).Scan(&r); err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v\n%s", d, i, test.Name, err, sql)
					}
					if fmt.Sprint(r) != test.Value[d] {
						t.Errorf("%s test[%d] %s want %v got %v", d, i, test.Name, test.Value, r)
					}
				}()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
