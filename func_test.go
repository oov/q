package q

import (
	"database/sql"
	"testing"

	"github.com/oov/q/qutil"
)

func TestCurrentTimestampOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)
			for i, test := range intervalTests {
				var r int64
				sql, args := Select().SetDialect(d).Column(CountAll().C("c")).From(T("post")).Where(Lt(C("at"), Now())).ToSQL()
				if err := db.QueryRow(sql, args...).Scan(&r); err != nil {
					t.Fatalf("%s tests[%d] %s Error: %v\n%s", d, i, test.Name, err, sql)
				}
				if r != 4 {
					t.Errorf("%s test[%d] %s want 1 got %v", d, i, test.Name, r)
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
