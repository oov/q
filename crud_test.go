package q

import (
	"fmt"
	"testing"
)

var crudTests = []struct {
	Name                       string
	T                          Table
	C                          Column
	WantC, WantR, WantU, WantD string
}{
	{
		Name:  `T(name).C(name, alias)`,
		T:     T("user"),
		C:     T("user").C("id", "i"),
		WantC: `INSERT INTO "user"("id") VALUES (?) [1]`,
		WantR: `SELECT "user"."id" AS "i" FROM "user" []`,
		WantU: `UPDATE "user" SET "id" = ? [1]`,
		WantD: `DELETE FROM "user" WHERE "id" = ? [1]`,
	},
}

func TestColumnCRUD(t *testing.T) {
	for i, test := range crudTests {
		if r := fmt.Sprint(Insert().Into(test.T).Set(test.C, 1)); r != test.WantC {
			t.Errorf("tests[%d] %s Create(INSERT) want %s got %s", i, test.Name, test.WantC, r)
		}
		if r := fmt.Sprint(Select().From(test.T).Column(test.C)); r != test.WantR {
			t.Errorf("tests[%d] %s Read(SELECT) want %s got %s", i, test.Name, test.WantR, r)
		}
		if r := fmt.Sprint(Update(test.T).Set(test.C, 1)); r != test.WantU {
			t.Errorf("tests[%d] %s Update(UPDATE) want %s got %s", i, test.Name, test.WantU, r)
		}
		if r := fmt.Sprint(Delete().From(test.T).Where(Eq(test.C, 1))); r != test.WantD {
			t.Errorf("tests[%d] %s Update(UPDATE) want %s got %s", i, test.Name, test.WantD, r)
		}
	}
}
