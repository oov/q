package q

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/oov/q/qutil"
)

var expressionTests = []struct {
	Name string
	E    Expression
	Want map[qutil.Dialect]string
	V    string
}{
	{
		Name: `=`,
		E:    Eq(0, 1),
		Want: resultMap("0", "false", "0"),
		V:    `? = ? [0 1]`,
	},
	{
		Name: `=(NULL)`,
		// Unsafe is workaround for PostgreSQL "could not determine data type of parameter $1" error.
		E:    Eq(Unsafe(0), nil),
		Want: resultMap("0", "false", "0"),
		V:    `0 IS NULL []`,
	},
	{
		Name: `=(NULL)2`,
		E:    Eq(nil, Unsafe(0)),
		Want: resultMap("0", "false", "0"),
		V:    `0 IS NULL []`,
	},
	{
		Name: `=(NULL)3`,
		E:    Eq(nil, nil),
		Want: resultMap("1", "true", "1"),
		V:    `NULL IS NULL []`,
	},
	{
		Name: `= empty slice`,
		E:    Eq(0, []int{}),
		Want: resultMap("0", "false", "0"),
		V:    `'IN' = '()' []`,
	},
	{
		Name: `= slice`,
		E:    Eq(0, []int{1, 2, 3}),
		Want: resultMap("0", "false", "0"),
		V:    `? IN (?,?,?) [0 1 2 3]`,
	},
	{
		Name: `!=`,
		E:    Neq(0, 1),
		Want: resultMap("1", "true", "1"),
		V:    `? != ? [0 1]`,
	},
	{
		Name: `!=(NULL)`,
		E:    Neq(Unsafe(0), nil),
		Want: resultMap("1", "true", "1"),
		V:    `0 IS NOT NULL []`,
	},
	{
		Name: `!=(NULL)2`,
		E:    Neq(nil, Unsafe(0)),
		Want: resultMap("1", "true", "1"),
		V:    `0 IS NOT NULL []`,
	},
	{
		Name: `!=(NULL)3`,
		E:    Neq(nil, nil),
		Want: resultMap("0", "false", "0"),
		V:    `NULL IS NOT NULL []`,
	},
	{
		Name: `!= empty slice`,
		E:    Neq(0, []int{}),
		Want: resultMap("1", "true", "1"),
		V:    `'IN' != '()' []`,
	},
	{
		Name: `!= slice`,
		E:    Neq(0, []int{1, 2, 3}),
		Want: resultMap("1", "true", "1"),
		V:    `? NOT IN (?,?,?) [0 1 2 3]`,
	},
	{
		Name: `>`,
		E:    Gt(0, 1),
		Want: resultMap("0", "false", "0"),
		V:    `? > ? [0 1]`,
	},
	{
		Name: `>=`,
		E:    Gte(0, 1),
		Want: resultMap("0", "false", "0"),
		V:    `? >= ? [0 1]`,
	},
	{
		Name: `<`,
		E:    Lt(1, 0),
		Want: resultMap("0", "false", "0"),
		V:    `? < ? [1 0]`,
	},
	{
		Name: `<=`,
		E:    Lte(1, 0),
		Want: resultMap("0", "false", "0"),
		V:    `? <= ? [1 0]`,
	},
	{
		Name: `IN empty slice`,
		E:    In(0, []int{}),
		Want: resultMap("0", "false", "0"),
		V:    `'IN' = '()' []`,
	},
	{
		Name: `IN slice`,
		E:    In(0, []int{1, 2, 3}),
		Want: resultMap("0", "false", "0"),
		V:    `? IN (?,?,?) [0 1 2 3]`,
	},
	{
		Name: `IN builder`,
		E:    In(0, Select().Column(C("id")).From(T("user"))),
		Want: resultMap("0", "false", "0"),
		V:    `? IN (SELECT "id" FROM "user") [0]`,
	},
	{
		Name: `NOT IN empty slice`,
		E:    NotIn(0, []int{}),
		Want: resultMap("1", "true", "1"),
		V:    `'IN' != '()' []`,
	},
	{
		Name: `NOT IN slice`,
		E:    NotIn(0, []int{1, 2, 3}),
		Want: resultMap("1", "true", "1"),
		V:    `? NOT IN (?,?,?) [0 1 2 3]`,
	},
	{
		Name: `NOT IN builder`,
		E:    NotIn(0, Select().Column(C("id")).From(T("user"))),
		Want: resultMap("1", "true", "1"),
		V:    `? NOT IN (SELECT "id" FROM "user") [0]`,
	},
	{
		Name: `AND empty`,
		E:    And(),
		Want: resultMap("0", "false", "0"),
		V:    `('empty' = 'AND') []`,
	},
	{
		Name: `AND 1`,
		E:    And(Eq(0, 1)),
		Want: resultMap("0", "false", "0"),
		V:    `? = ? [0 1]`,
	},
	{
		Name: `AND 2`,
		E:    And(Eq(0, 1), Neq(0, 0)),
		Want: resultMap("0", "false", "0"),
		V:    `(? = ?)AND(? != ?) [0 1 0 0]`,
	},
	{
		Name: `OR empty`,
		E:    Or(),
		Want: resultMap("0", "false", "0"),
		V:    `('empty' = 'OR') []`,
	},
	{
		Name: `OR 1`,
		E:    Or(Eq(0, 1)),
		Want: resultMap("0", "false", "0"),
		V:    `? = ? [0 1]`,
	},
	{
		Name: `OR 2`,
		E:    Or(Eq(0, 1), Neq(0, 0)),
		Want: resultMap("0", "false", "0"),
		V:    `(? = ?)OR(? != ?) [0 1 0 0]`,
	},
	{
		Name: `Unsafe`,
		E:    Unsafe(3, "%", 2),
		Want: resultMap("1", "1", "1"),
		V:    `3%2 []`,
	},
	{
		Name: `Unsafe + Expression nil`,
		E:    Unsafe(C("id"), " IS ", nil),
		Want: resultMap("0", "false", "0"),
		V:    `"id" IS NULL []`,
	},
	{
		Name: `Unsafe + V`,
		E:    Unsafe(C("id"), " = ", V(0)),
		Want: resultMap("0", "false", "0"),
		V:    `"id" = ? [0]`,
	},
	{
		Name: `Unsafe + InV(not slice)`,
		E:    Unsafe(C("id"), " IN ", InV(0)),
		Want: resultMap("0", "false", "0"),
		V:    `"id" IN (?) [0]`,
	},
	{
		Name: `Unsafe + InV(slice)`,
		E:    Unsafe(C("id"), " IN ", InV([]int{100, 101, 102})),
		Want: resultMap("0", "false", "0"),
		V:    `"id" IN (?,?,?) [100 101 102]`,
	},
	{
		Name: `COUNT(*)`,
		E:    CountAll(),
		Want: resultMap("2", "2", "2"),
		V:    `COUNT(*) []`,
	},
	{
		Name: `COUNT`,
		E:    Count(C("id")),
		Want: resultMap("2", "2", "2"),
		V:    `COUNT("id") []`,
	},
	{
		Name: `AVG`,
		E:    Avg(C("age")),
		Want: resultMap("29.5000", "29.5000000000000000", "29.5"),
		V:    `AVG("age") []`,
	},
	{
		Name: `MAX`,
		E:    Max(C("age")),
		Want: resultMap("44", "44", "44"),
		V:    `MAX("age") []`,
	},
	{
		Name: `MIN`,
		E:    Min(C("age")),
		Want: resultMap("15", "15", "15"),
		V:    `MIN("age") []`,
	},
	{
		Name: `SUM`,
		E:    Sum(C("age")),
		Want: resultMap("59", "59", "59"),
		V:    `SUM("age") []`,
	},
	{
		Name: `CHAR_LENGTH`,
		E:    CharLength(C("name")),
		Want: resultMap("6", "6", "6"),
		V:    `CHAR_LENGTH("name") []`,
	},
}

func TestNullC(t *testing.T) {
	want := `NULL AS "n" []`
	if r := fmt.Sprint(nullExpr{}.C("n")); want != r {
		t.Errorf("want %q got %q", want, r)
	}
}

func TestInVEmptySlice(t *testing.T) {
	want := "() []"
	if r := fmt.Sprint(InV([]int{})); want != r {
		t.Errorf("want %q got %q", want, r)
	}
}

func TestExpression(t *testing.T) {
	for i, test := range expressionTests {
		if r := fmt.Sprint(test.E); r != test.V {
			t.Errorf("test[%d] %s want %s got %s", i, test.Name, test.V, r)
		}
	}
}

func resultMap(r ...string) map[qutil.Dialect]string {
	return map[qutil.Dialect]string{
		MySQL:      r[0],
		PostgreSQL: r[1],
		SQLite:     r[2],
	}
}

func TestExpressionOnDB(t *testing.T) {
	for _, testData := range testModel {
		err := testData.tester(func(db *sql.DB, d qutil.Dialect) {
			defer exec(t, "drops", db, d, testData.drops)
			exec(t, "drops", db, d, testData.drops)
			exec(t, "creates", db, d, testData.creates)
			exec(t, "inserts", db, d, testData.inserts)
			for i, test := range expressionTests {
				func() {
					var r string
					sql, args := Select().Column(test.E.C()).From(T("user")).Limit(1).SetDialect(d).ToSQL()
					if err := db.QueryRow(sql, args...).Scan(&r); err != nil {
						t.Fatalf("%s tests[%d] %s Error: %v\n%s", d, i, test.Name, err, sql)
					}
					if fmt.Sprint(r) != fmt.Sprint(test.Want[d]) {
						t.Errorf("%s test[%d] %s want %v got %v", d, i, test.Name, test.Want[d], r)
					}
				}()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestVariableStringer(t *testing.T) {
	if want, r := `? [1]`, fmt.Sprint(V(1)); r != want {
		t.Errorf(`V(1) want %s got %s`, want, r)
	}
	if want, r := `? AS "v" [1]`, fmt.Sprint(V(1).C("v")); r != want {
		t.Errorf(`V(1).C() want %s got %s`, want, r)
	}
	if want, r := `(?,?,?) [1 2 3]`, fmt.Sprint(InV([]int{1, 2, 3})); r != want {
		t.Errorf(`InV([]int{1, 2, 3}) want %s got %s`, want, r)
	}
	if want, r := `(?) AS "v" [1]`, fmt.Sprint(InV([]int{1}).C("v")); r != want {
		t.Errorf(`InV([]int{1}).C() want %s got %s`, want, r)
	}
}

func BenchmarkSimpleExpr(b *testing.B) {
	c := C("test")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Gt(c, 1)
	}
}

func BenchmarkEqExpr(b *testing.B) {
	c := C("test")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Eq(c, 1)
	}
}

func BenchmarkInExpr(b *testing.B) {
	c, s := C("test"), []int{1, 2, 3}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		In(c, s)
	}
}

func BenchmarkLogicalExpr(b *testing.B) {
	eq := Eq(C("test"), []int{1, 2, 3})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		And(eq, eq, eq)
	}
}

func BenchmarkInV(b *testing.B) {
	s := []int{1, 2, 3}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		InV(s)
	}
}
