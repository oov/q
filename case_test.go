package q

import "testing"

func TestCase(t *testing.T) {
	tests := []struct {
		Name string
		C    *ZCaseBuilder
		Want string
	}{
		{
			Name: "empty simple case",
			C:    Case(C("test")),
			Want: "NULL []",
		},
		{
			Name: "else only simple case",
			C:    Case(C("test")).Else(0),
			Want: "? [0]",
		},
		{
			Name: "no else simple case",
			C:    Case(C("test")).When(0, 1),
			Want: `CASE "test" WHEN ? THEN ? END [0 1]`,
		},
		{
			Name: "simple case",
			C:    Case(C("test")).When(0, 1).Else(2),
			Want: `CASE "test" WHEN ? THEN ? ELSE ? END [0 1 2]`,
		},
		{
			Name: "empty searched case",
			C:    Case(),
			Want: "NULL []",
		},
		{
			Name: "else only searched case",
			C:    Case().Else(0),
			Want: "? [0]",
		},
		{
			Name: "no else searched case",
			C:    Case().When(Eq(C("test"), 0), 1),
			Want: `CASE WHEN "test" = ? THEN ? END [0 1]`,
		},
		{
			Name: "searched case",
			C:    Case().When(Eq(C("test"), 0), 1).Else(2),
			Want: `CASE WHEN "test" = ? THEN ? ELSE ? END [0 1 2]`,
		},
	}
	for i, test := range tests {
		if r := test.C.String(); r != test.Want {
			t.Errorf("tests[%d] %s: want %q got %q", i, test.Name, test.Want, r)
		}
	}
}
