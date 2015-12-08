package q

import "testing"

func TestDelete(t *testing.T) {
	Delete().From(T("user")).Where(Eq(C("id"), 1))
}
