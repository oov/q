package q

import "testing"

func TestEscape(t *testing.T) {
	testData := []struct {
		Before, After string
	}{
		{Before: `keyword`, After: `"keyword"`},
		{Before: `key"word`, After: `"key""word"`},
		{Before: `"key"word"`, After: `"""key""word"""`},
		{Before: ``, After: `""`},
	}
	for i, test := range testData {
		b := escape(nil, '"', test.Before)
		if string(b) != test.After {
			t.Errorf("[%d] want %q got %q", i, test.After, b)
		}
	}
}
