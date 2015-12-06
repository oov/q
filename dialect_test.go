package q

import "testing"

func TestPostgresPlaceholder(t *testing.T) {
	testData := []struct {
		Before, After string
	}{
		{Before: ``, After: `$1`},
		{Before: `keyword`, After: `keyword$2`},
	}
	p := &postgresPlaceholder{}
	for i, test := range testData {
		b := p.Next([]byte(test.Before))
		if string(b) != test.After {
			t.Errorf("[%d] want %q got %q", i, test.After, b)
		}
	}
}

func TestEscape(t *testing.T) {
	testData := []struct {
		Before, After string
		Buf           []byte
	}{
		{Before: `keyword`, After: `"keyword"`},
		{Before: `key"word`, After: `"key""word"`},
		{Before: `"key"word"`, After: `"""key""word"""`},
		{Before: ``, After: `""`},
		{Before: `keyword`, After: string(make([]byte, 128)) + `"keyword"`, Buf: make([]byte, 128)},
		{Before: `key"word`, After: string(make([]byte, 128)) + `"key""word"`, Buf: make([]byte, 128)},
		{Before: `"key"word"`, After: string(make([]byte, 128)) + `"""key""word"""`, Buf: make([]byte, 128)},
		{Before: ``, After: string(make([]byte, 128)) + `""`, Buf: make([]byte, 128)},
	}
	for i, test := range testData {
		b := escape(test.Buf, '"', test.Before)
		if string(b) != test.After {
			t.Errorf("[%d] want %q got %q", i, test.After, b)
		}
	}
}
