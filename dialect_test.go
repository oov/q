package q

import "testing"

func TestPostgresPlaceholder(t *testing.T) {
	testData := []struct {
		Before []byte
		After  string
		C      int
	}{
		{Before: nil, After: `$1`, C: 0},
		{Before: []byte(`keyword`), After: `keyword$11`, C: 10},
		{Before: make([]byte, 0, 32), After: `$101`, C: 100},
		{Before: make([]byte, 0, 32), After: `$1001`, C: 1000},
	}
	p := &postgresPlaceholder{}
	for i, test := range testData {
		p.c = test.C
		b := p.Next([]byte(test.Before))
		if string(b) != test.After {
			t.Errorf("[%d] want %q got %q", i, test.After, b)
		}
	}
}

func phBench(c int, buf []byte, b *testing.B) {
	p := &postgresPlaceholder{}
	for i := 0; i < b.N; i++ {
		p.c = c
		p.Next(buf)
	}
}

func BenchmarkPostgresPlaceholder(b *testing.B)      { phBench(1, nil, b) }
func BenchmarkPostgresPlaceholder10(b *testing.B)    { phBench(10, nil, b) }
func BenchmarkPostgresPlaceholder100(b *testing.B)   { phBench(100, nil, b) }
func BenchmarkPostgresPlaceholder1000(b *testing.B)  { phBench(1000, nil, b) }
func BenchmarkPostgresPlaceholder10000(b *testing.B) { phBench(10000, nil, b) }

func BenchmarkPostgresPlaceholderCap(b *testing.B)      { phBench(1, make([]byte, 0, 32), b) }
func BenchmarkPostgresPlaceholderCap10(b *testing.B)    { phBench(10, make([]byte, 0, 32), b) }
func BenchmarkPostgresPlaceholderCap100(b *testing.B)   { phBench(100, make([]byte, 0, 32), b) }
func BenchmarkPostgresPlaceholderCap1000(b *testing.B)  { phBench(1000, make([]byte, 0, 32), b) }
func BenchmarkPostgresPlaceholderCap10000(b *testing.B) { phBench(10000, make([]byte, 0, 32), b) }

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
