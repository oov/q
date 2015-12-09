package qutil

type Dialect interface {
	Placeholder() Placeholder
	Quote(buf []byte, word string) []byte
	CharLengthName() string
}

type Placeholder interface {
	Next(buf []byte) []byte
}

func escape(buf []byte, q byte, word string) []byte {
	buf = append(buf, q)
	p := 0
	for i, c := range []byte(word) {
		if c != q {
			continue
		}
		buf = append(buf, word[p:i+1]...)
		buf = append(buf, q)
		p = i + 1
	}
	buf = append(buf, word[p:]...)
	buf = append(buf, q)
	return buf
}

var (
	// MySQL implements a dialect in MySQL.
	MySQL = Dialect(mySQL{})
	// PostgreSQL implements a dialect in PostgreSQL.
	PostgreSQL = Dialect(postgreSQL{})
	// SQLite implements a dialect in SQLite.
	SQLite = Dialect(sqlite{})
)

type mySQL struct{}

func (mySQL) String() string                       { return "MySQL" }
func (mySQL) Placeholder() Placeholder             { return genericPlaceholder{} }
func (mySQL) Quote(buf []byte, word string) []byte { return escape(buf, '`', word) }
func (mySQL) CharLengthName() string               { return "CHAR_LENGTH" }

type postgreSQL struct{}

func (postgreSQL) String() string                       { return "PostgreSQL" }
func (postgreSQL) Placeholder() Placeholder             { return &postgresPlaceholder{} }
func (postgreSQL) Quote(buf []byte, word string) []byte { return escape(buf, '"', word) }
func (postgreSQL) CharLengthName() string               { return "CHAR_LENGTH" }

type sqlite struct{}

func (sqlite) String() string                       { return "SQLite" }
func (sqlite) Placeholder() Placeholder             { return genericPlaceholder{} }
func (sqlite) Quote(buf []byte, word string) []byte { return escape(buf, '"', word) }
func (sqlite) CharLengthName() string               { return "LENGTH" }

type fakeDialect struct{}

func (fakeDialect) Quote(buf []byte, word string) []byte { return escape(buf, '"', word) }
func (fakeDialect) Placeholder() Placeholder             { return fakeDialect{} }
func (fakeDialect) Next(buf []byte) []byte               { return append(buf, '?') }
func (fakeDialect) CharLengthName() string               { return "CHAR_LENGTH" }

type genericPlaceholder struct{}

func (genericPlaceholder) Next(buf []byte) []byte { return append(buf, '?') }

type postgresPlaceholder struct {
	c int
}

func (ph *postgresPlaceholder) Next(buf []byte) []byte {
	ph.c++
	x := ph.c
	if x < 10 {
		return append(buf, '$', byte(x+'0'))
	} else if x < 100 {
		return append(buf, '$', byte(x/10+'0'), byte(x%10+'0'))
	}

	var b [32]byte
	i := len(b) - 1
	for x > 9 {
		b[i] = byte(x%10 + '0')
		x /= 10
		i--
	}
	b[i] = byte(x + '0')
	i--
	b[i] = '$'
	return append(buf, b[i:]...)
}
