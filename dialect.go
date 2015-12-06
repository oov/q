package q

type dialect interface {
	Placeholder() placeholder
	Quote(buf []byte, word string) []byte
	CharLengthName() string
}

type placeholder interface {
	Next() string
}

var (
	// MySQL implements a dialect in MySQL.
	MySQL = dialect(mySQL{})
	// PostgreSQL implements a dialect in PostgreSQL.
	PostgreSQL = dialect(postgreSQL{})
	// SQLite implements a dialect in SQLite.
	SQLite = dialect(sqlite{})
)

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

type mySQL struct{}

func (mySQL) String() string { return "MySQL" }

func (mySQL) Placeholder() placeholder {
	return &genericPlaceholder{}
}

func (mySQL) Quote(buf []byte, word string) []byte {
	return escape(buf, '`', word)
}

func (mySQL) CharLengthName() string {
	return "CHAR_LENGTH"
}

type postgreSQL struct{}

func (postgreSQL) String() string { return "PostgreSQL" }

func (postgreSQL) Placeholder() placeholder {
	return &postgresPlaceholder{}
}

func (postgreSQL) Quote(buf []byte, word string) []byte {
	return escape(buf, '"', word)
}

func (postgreSQL) CharLengthName() string {
	return "CHAR_LENGTH"
}

type sqlite struct{}

func (sqlite) String() string { return "SQLite" }

func (sqlite) Placeholder() placeholder {
	return &genericPlaceholder{}
}

func (sqlite) Quote(buf []byte, word string) []byte {
	return escape(buf, '"', word)
}

func (sqlite) CharLengthName() string {
	return "LENGTH"
}

type fakeDialect struct{}

func (fakeDialect) Quote(buf []byte, word string) []byte { return escape(buf, '"', word) }
func (fakeDialect) Placeholder() placeholder             { return fakeDialect{} }
func (fakeDialect) Next() string                         { return "?" }
func (fakeDialect) CharLengthName() string               { return "CHAR_LENGTH" }

type genericPlaceholder struct{}

func (*genericPlaceholder) Next() string { return "?" }

type postgresPlaceholder struct {
	c int
}

func (ph *postgresPlaceholder) Next() string {
	ph.c++

	var buf [32]byte
	x := ph.c
	i := len(buf) - 1
	for x > 9 {
		buf[i] = byte(x%10 + '0')
		x /= 10
		i--
	}
	buf[i] = byte(x + '0')
	i--
	buf[i] = '$'
	return string(buf[i:])
}
