package q

import "github.com/oov/q/qutil"

var (
	// DefaultDialect is default setting of builder's dialect. When not set a Dialect in builder, this value is used.
	DefaultDialect qutil.Dialect

	// MySQL implements a dialect in MySQL.
	MySQL = qutil.MySQL
	// PostgreSQL implements a dialect in PostgreSQL.
	PostgreSQL = qutil.PostgreSQL
	// SQLite implements a dialect in SQLite.
	SQLite = qutil.SQLite
)
