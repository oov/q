package q

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/oov/dockertest"
	"github.com/oov/q/qutil"
)

type Tester func(func(*sql.DB, qutil.Dialect)) error

func mySQLTest(f func(*sql.DB, qutil.Dialect)) error {
	const (
		User     = "username"
		Password = "password"
		DBName   = "qdb"
	)
	c, err := dockertest.New(dockertest.Config{
		Image: "mysql", // or "mysql:latest"
		Name:  "q-mysql",
		PortMapping: map[string]string{
			"3306/tcp": "auto",
		},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": Password,
			"MYSQL_DATABASE":      DBName,
			"MYSQL_USER":          User,
			"MYSQL_PASSWORD":      Password,
		},
		StopOnClose: true,
	})
	if err != nil {
		return err
	}
	defer c.Close()

	// wait until the container has started listening
	if err = c.Wait(nil); err != nil {
		return err
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4",
		User, Password, c.Mapped["3306/tcp"], DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	f(db, MySQL)
	return nil
}

func postgreSQLTest(f func(*sql.DB, qutil.Dialect)) error {
	const (
		User     = "username"
		Password = "mypassword"
		DBName   = User
	)
	c, err := dockertest.New(dockertest.Config{
		Image: "postgres",
		Name:  "q-postgres",
		PortMapping: map[string]string{
			"5432/tcp": "auto",
		},
		Env: map[string]string{
			"POSTGRES_USER":     User,
			"POSTGRES_PASSWORD": Password,
		},
		StopOnClose: true,
	})
	if err != nil {
		return err
	}
	defer c.Close()

	// wait until the container has started listening
	if err = c.Wait(nil); err != nil {
		return err
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		User, Password, c.Mapped["5432/tcp"], DBName)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

ping:
	if err = db.Ping(); err != nil {
		// Sometimes fails with this error, so we need ignore and retry later.
		if err.Error() == "pq: the database system is starting up" {
			time.Sleep(50 * time.Millisecond)
			goto ping
		}
		return err
	}

	f(db, PostgreSQL)
	return nil
}

func sqliteTest(f func(*sql.DB, qutil.Dialect)) error {
	db, err := sql.Open("sqlite3", ":memory:?_loc=auto")
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err = db.Exec("PRAGMA foreign_keys = ON;"); err != nil {
		return err
	}

	f(db, SQLite)
	return nil
}

func TestMySQLTester(t *testing.T) {
	err := mySQLTest(func(*sql.DB, qutil.Dialect) {})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLTester(t *testing.T) {
	err := postgreSQLTest(func(*sql.DB, qutil.Dialect) {})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteTester(t *testing.T) {
	err := sqliteTest(func(*sql.DB, qutil.Dialect) {})
	if err != nil {
		t.Fatal(err)
	}
}
