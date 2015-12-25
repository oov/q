// +build ignore

package main

import (
	"bytes"
	"go/format"
	"log"
	"os"
	"strings"
	"text/template"
)

func main() {
	vars := []string{
		"Year",
		"Month",
		"Day",
		"Hour",
		"Minute",
		"Second",
	}

	funcMap := template.FuncMap{
		"ToLower": strings.ToLower,
		"ToUpper": strings.ToUpper,
	}
	t, err := template.New("").Funcs(funcMap).Parse(tpl)
	if err != nil {
		log.Fatal(err)
	}
	b := bytes.NewBufferString("")
	if err = t.Execute(b, vars); err != nil {
		log.Fatal(err)
	}
	buf, err := format.Source(b.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create("intervals.go")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(buf); err != nil {
		log.Fatal(err)
	}
}

var tpl = `package q

import "github.com/oov/q/qutil"

{{range .}}
type {{. | ToLower}}s int
func (i {{. | ToLower}}s) Value() int   { return int(i) }
func (i {{. | ToLower}}s) Unit() qutil.IntervalUnit   { return qutil.{{.}} }
// {{.}}s creates Interval such as "INTERVAL n {{. | ToUpper}}".
func {{.}}s(n int) Interval { return {{. | ToLower}}s(n) }
{{end}}
`
