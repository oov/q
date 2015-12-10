package q

import (
	"fmt"

	"github.com/oov/q/qutil"
)

type builder interface {
	write(ctx *qutil.Context, buf []byte) []byte
}

func write(b builder, d qutil.Dialect, bufCap int, argsCap int, cud bool) ([]byte, []interface{}) {
	if d == nil {
		d = DefaultDialect
	}
	buf, ctx := qutil.NewContext(b, bufCap, argsCap, d)
	ctx.CUD = cud
	buf = b.write(ctx, buf)
	return buf, ctx.Args
}

func builderToSQL(b builder, d qutil.Dialect, bufCap int, argsCap int, cud bool) (string, []interface{}) {
	buf, args := write(b, d, bufCap, argsCap, cud)
	return string(buf), args
}

func builderToString(b builder, d qutil.Dialect, bufCap int, argsCap int, cud bool) string {
	buf, args := write(b, d, bufCap, argsCap, cud)
	return toString(buf, args)
}

func toString(buf []byte, args []interface{}) string {
	buf = append(buf, ' ')
	buf = append(buf, fmt.Sprint(args)...)
	return string(buf)
}
