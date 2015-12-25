package q

import (
	"fmt"

	"github.com/oov/q/qutil"
)

type builder interface {
	write(ctx *qutil.Context, buf []byte) []byte
}

func write(b builder, d qutil.Dialect, bufCap int, argsCap int, cud bool) ([]byte, *qutil.Context) {
	if d == nil {
		d = DefaultDialect
	}
	buf, ctx := qutil.NewContext(b, bufCap, argsCap, d)
	ctx.CUD = cud
	return b.write(ctx, buf), ctx
}

func builderToSQL(b builder, d qutil.Dialect, bufCap int, argsCap int, cud bool) (string, []interface{}) {
	buf, ctx := write(b, d, bufCap, argsCap, cud)
	return string(buf), ctx.Args
}

func builderToPrepared(b builder, d qutil.Dialect, bufCap int, argsCap int, cud bool) (string, func() *ZArgsBuilder) {
	buf, ctx := write(b, d, bufCap, argsCap, cud)
	return string(buf), (&args{args: ctx.Args, argsMap: ctx.ArgsMap}).Builder
}

func builderToString(b builder, d qutil.Dialect, bufCap int, argsCap int, cud bool) string {
	buf, ctx := write(b, d, bufCap, argsCap, cud)
	return toString(buf, ctx.Args)
}

func toString(buf []byte, args []interface{}) string {
	buf = append(buf, ' ')
	buf = append(buf, fmt.Sprint(args)...)
	return string(buf)
}
