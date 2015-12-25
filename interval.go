//go:generate go run geninterval.go

package q

import "github.com/oov/q/qutil"

// Interval represents intervals in SQL statements.
type Interval interface {
	Value() int
	Unit() qutil.IntervalUnit
}

type addIntervalFunc struct {
	V         interface{}
	Intervals []qutil.Interval
}

func (f *addIntervalFunc) String() string               { return expressionToString(f) }
func (f *addIntervalFunc) C(aliasName ...string) Column { return columnExpr(f, aliasName...) }
func (f *addIntervalFunc) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	return ctx.Dialect.AddInterval(ctx, buf, f.V, f.Intervals...)
}

// AddInterval creates Function such as "v + INTERVAL intervals[n] YEAR + ...".
func AddInterval(v interface{}, intervals ...Interval) Function {
	ivs := make([]qutil.Interval, len(intervals))
	for i, v := range intervals {
		ivs[i] = v
	}
	return &addIntervalFunc{V: v, Intervals: ivs}
}
