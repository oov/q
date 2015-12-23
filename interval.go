package q

import "github.com/oov/q/qutil"

// Interval represents intervals in SQL statements.
type Interval interface {
	Value() int
	Unit() qutil.IntervalUnit
}

type (
	years   int
	months  int
	days    int
	hours   int
	minutes int
	seconds int
)

func (i years) Value() int   { return int(i) }
func (i months) Value() int  { return int(i) }
func (i days) Value() int    { return int(i) }
func (i hours) Value() int   { return int(i) }
func (i minutes) Value() int { return int(i) }
func (i seconds) Value() int { return int(i) }

func (i years) Unit() qutil.IntervalUnit   { return qutil.Year }
func (i months) Unit() qutil.IntervalUnit  { return qutil.Month }
func (i days) Unit() qutil.IntervalUnit    { return qutil.Day }
func (i hours) Unit() qutil.IntervalUnit   { return qutil.Hour }
func (i minutes) Unit() qutil.IntervalUnit { return qutil.Minute }
func (i seconds) Unit() qutil.IntervalUnit { return qutil.Second }

// Years creates Interval such as "INTERVAL n YEAR".
func Years(n int) Interval { return years(n) }

// Months creates Interval such as "INTERVAL n MONTH".
func Months(n int) Interval { return months(n) }

// Days creates Interval such as "INTERVAL n DAY".
func Days(n int) Interval { return days(n) }

// Hours creates Interval such as "INTERVAL n HOUR".
func Hours(n int) Interval { return hours(n) }

// Minutes creates Interval such as "INTERVAL n MINUTE".
func Minutes(n int) Interval { return minutes(n) }

// Seconds creates Interval such as "INTERVAL n SECOND".
func Seconds(n int) Interval { return seconds(n) }

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
