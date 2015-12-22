package qutil

const (
	Year = IntervalUnit(iota)
	Month
	Day
	Hour
	Minute
	Second
)

type Interval interface {
	Value() int
	Unit() IntervalUnit
}

type expression interface {
	WriteExpression(ctx *Context, buf []byte) []byte
}

func writeIntf(x interface{}, ctx *Context, buf []byte) []byte {
	if x == nil {
		return append(buf, "NULL"...)
	}
	if v, ok := x.(expression); ok {
		return v.WriteExpression(ctx, buf)
	}
	ctx.Args = append(ctx.Args, x)
	return ctx.Placeholder.Next(buf)
}

func writeInt(buf []byte, x int) []byte {
	if x < 0 {
		return writeInt(append(buf, '-'), -x)
	}

	var b [32]byte
	i := len(b) - 1
	for x > 9 {
		b[i] = byte(x%10 + '0')
		x /= 10
		i--
	}
	b[i] = byte(x + '0')
	return append(buf, b[i:]...)
}

func (mySQL) AddInterval(ctx *Context, buf []byte, l interface{}, intervals ...Interval) []byte {
	var v int
	buf = writeIntf(l, ctx, buf)
	for _, iv := range intervals {
		v = iv.Value()
		if v == 0 {
			continue
		}
		buf = append(buf, " + INTERVAL "...)
		buf = writeInt(buf, v)
		switch iv.Unit() {
		case Year:
			buf = append(buf, " YEAR"...)
		case Month:
			buf = append(buf, " MONTH"...)
		case Day:
			buf = append(buf, " DAY"...)
		case Hour:
			buf = append(buf, " HOUR"...)
		case Minute:
			buf = append(buf, " MINUTE"...)
		case Second:
			buf = append(buf, " SECOND"...)
		default:
			panic("unsupported interval unit type: " + string(writeInt(nil, int(iv.Unit()))))
		}
	}
	return buf
}

func (postgreSQL) AddInterval(ctx *Context, buf []byte, l interface{}, intervals ...Interval) []byte {
	var v int
	buf = writeIntf(l, ctx, buf)
	for _, iv := range intervals {
		v = iv.Value()
		if v == 0 {
			continue
		}
		buf = append(buf, " + INTERVAL '"...)
		buf = writeInt(buf, v)
		switch iv.Unit() {
		case Year:
			buf = append(buf, " year"...)
		case Month:
			buf = append(buf, " month"...)
		case Day:
			buf = append(buf, " day"...)
		case Hour:
			buf = append(buf, " hour"...)
		case Minute:
			buf = append(buf, " minute"...)
		case Second:
			buf = append(buf, " second"...)
		default:
			panic("unsupported interval unit type: " + string(writeInt(nil, int(iv.Unit()))))
		}
		if v != 1 {
			buf = append(buf, 's')
		}
		buf = append(buf, '\'')
	}
	return buf
}

func (sqlite) AddInterval(ctx *Context, buf []byte, l interface{}, intervals ...Interval) []byte {
	var v int
	buf = append(buf, "DATETIME("...)
	buf = writeIntf(l, ctx, buf)
	for _, iv := range intervals {
		v = iv.Value()
		if v == 0 {
			continue
		}
		buf = append(buf, ", '"...)
		buf = writeInt(buf, v)
		switch iv.Unit() {
		case Year:
			buf = append(buf, " year"...)
		case Month:
			buf = append(buf, " month"...)
		case Day:
			buf = append(buf, " day"...)
		case Hour:
			buf = append(buf, " hour"...)
		case Minute:
			buf = append(buf, " minute"...)
		case Second:
			buf = append(buf, " second"...)
		default:
			panic("unsupported interval unit type: " + string(writeInt(nil, int(iv.Unit()))))
		}
		if v != 1 {
			buf = append(buf, 's')
		}
		buf = append(buf, '\'')
	}
	buf = append(buf, ')')
	return buf
}

func (fakeDialect) AddInterval(ctx *Context, buf []byte, l interface{}, intervals ...Interval) []byte {
	var v int
	buf = writeIntf(l, ctx, buf)
	for _, iv := range intervals {
		v = iv.Value()
		if v == 0 {
			continue
		}
		buf = append(buf, " + INTERVAL "...)
		buf = writeInt(buf, v)
		switch iv.Unit() {
		case Year:
			buf = append(buf, " YEAR"...)
		case Month:
			buf = append(buf, " MONTH"...)
		case Day:
			buf = append(buf, " DAY"...)
		case Hour:
			buf = append(buf, " HOUR"...)
		case Minute:
			buf = append(buf, " MINUTE"...)
		case Second:
			buf = append(buf, " SECOND"...)
		default:
			panic("unsupported interval unit type: " + string(writeInt(nil, int(iv.Unit()))))
		}
	}
	return buf
}
