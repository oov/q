package q

import "github.com/oov/q/qutil"

// ZCaseBuilder implements a CASE expression builder.
// This also implements Expression interface, so it can use in many place.
type ZCaseBuilder struct {
	Base     Expression
	WhenThen [][2]Expression
	ElseThen Expression
}

// Case creates ZCaseBuilder.
// If omitting an argument, it'll be a searched CASE builder.
//	Simple CASE:
//		CASE base WHEN 0 THEN 'false' THEN 1 THEN 'true' END
//	Searched CASE:
//		CASE WHEN base = 0 THEN 'false' THEN base = 1 THEN 'true' END
func Case(base ...Expression) *ZCaseBuilder {
	if len(base) > 0 {
		return &ZCaseBuilder{Base: base[0]}
	}
	return &ZCaseBuilder{}
}

// When adds "WHEN cond THEN then" to the builder.
func (b *ZCaseBuilder) When(cond, then interface{}) *ZCaseBuilder {
	b.WhenThen = append(b.WhenThen, [2]Expression{
		interfaceToExpression(cond),
		interfaceToExpression(then),
	})
	return b
}

// Else sets "ELSE then" to the builder.
func (b *ZCaseBuilder) Else(then interface{}) *ZCaseBuilder {
	b.ElseThen = interfaceToExpression(then)
	return b
}

// C implements Expression interface.
func (b *ZCaseBuilder) C(aliasName ...string) Column {
	return columnExpr(b, aliasName...)
}

// WriteExpression implements Expression interface.
func (b *ZCaseBuilder) WriteExpression(ctx *qutil.Context, buf []byte) []byte {
	if len(b.WhenThen) == 0 {
		// If valid CASE expression can't be generated,
		// then returns a result of ELSE clause.
		if b.ElseThen != nil {
			return b.ElseThen.WriteExpression(ctx, buf)
		}
		return append(buf, "NULL"...)
	}

	buf = append(buf, "CASE"...)
	if b.Base != nil {
		buf = append(buf, ' ')
		buf = b.Base.WriteExpression(ctx, buf)
	}
	for _, wt := range b.WhenThen {
		buf = append(buf, " WHEN "...)
		buf = wt[0].WriteExpression(ctx, buf)
		buf = append(buf, " THEN "...)
		buf = wt[1].WriteExpression(ctx, buf)
	}
	if b.ElseThen != nil {
		buf = append(buf, " ELSE "...)
		buf = b.ElseThen.WriteExpression(ctx, buf)
	}
	return append(buf, " END"...)
}

// String implements fmt.Stringer interface.
func (b *ZCaseBuilder) String() string {
	return expressionToString(b)
}
