package q

type ctx struct {
	Dialect     dialect
	Placeholder placeholder
	Args        []interface{}
}

func newCtx(bufCap int, argsCap int, d dialect) ([]byte, *ctx) {
	return make([]byte, 0, bufCap), &ctx{
		Dialect:     d,
		Placeholder: d.Placeholder(),
		Args:        make([]interface{}, 0, argsCap),
	}
}

func newDummyCtx(bufCap int, argsCap int) ([]byte, *ctx) {
	return newCtx(bufCap, argsCap, fakeDialect{})
}
