package qutil

type Context struct {
	Starter     interface{}
	Dialect     Dialect
	Placeholder Placeholder
	Args        []interface{}
}

type Dialect interface {
	Placeholder() Placeholder
	Quote(buf []byte, word string) []byte
	CharLengthName() string
}

type Placeholder interface {
	Next(buf []byte) []byte
}

func NewContext(starter interface{}, bufCap int, argsCap int, d Dialect) ([]byte, *Context) {
	if d == nil {
		d = fakeDialect{}
	}
	return make([]byte, 0, bufCap), &Context{
		Starter:     starter,
		Dialect:     d,
		Placeholder: d.Placeholder(),
		Args:        make([]interface{}, 0, argsCap),
	}
}
