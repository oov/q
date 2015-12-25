package qutil

type Context struct {
	Starter     interface{}
	CUD         bool // Whether current context is Create or Update or Delete.
	Dialect     Dialect
	Placeholder Placeholder
	Args        []interface{}
	ArgsMap     map[interface{}]int
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
		ArgsMap:     make(map[interface{}]int),
	}
}
