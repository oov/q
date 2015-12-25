package q

type args struct {
	args    []interface{}
	argsMap map[interface{}]int
}

func (a *args) Builder() *ZArgsBuilder {
	r := &ZArgsBuilder{
		parent: a,
		Args:   make([]interface{}, len(a.args)),
	}
	copy(r.Args, a.args)
	return r
}

// ZArgsBuilder is query arguments builder.
type ZArgsBuilder struct {
	parent *args
	Args   []interface{}
}

// Set sets the index entries associated with key to the single element value.
func (b *ZArgsBuilder) Set(key, value interface{}) {
	b.Args[b.parent.argsMap[key]] = value
}
