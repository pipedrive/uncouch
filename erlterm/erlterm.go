package erlterm

// TermType is Erlanf data type tag used in serialisation
type TermType byte

// Term is structure to hold de-serialise Erlang term
type Term struct {
	Term         TermType
	IntegerValue int64
	FloatValue   float64
	Binary       []byte
}
