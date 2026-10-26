package store

// Event kinds with a meaning to the store. Custom kinds are validated by the Hub, not here.
const (
	// KindData is the default kind of producer events.
	KindData = "data"
	// KindDone is the terminal kind of a stream that completed normally.
	KindDone = "done"
	// KindError is the terminal kind of a stream that failed, was cancelled or timed out.
	KindError = "error"
)

// Event is one entry of a stream's log.
type Event struct {
	// Seq is dense within a stream: 1, 2, 3, … assigned by the store at append time.
	Seq uint64
	// Kind is "data", a custom kind, or a terminal kind ("done", "error").
	Kind string
	// Data is the opaque payload chosen by the producer.
	Data []byte
}

// IsTerminal reports whether kind ends a stream ("done" or "error").
func IsTerminal(kind string) bool {
	return kind == KindDone || kind == KindError
}
