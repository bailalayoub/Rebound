package store

import "time"

// State is the lifecycle state of a stream.
type State string

// Stream states. A stream starts running and ends in exactly one terminal state.
const (
	StateRunning State = "running"
	StateDone    State = "done"
	StateError   State = "error"
)

// IsTerminal reports whether s is a final state (done or error).
func (s State) IsTerminal() bool {
	return s == StateDone || s == StateError
}

// Status is a consistent snapshot of a stream's metadata.
type Status struct {
	ID        string
	State     State
	ErrorCode string // set when State == StateError
	// FirstSeq is the lowest seq still retained (≥ 1), or 0 when the log is empty.
	FirstSeq  uint64
	LastSeq   uint64
	Cancelled bool
	CreatedAt time.Time
	// LastActivity is the time of the last append or heartbeat touch.
	LastActivity time.Time
	Meta         map[string]string
}
