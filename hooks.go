package rebound

import "time"

// Hooks are optional callbacks for metrics. They run synchronously on the Hub's goroutines and
// must return quickly; a panicking hook is recovered and logged. Nil fields are skipped.
type Hooks struct {
	// OnStreamStart runs when Start has created a stream.
	OnStreamStart func(streamID string)
	// OnStreamEnd runs when a local producer's stream ended. reason is the stored terminal: "done"
	// or its error code (spec §5.1), also when another writer stored it, or "closed_elsewhere"
	// when it cannot be read back (the stream expired).
	OnStreamEnd func(streamID, reason string, duration time.Duration)
	// OnSubscribe runs when a Subscription is created.
	OnSubscribe func(streamID string, afterSeq uint64)
	// OnUnsubscribe runs when a Subscription is closed.
	OnUnsubscribe func(streamID string)
	// OnEvent runs after a producer event is stored.
	OnEvent func(streamID string, seq uint64, kind string)
}

// callHook runs fn, recovering from a panic in user code: library code never panics.
func (h *Hub) callHook(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			h.cfg.logger.Error("rebound: hook panicked", "hook", name, "panic", r)
		}
	}()
	fn()
}

func (h *Hub) hookStreamStart(id string) {
	if f := h.cfg.hooks.OnStreamStart; f != nil {
		h.callHook("OnStreamStart", func() { f(id) })
	}
}

func (h *Hub) hookStreamEnd(id, reason string, d time.Duration) {
	if f := h.cfg.hooks.OnStreamEnd; f != nil {
		h.callHook("OnStreamEnd", func() { f(id, reason, d) })
	}
}

func (h *Hub) hookSubscribe(id string, after uint64) {
	if f := h.cfg.hooks.OnSubscribe; f != nil {
		h.callHook("OnSubscribe", func() { f(id, after) })
	}
}

func (h *Hub) hookUnsubscribe(id string) {
	if f := h.cfg.hooks.OnUnsubscribe; f != nil {
		h.callHook("OnUnsubscribe", func() { f(id) })
	}
}

func (h *Hub) hookEvent(id string, seq uint64, kind string) {
	if f := h.cfg.hooks.OnEvent; f != nil {
		h.callHook("OnEvent", func() { f(id, seq, kind) })
	}
}
