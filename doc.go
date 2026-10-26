// Package rebound makes Server-Sent Events streams resumable.
//
// A Hub starts a producer (for example an LLM call) in a goroutine that is detached from any HTTP
// request. The producer appends events to a Store, which numbers them densely from 1. Readers
// subscribe after a cursor — the SSE Last-Event-ID — and receive exactly the events they missed,
// then keep following live, because replay and live delivery read the same log. A client can
// disconnect, reload, or reconnect to another server instance (with the Redis store) and still
// end up with the complete, ordered sequence of events.
//
// Guarantees (docs/02-spec.md §2): ordering, no gap and no duplicate, producer independence from
// connections, instance independence, guaranteed termination, bounded resources and an idempotent
// terminal event.
//
// HTTP handlers live in package httpx (net/http) and ginx (Gin); storage backends in store/memory
// and store/redis.
package rebound
