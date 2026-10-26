// Package memory is an in-process store.Store: a mutex-guarded map of streams, each holding its
// event slice (capped at MaxEvents, never trimmed) and a broadcast channel that wakes blocked
// readers on every append. TTLs are enforced on the injected clock. Use it for development, tests and single-instance deployments.
package memory
