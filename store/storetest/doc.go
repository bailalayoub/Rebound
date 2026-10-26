// Package storetest is the reusable conformance suite for store.Store implementations. A backend
// passes when storetest.Run succeeds against it: dense atomic sequence numbers, blocking reads
// from the log, a single terminal event under concurrent writers, cancellation, trimming with
// ErrGap and TTL expiry (docs/02-spec.md §6).
package storetest
