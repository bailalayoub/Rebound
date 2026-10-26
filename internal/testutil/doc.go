// Package testutil holds test helpers shared by the module's tests: a manual Clock that makes
// timeouts, heartbeats and TTLs deterministic, and Eventually, a 10 ms polling helper for
// goroutine hand-offs that cannot be observed otherwise. It is never part of a public API.
package testutil
