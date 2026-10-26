// Package conformance implements both sides of the conformance contract of
// docs/06-conformance.md: the deterministic test producer a server exposes at
// POST /conformance/streams, and the chaos tester that drives any server implementing the
// contract (cmd/rebound-conformance is its command-line front end).
package conformance
