// Package core implements package rebound: the Hub, its producers and its subscriptions.
//
// Package rebound, at the module root, is a thin facade over this package so that the root of the
// repository holds only the public API (ADR 0009). Its doc comments are the reference for users;
// the code, the invariants and the unit tests live here.
//
// Guarantees (docs/02-spec.md §2): ordering, no gap and no duplicate, producer independence from
// connections, instance independence, guaranteed termination, bounded resources and an idempotent
// terminal event.
package core
