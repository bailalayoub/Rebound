// Package store defines the contract between the Rebound Hub and its storage backends, and the
// vocabulary they share: events, stream status, the injectable clock and the sentinel errors a
// store returns.
//
// A Store owns, per stream, a dense log of events numbered from 1 and the stream's metadata
// (state, cancellation flag, liveness, TTLs). Appends assign sequence numbers atomically; readers
// read from the log after a cursor, which makes replay and live delivery the same operation
// (docs/adr/0001). Implementations: store/memory and store/redis. Any implementation must pass
// the reusable suite in store/storetest.
//
// These types are re-exported by package rebound (docs/adr/0002), so rebound.Event and store.Event
// are the same type.
package store
