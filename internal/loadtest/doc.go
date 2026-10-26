// Package loadtest is the load and endurance harness of docs/STRESS-v0.1.0.md: a timestamped test
// producer for the example server (server side), and the client that drives concurrent streams,
// checks every event a reader receives and measures latencies (client side). It uses the standard
// library only.
package loadtest
