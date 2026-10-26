// Package sse encodes Server-Sent Events on the wire (events with id, event and data lines,
// comments, the retry field) and parses the Last-Event-ID cursor sent by reconnecting clients.
package sse
