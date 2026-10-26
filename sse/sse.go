package sse

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/bailalayoub/rebound/store"
)

// maxLastEventIDLen is the longest accepted cursor: 20 digits hold any uint64.
const maxLastEventIDLen = 20

var (
	// ErrInvalidLastEventID reports a cursor that is not a decimal uint64 of at most 20 digits.
	ErrInvalidLastEventID = errors.New("sse: invalid Last-Event-ID")
	// ErrInvalidField reports an event name or comment containing a line break, which would
	// corrupt the framing of the stream.
	ErrInvalidField = errors.New("sse: field contains a line break")
)

// WriteEvent writes ev as one SSE event: "id: <seq>", "event: <kind>", one "data:" line per line
// of ev.Data, then a blank line. Data lines are split on CRLF, LF and CR, as SSE parsers do, so a
// client joining them with LF gets the payload back (a CR becomes LF). The event is written with a
// single Write call.
func WriteEvent(w io.Writer, ev store.Event) error {
	if strings.ContainsAny(ev.Kind, "\r\n") {
		return fmt.Errorf("%w: event %q", ErrInvalidField, ev.Kind)
	}
	var b bytes.Buffer
	b.Grow(len(ev.Data) + len(ev.Kind) + 40)
	b.WriteString("id: ")
	b.WriteString(strconv.FormatUint(ev.Seq, 10))
	b.WriteString("\nevent: ")
	b.WriteString(ev.Kind)
	b.WriteByte('\n')
	for _, line := range splitLines(ev.Data) {
		if len(line) == 0 {
			b.WriteString("data:\n")
			continue
		}
		b.WriteString("data: ")
		b.Write(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return write(w, b.Bytes())
}

// WriteComment writes ": <text>" and a blank line. Comments carry no id and do not advance the
// cursor of the client; Rebound uses them as keep-alive pings.
func WriteComment(w io.Writer, text string) error {
	if strings.ContainsAny(text, "\r\n") {
		return fmt.Errorf("%w: comment", ErrInvalidField)
	}
	return write(w, []byte(": "+text+"\n\n"))
}

// WriteRetry writes the reconnection delay field "retry: <milliseconds>" and a blank line.
// Negative delays are written as 0.
func WriteRetry(w io.Writer, d time.Duration) error {
	return write(w, []byte("retry: "+strconv.FormatInt(max(d.Milliseconds(), 0), 10)+"\n\n"))
}

// ParseLastEventID returns the resume cursor from the Last-Event-ID header value or, when the
// header is empty, from the last_event_id query value. Both empty means 0 (from the beginning).
// The value must be 1-20 ASCII digits fitting a uint64; anything else is ErrInvalidLastEventID.
func ParseLastEventID(header, query string) (uint64, error) {
	raw := header
	if raw == "" {
		raw = query
	}
	if raw == "" {
		return 0, nil
	}
	if len(raw) > maxLastEventIDLen {
		return 0, fmt.Errorf("%w: longer than %d characters", ErrInvalidLastEventID, maxLastEventIDLen)
	}
	for i := range len(raw) {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, fmt.Errorf("%w: %q is not a non-negative decimal integer", ErrInvalidLastEventID, raw)
		}
	}
	seq, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q overflows uint64", ErrInvalidLastEventID, raw)
	}
	return seq, nil
}

// splitLines splits data on CRLF, LF and CR. Empty data is one empty line, so that the event still
// has a data field and is dispatched by clients.
func splitLines(data []byte) [][]byte {
	var lines [][]byte
	for {
		i := bytes.IndexAny(data, "\r\n")
		if i < 0 {
			return append(lines, data)
		}
		lines = append(lines, data[:i])
		if data[i] == '\r' && i+1 < len(data) && data[i+1] == '\n' {
			i++
		}
		data = data[i+1:]
	}
}

func write(w io.Writer, p []byte) error {
	if _, err := w.Write(p); err != nil {
		return fmt.Errorf("sse: write: %w", err)
	}
	return nil
}
