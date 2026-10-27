package loadtest

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/bailalayoub/rebound"
)

// Tally is what one reader observed of one stream across all its connections, checked event by
// event against spec §2: dense increasing seqs (G1, G2), exactly one terminal event (G7), and
// payloads that match their seq and come from one producer run (G3, G4).
type Tally struct {
	// Last is the highest seq received, events after a gap included.
	Last uint64 `json:"last"`
	// Events counts the non-terminal events accepted in order.
	Events int `json:"events"`
	// Gaps counts the seqs skipped; Dups the events received again at or below Last.
	Gaps int `json:"gaps"`
	Dups int `json:"dups"`
	// Terminals counts terminal events; after the first, any event counts in AfterTerminal.
	Terminals     int    `json:"terminals"`
	AfterTerminal int    `json:"after_terminal"`
	TerminalSeq   uint64 `json:"terminal_seq,omitempty"`
	// TerminalCode is "done" or the code of the error event.
	TerminalCode string `json:"terminal_code,omitempty"`
	// BadPayload counts data events whose payload does not decode or names another seq.
	BadPayload int `json:"bad_payload"`
	// Host and Nonce are those of the first data event; Moved and Restarted count later events
	// written by another instance or another producer run.
	Host      string `json:"host,omitempty"`
	Nonce     string `json:"nonce,omitempty"`
	Moved     int    `json:"moved"`
	Restarted int    `json:"restarted"`
	// hashes[k] is the running hash of the first k events (terminal included) received in order.
	hashes []uint64
}

// Observe checks one received event and returns its payload when it is a decodable data event.
func (t *Tally) Observe(ev rebound.Event) (Payload, bool) {
	if t.Terminals > 0 {
		t.AfterTerminal++
		return Payload{}, false
	}
	switch expect := t.Last + 1; {
	case ev.Seq < expect:
		t.Dups++
		return Payload{}, false
	case ev.Seq > expect:
		t.Gaps += int(ev.Seq - expect) //nolint:gosec // bounded by the seqs of one stream
	}
	t.Last = ev.Seq
	t.extend(ev)
	if rebound.IsTerminal(ev.Kind) {
		t.Terminals++
		t.TerminalSeq, t.TerminalCode = ev.Seq, TerminalCode(ev)
		return Payload{}, false
	}
	t.Events++
	if ev.Kind != "data" {
		t.BadPayload++
		return Payload{}, false
	}
	var p Payload
	if err := json.Unmarshal(ev.Data, &p); err != nil || p.I != ev.Seq {
		t.BadPayload++
		return Payload{}, false
	}
	switch {
	case t.Host == "":
		t.Host, t.Nonce = p.H, p.N
	case p.H != t.Host:
		t.Moved++
	case p.N != t.Nonce:
		t.Restarted++
	}
	return p, true
}

// extend appends ev to the running hash of the in-order events.
func (t *Tally) extend(ev rebound.Event) {
	if t.hashes == nil {
		t.hashes = []uint64{fnv.New64a().Sum64()}
	}
	t.hashes = append(t.hashes, hashEvent(t.hashes[len(t.hashes)-1], ev))
}

func hashEvent(prev uint64, ev rebound.Event) uint64 {
	h := fnv.New64a()
	var b [16]byte
	binary.LittleEndian.PutUint64(b[:8], prev)
	binary.LittleEndian.PutUint64(b[8:], ev.Seq)
	_, _ = h.Write(b[:])            // hash writes never fail
	_, _ = h.Write([]byte(ev.Kind)) // hash writes never fail
	_, _ = h.Write([]byte{0})       // hash writes never fail
	_, _ = h.Write(ev.Data)         // hash writes never fail
	return h.Sum64()
}

// Integrity returns the violations of the guarantees: an empty result means every event arrived
// once, in order, from one producer run, and at most one terminal event was seen.
func (t *Tally) Integrity() []string {
	var out []string
	add := func(n int, what string) {
		if n > 0 {
			out = append(out, fmt.Sprintf("%d %s", n, what))
		}
	}
	add(t.Gaps, "gap")
	add(t.Dups, "duplicate")
	add(t.Terminals-1, "extra terminal")
	add(t.AfterTerminal, "event after the terminal")
	add(t.BadPayload, "bad payload")
	add(t.Moved, "event from another instance")
	add(t.Restarted, "event from another producer run")
	return out
}

// Problems returns Integrity plus a missing terminal event, and, when want > 0 and the stream
// ended with done, a wrong number of events.
func (t *Tally) Problems(want int) []string {
	out := t.Integrity()
	if t.Terminals == 0 {
		out = append(out, "missing terminal")
	}
	if want > 0 && t.TerminalCode == "done" && (t.Events != want || t.TerminalSeq != uint64(want)+1) { //nolint:gosec // want > 0
		out = append(out, fmt.Sprintf("done after %d events at seq %d, want %d events", t.Events, t.TerminalSeq, want))
	}
	return out
}

// IsPrefixOf reports whether the events this reader received in order are the first events of
// the log audit read: what a reader got must be a prefix of what was written.
func (t *Tally) IsPrefixOf(audit *Tally) bool {
	k := len(t.hashes)
	if k == 0 {
		return true
	}
	return k <= len(audit.hashes) && audit.hashes[k-1] == t.hashes[k-1]
}

// TerminalCode returns "done", or the code of an error event ("error(?)" when it has none).
func TerminalCode(ev rebound.Event) string {
	if ev.Kind == "done" {
		return "done"
	}
	var p struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(ev.Data, &p); err != nil || p.Code == "" {
		return "error(?)"
	}
	return p.Code
}

// Problem classes, the keys of Counts.
const (
	ClassGap            = "gap"
	ClassDuplicate      = "duplicate"
	ClassMissing        = "missing_terminal"
	ClassExtra          = "extra_terminal"
	ClassAfterTerminal  = "after_terminal"
	ClassBadPayload     = "bad_payload"
	ClassMoved          = "moved"
	ClassRestarted      = "restarted"
	ClassIncomplete     = "incomplete_done"
	ClassNotPrefix      = "not_prefix"
	ClassTerminalDiffer = "terminal_differs"
)

// Counts adds this tally's violations, by class, to c.
func (t *Tally) Counts(c map[string]int, want int) {
	add := func(class string, n int) {
		if n > 0 {
			c[class] += n
		}
	}
	add(ClassGap, t.Gaps)
	add(ClassDuplicate, t.Dups)
	add(ClassAfterTerminal, t.AfterTerminal)
	add(ClassBadPayload, t.BadPayload)
	add(ClassMoved, t.Moved)
	add(ClassRestarted, t.Restarted)
	if t.Terminals == 0 {
		add(ClassMissing, 1)
	}
	add(ClassExtra, t.Terminals-1)
	for _, p := range t.Problems(want) {
		if strings.HasPrefix(p, "done after") {
			add(ClassIncomplete, 1)
		}
	}
}
