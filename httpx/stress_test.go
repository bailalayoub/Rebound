package httpx

// Adversarial tests of the HTTP layer: hostile Last-Event-ID values, goroutine leaks on client
// disconnects and large events. Regression tests for specific defects are in regression_test.go.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/internal/sseclient"
	"github.com/bailalayoub/rebound/internal/testutil"
)

// Hostile Last-Event-ID values, as header or query: every malformed value is 400 with a fixed body
// that does not echo the input; valid edge values resume where they say.
func TestStress_LastEventIDHostileInputs(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	id := e.start(writeWords("a", "b", "c", "d"))
	st := e.awaitTerminal(id) // LastSeq 5
	require.Equal(t, uint64(5), st.LastSeq)

	bad := []string{
		"-1", "-0", "+1", "abc", "1.5", "1e3", "0x10", "1_000", "１", "٣", "1\x00", "NaN",
		"18446744073709551615",  // max uint64: syntactically valid, beyond LastSeq
		"18446744073709551616",  // max uint64 + 1: overflow
		"99999999999999999999",  // 20 digits, overflow
		"000000000000000000001", // 21 characters
		"6",                     // LastSeq + 1
		strings.Repeat("9", 64<<10),
	}
	for _, v := range bad {
		for _, where := range []string{"header", "query"} {
			path, headers := "/streams/"+id, map[string]string{}
			if where == "header" {
				if strings.ContainsAny(v, "\x00") {
					continue // not a valid header value; Go's client refuses to send it
				}
				headers["Last-Event-ID"] = v
			} else {
				path += "?last_event_id=" + url.QueryEscape(v)
			}
			resp := e.get(context.Background(), path, headers)
			require.Equal(t, http.StatusBadRequest, resp.status, "%s %.30q", where, v)
			body, err := io.ReadAll(resp.body)
			require.NoError(t, err)
			assert.JSONEq(t, `{"error":"invalid_last_event_id"}`, string(body), "%s %.30q", where, v)
		}
	}

	good := map[string]string{"0": "1", "00000000000000000002": "3", "4": "5"}
	for v, first := range good {
		resp := e.get(context.Background(), "/streams/"+id, map[string]string{"Last-Event-ID": v})
		require.Equal(t, http.StatusOK, resp.status, v)
		evs := events(frames(t, resp))
		require.NotEmpty(t, evs)
		assert.Equal(t, first, evs[0].ID, v)
	}
	resp := e.get(context.Background(), "/streams/"+id, map[string]string{"Last-Event-ID": "5"})
	assert.Equal(t, http.StatusNoContent, resp.status)
	// Header wins, even when it is invalid and the query is valid.
	resp = e.get(context.Background(), "/streams/"+id+"?last_event_id=2", map[string]string{"Last-Event-ID": "x"})
	assert.Equal(t, http.StatusBadRequest, resp.status)
}

// 1,000 connect/disconnect cycles over real HTTP on a live stream: client goes away after the
// first event; afterwards no handler, pump or subscription goroutine is left, and the producer is
// still running.
func TestStress_NoGoroutineLeak_HTTPDisconnectCycles(t *testing.T) {
	e := newEnv(t, envConfig{})
	id := e.start(func(ctx context.Context, w rebound.Writer) error {
		if err := w.Write([]byte("first")); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	})
	testutil.Eventually(t, func() bool {
		st, err := e.hub.Status(context.Background(), id)
		require.NoError(t, err)
		return st.LastSeq == 1
	}, waitLimit)
	tr := &http.Transport{DisableKeepAlives: true}
	client := &http.Client{Transport: tr}
	before := runtime.NumGoroutine()

	for range 1000 {
		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.srv.URL+"/streams/"+id, http.NoBody)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		rd := sseclient.NewReader(resp.Body)
		for {
			f, err := rd.Next()
			require.NoError(t, err)
			if f.IsEvent() {
				require.Equal(t, "1", f.ID)
				break
			}
		}
		cancel()
		_ = resp.Body.Close() // the request was cancelled; the error is expected
	}
	tr.CloseIdleConnections()
	testutil.Eventually(t, func() bool { return e.unsubs.get() == 1000 }, waitLimit)
	testutil.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+2 }, waitLimit)
	t.Logf("goroutines: before %d, after %d", before, runtime.NumGoroutine())
	st, err := e.hub.Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, rebound.StateRunning, st.State, "the producer is unaffected by 1,000 disconnects")
}

// 64 KiB events with line breaks survive the SSE framing and a resume in the middle of them.
func TestStress_LargeEventsOverHTTP(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envConfig{})
	payload := func(i int) []byte {
		b := bytes.Repeat([]byte(fmt.Sprintf("%02d", i)), 32<<10)
		for j := 1000; j < len(b); j += 7000 {
			b[j] = '\n'
		}
		b[len(b)-1] = '\n' // trailing newline: an empty last data line
		return b
	}
	id := e.start(func(_ context.Context, w rebound.Writer) error {
		for i := range 10 {
			if err := w.Write(payload(i)); err != nil {
				return err
			}
		}
		return nil
	})
	e.awaitTerminal(id)
	resp := e.get(context.Background(), "/streams/"+id, map[string]string{"Last-Event-ID": "4"})
	require.Equal(t, http.StatusOK, resp.status)
	evs := events(frames(t, resp))
	require.Len(t, evs, 7)
	for i, f := range evs[:6] {
		assert.Equal(t, fmt.Sprint(i+5), f.ID)
		assert.Equal(t, string(payload(i+4)), f.Data, "event %s", f.ID)
	}
	assert.Equal(t, "done", evs[6].Event)
}
