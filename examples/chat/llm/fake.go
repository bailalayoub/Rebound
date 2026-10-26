package llm

import (
	"context"
	"errors"
	"strings"
	"time"
)

// cannedAnswer is what the fake model says, word by word.
const cannedAnswer = `Resumable streams change how a chat feels on a bad network. ` +
	`The answer you are reading is produced by a generator that lives on the server, detached from ` +
	`your connection. Every word is appended to an ordered log with a sequence number. When your ` +
	`connection drops, nothing is lost: the generator keeps writing, and your browser reconnects with ` +
	`the number of the last word it saw. The server replays exactly what you missed, then follows the ` +
	`live stream, with no gap and no duplicate. Reload the page, switch from Wi-Fi to mobile data, or ` +
	`let the load balancer send you to another instance: the text simply continues. With several ` +
	`instances, the log lives in Redis, so any instance can serve any reader. Cancelling is a request ` +
	`like any other: the instance running the generator notices it at its next heartbeat and closes ` +
	`the stream with a final event. If the generator's process dies, readers notice the silence and ` +
	`close the stream themselves, so nobody waits forever. That is the whole idea: start a generation ` +
	`once, let clients come and go, and every client ends up with the same complete answer.`

// ErrPlannedFailure is returned by the fake model for prompts containing "!fail".
var ErrPlannedFailure = errors.New("fake llm: planned failure (!fail in the prompt)")

// Fake is a deterministic model: it streams cannedAnswer word by word with Delay between words.
// A prompt containing "!fail" fails halfway; one containing "!hang" never answers until cancelled.
type Fake struct {
	Delay time.Duration
	// Repeat streams the canned answer this many times (default 1), for longer demos.
	Repeat int
}

// Stream implements Provider.
func (f Fake) Stream(ctx context.Context, prompt string) (<-chan Chunk, error) {
	words := strings.SplitAfter(strings.Repeat(cannedAnswer+" ", max(f.Repeat, 1)), " ")
	words = words[:len(words)-1] // SplitAfter leaves an empty tail
	out := make(chan Chunk)
	go func() {
		defer close(out)
		if strings.Contains(prompt, "!hang") {
			<-ctx.Done()
			return
		}
		failAt := -1
		if strings.Contains(prompt, "!fail") {
			failAt = len(words) / 2
		}
		for i, w := range words {
			if i == failAt {
				send(ctx, out, Chunk{Err: ErrPlannedFailure})
				return
			}
			if i > 0 && !wait(ctx, f.Delay) {
				return
			}
			if !send(ctx, out, Chunk{Text: w}) {
				return
			}
		}
		send(ctx, out, Chunk{Usage: &Usage{PromptTokens: len(strings.Fields(prompt)), CompletionTokens: len(words), TotalTokens: len(strings.Fields(prompt)) + len(words)}})
	}()
	return out, nil
}

func send(ctx context.Context, out chan<- Chunk, c Chunk) bool {
	select {
	case out <- c:
		return true
	case <-ctx.Done():
		return false
	}
}

func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
