package testutil_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/bailalayoub/rebound/internal/testutil"
)

// recordingTB captures Fatalf instead of stopping the test, to exercise the failure path.
type recordingTB struct {
	testing.TB
	failed atomic.Bool
	msg    atomic.Value
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.failed.Store(true)
	r.msg.Store(fmt.Sprintf(format, args...))
}

func TestEventually_ImmediateAndLater(t *testing.T) {
	t.Parallel()
	assert.True(t, testutil.Eventually(t, func() bool { return true }, time.Second))

	var calls atomic.Int32
	ok := testutil.Eventually(t, func() bool { return calls.Add(1) >= 3 }, 5*time.Second)
	assert.True(t, ok)
	assert.Equal(t, int32(3), calls.Load())
}

func TestEventually_TimesOut(t *testing.T) {
	t.Parallel()
	rec := &recordingTB{TB: t}
	ok := testutil.Eventually(rec, func() bool { return false }, 30*time.Millisecond)
	assert.False(t, ok)
	assert.True(t, rec.failed.Load())
	assert.Contains(t, rec.msg.Load(), "condition not met within 30ms")
}
