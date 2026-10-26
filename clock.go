package rebound

import "github.com/bailalayoub/rebound/store"

// Clock is the Hub's time source, injected with WithClock for deterministic tests.
type Clock = store.Clock

// Timer is the timer returned by Clock.NewTimer.
type Timer = store.Timer

// Ticker is the ticker returned by Clock.NewTicker.
type Ticker = store.Ticker
