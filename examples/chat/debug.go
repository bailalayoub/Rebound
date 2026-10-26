package main

import (
	"expvar"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// newDebugServer returns the opt-in diagnostics listener (DEBUG_ADDR): pprof under /debug/pprof/
// and expvar under /debug/vars (see publishProcessVars). It refuses any address but a loopback
// one: profiles expose internals and the demo has no authentication.
func newDebugServer(addr string) (*http.Server, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("chat: DEBUG_ADDR %q: %w", addr, err)
	}
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("chat: DEBUG_ADDR %q must be a loopback address, such as 127.0.0.1:6060", addr)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /debug/vars", expvar.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}, nil
}

// publishProcessVars adds goroutines, rss_bytes and open_fds to the expvar memstats. Call it once
// per process: expvar panics on a second registration.
func publishProcessVars() {
	expvar.Publish("goroutines", expvar.Func(func() any { return runtime.NumGoroutine() }))
	expvar.Publish("rss_bytes", expvar.Func(func() any { return residentBytes() }))
	expvar.Publish("open_fds", expvar.Func(func() any { return openFDs() }))
}

// residentBytes reads the resident set size from /proc (Linux); -1 elsewhere.
func residentBytes() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return -1
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return -1
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return -1
	}
	return pages * int64(os.Getpagesize())
}

// openFDs counts the entries of /proc/self/fd (Linux); -1 elsewhere.
func openFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}
