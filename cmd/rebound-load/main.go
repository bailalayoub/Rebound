// Command rebound-load is the load and endurance harness of docs/STRESS-v0.1.0.md. It is a tool,
// not a test: it is never part of `make check`; the Makefile targets load, soak, spike, storm,
// faults and slow run it (scripts/stress).
//
//	rebound-load run -base-url http://nginx:8080 -phases 50x1-3@3m,200x1-3@3m -out ramp.json
//	rebound-load sample -url http://127.0.0.1:6060/debug/vars -source app-1 -every 1m -out app-1.csv
//	rebound-load host-sample -containers deploy-app-1-1,deploy-redis-1 -redis deploy-redis-1 -out host.csv
//	rebound-load analyze -in app-1.csv,host.csv -series app-1:rss_bytes -from 10m
//	rebound-load chart -in app-1.csv -series app-1:rss_bytes -unit MiB -scale 9.5367431640625e-07 -out rss.svg
//
// run exits 0 when every phase passes, 1 when one fails, 2 on invalid usage or setup errors.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// commands maps each subcommand to its implementation.
var commands = map[string]func(ctx context.Context, args []string, stdout, stderr io.Writer) error{
	"run":         cmdRun,
	"sample":      cmdSample,
	"host-sample": cmdHostSample,
	"analyze":     cmdAnalyze,
	"chart":       cmdChart,
}

// errFailed means the command ran and the result is a failure (exit code 1).
var errFailed = errors.New("rebound-load: failed")

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || commands[args[0]] == nil {
		diag(stderr, "usage: rebound-load run|sample|host-sample|analyze|chart [flags] (-h for flags)")
		return 2
	}
	err := commands[args[0]](ctx, args[1:], stdout, stderr)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errFailed):
		return 1
	case errors.Is(err, flag.ErrHelp):
		return 2
	default:
		diag(stderr, "rebound-load:", err)
		return 2
	}
}

// diag writes a diagnostic line to stderr; if stderr itself fails there is nowhere left to report.
func diag(w io.Writer, args ...any) {
	_, _ = fmt.Fprintln(w, args...)
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("rebound-load "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	return nil
}
