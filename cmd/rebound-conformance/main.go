// Command rebound-conformance is the black-box chaos tester of docs/06-conformance.md. It starts
// deterministic streams on a server that implements the conformance contract, drops and resumes
// connections at random, and checks that every reader gets exactly the events the producer wrote.
//
//	rebound-conformance --base-url http://localhost:8080 --runs 200 --drop-rate 0.3
//
// The seed is printed first; re-running with --seed replays the same drops. The exit code is 0
// when every scenario passes, 1 when one fails, 2 on invalid usage or setup errors.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bailalayoub/rebound/internal/conformance"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// options are the parsed command line.
type options struct {
	cfg    conformance.Config
	asJSON bool
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	cfg := conformance.DefaultConfig()
	fs := flag.NewFlagSet("rebound-conformance", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.BaseURL, "base-url", "http://localhost:8080", "server implementing the conformance contract")
	fs.IntVar(&cfg.Runs, "runs", cfg.Runs, "runs of S2 (random drops + resume)")
	fs.IntVar(&cfg.OtherRuns, "other-runs", cfg.OtherRuns, "runs of every other scenario")
	fs.IntVar(&cfg.Parallel, "parallel", cfg.Parallel, "runs in flight at once, and streams at once in S10")
	fs.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "readers per stream in S6")
	fs.Float64Var(&cfg.DropRate, "drop-rate", cfg.DropRate, "probability of dropping the connection after each event")
	fs.IntVar(&cfg.TextSize, "text-size", cfg.TextSize, "characters of the generated text")
	fs.IntVar(&cfg.Chunk, "chunk", cfg.Chunk, "characters per event")
	fs.IntVar(&cfg.DelayMS, "delay-ms", cfg.DelayMS, "pause between events, in ms")
	fs.DurationVar(&cfg.MaxReconnectDelay, "max-reconnect-delay", cfg.MaxReconnectDelay, "reconnect after a random delay up to this")
	fs.DurationVar(&cfg.CancelWithin, "cancel-within", cfg.CancelWithin, "S4: the terminal event must follow the cancel within this")
	fs.Uint64Var(&cfg.Seed, "seed", 0, "seed of every random decision; 0 picks one (always printed)")
	fs.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "timeout of one run")
	fs.BoolVar(&cfg.Trace, "trace", false, "include every run's connections in the JSON report")
	scenarios := fs.String("scenarios", "", "comma-separated scenario IDs (default: all, S1-S12)")
	asJSON := fs.Bool("json", false, "print a machine-readable JSON report")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *scenarios != "" {
		cfg.Scenarios = strings.Split(*scenarios, ",")
	}
	return options{cfg: cfg, asJSON: *asJSON}, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	opts, err := parseFlags(args, stderr)
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			diag(stderr, "rebound-conformance:", err)
		}
		return 2
	}
	if opts.cfg.Seed == 0 {
		if opts.cfg.Seed, err = conformance.NewSeed(); err != nil {
			diag(stderr, "rebound-conformance:", err)
			return 2
		}
	}
	diag(stderr, fmt.Sprintf("rebound-conformance: seed %d, target %s", opts.cfg.Seed, opts.cfg.BaseURL))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := conformance.Run(ctx, opts.cfg)
	if err != nil {
		diag(stderr, "rebound-conformance:", err)
		return 2
	}
	write := report.WriteTable
	if opts.asJSON {
		write = report.WriteJSON
	}
	if err := write(stdout); err != nil {
		diag(stderr, "rebound-conformance: write report:", err)
		return 2
	}
	if !report.Passed {
		return 1
	}
	return 0
}

// diag writes a diagnostic line to stderr; if stderr itself fails there is nowhere left to report.
func diag(w io.Writer, args ...any) {
	_, _ = fmt.Fprintln(w, args...)
}
