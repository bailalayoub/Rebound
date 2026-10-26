// Command chat is the example resumable chat server of docs/08-example-and-client.md and the
// target of the conformance tester (docs/06). Configuration comes from the environment:
//
//	PORT            listen port (default 8080)
//	REBOUND_STORE    memory (default) or redis
//	REDIS_ADDR      Redis address when REBOUND_STORE=redis (default localhost:6379)
//	INSTANCE_NAME   name shown in logs, /healthz and the UI (default: the hostname)
//	LLM_PROVIDER    fake (default) or openai (any OpenAI-compatible chat completions API)
//	LLM_BASE_URL    with openai: the API base including its version, as in the provider's docs
//	                (default https://api.openai.com/v1); "/chat/completions" is appended
//	LLM_API_KEY     with openai: required (any value for a local Ollama); never logged
//	LLM_MODEL       with openai: required, e.g. gpt-6-luna
//	LLM_STREAM_USAGE  0 omits stream_options.include_usage, for providers that reject it
//	                (default 1)
//
// See playground/.env.example for ready-made provider settings.
//
//	FAKE_DELAY_MS   pause between words of the fake model (default 120)
//	FAKE_REPEAT     times the fake model repeats its answer (default 2, about a minute)
//	CONFORMANCE     1 mounts POST /conformance/streams, the test-only producer of the conformance
//	                tester (docs/06); off by default: it lets anyone start large producers
//
// This is a demo: it has no authentication and no rate limit. Do not expose it to the internet as
// is, least of all with LLM_PROVIDER=openai, which would let anyone spend your API credits.
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/examples/chat/llm"
	"github.com/bailalayoub/rebound/ginx"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/internal/conformance"
	"github.com/bailalayoub/rebound/store"
	"github.com/bailalayoub/rebound/store/memory"
	redisstore "github.com/bailalayoub/rebound/store/redis"
)

//go:embed web
var webFiles embed.FS

// config is read from the environment.
type config struct {
	port       string
	storeKind  string
	redisAddr  string
	instance   string
	provider   string
	llmURL     string
	llmKey     string
	llmModel   string
	llmUsage   bool
	fakeDelay  time.Duration
	fakeRepeat int
	// conformance mounts the test-only conformance endpoint (CONFORMANCE=1).
	conformance bool
}

func loadConfig() config {
	host, err := os.Hostname()
	if err != nil {
		host = "chat"
	}
	return config{
		port:        envOr("PORT", "8080"),
		storeKind:   envOr("REBOUND_STORE", "memory"),
		redisAddr:   envOr("REDIS_ADDR", "localhost:6379"),
		instance:    envOr("INSTANCE_NAME", host),
		provider:    envOr("LLM_PROVIDER", "fake"),
		llmURL:      envOr("LLM_BASE_URL", "https://api.openai.com/v1"),
		llmKey:      strings.TrimSpace(os.Getenv("LLM_API_KEY")),
		llmModel:    strings.TrimSpace(os.Getenv("LLM_MODEL")),
		llmUsage:    os.Getenv("LLM_STREAM_USAGE") != "0",
		fakeDelay:   time.Duration(envInt("FAKE_DELAY_MS", 120)) * time.Millisecond,
		fakeRepeat:  envInt("FAKE_REPEAT", 2),
		conformance: os.Getenv("CONFORMANCE") == "1",
	}
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v >= 0 {
		return v
	}
	return def
}

// newProvider returns the configured model, or a readable error that stops the server at startup.
func newProvider(cfg config) (llm.Provider, error) {
	switch cfg.provider {
	case "fake":
		return llm.Fake{Delay: cfg.fakeDelay, Repeat: cfg.fakeRepeat}, nil
	case "openai":
		var missing []string
		if cfg.llmKey == "" {
			missing = append(missing, "LLM_API_KEY (any value for a local Ollama)")
		}
		if cfg.llmModel == "" {
			missing = append(missing, "LLM_MODEL")
		}
		if len(missing) > 0 {
			return nil, fmt.Errorf("chat: LLM_PROVIDER=openai needs %s; see playground/.env.example",
				strings.Join(missing, " and "))
		}
		if u, err := url.Parse(cfg.llmURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			// Never echo the value: a key pasted on the wrong line of .env would end up in the logs.
			return nil, errors.New("chat: LLM_BASE_URL must be an http(s) URL with a host, such as https://api.openai.com/v1")
		}
		return llm.OpenAI{BaseURL: cfg.llmURL, APIKey: cfg.llmKey, Model: cfg.llmModel, NoStreamUsage: !cfg.llmUsage}, nil
	default:
		return nil, fmt.Errorf("chat: LLM_PROVIDER must be fake or openai, got %q", cfg.provider)
	}
}

// about names the model that answers, for /healthz and the UI. It never includes the key.
func (c config) about() modelInfo {
	if c.provider == "openai" {
		host := c.llmURL
		if u, err := url.Parse(c.llmURL); err == nil && u.Host != "" {
			host = u.Host
		}
		return modelInfo{Provider: host, Model: c.llmModel}
	}
	return modelInfo{Provider: c.provider, Model: "built-in fake model"}
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := loadConfig()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("instance", cfg.instance)
	if err := run(cfg, logger); err != nil {
		logger.Error("chat: exiting", "error", err)
		fmt.Fprintln(os.Stderr, err) // plain text too, so a failed start reads well in docker logs
		os.Exit(1)
	}
}

func run(cfg config, logger *slog.Logger) error {
	provider, err := newProvider(cfg)
	if err != nil {
		return err
	}
	st, closeStore, err := openStore(cfg)
	if err != nil {
		return err
	}
	defer closeStore()

	hub, err := rebound.New(st,
		rebound.WithLogger(logger),
		// A short heartbeat makes a cancel received by another instance take effect within ~1 s.
		rebound.WithHeartbeatInterval(time.Second),
	)
	if err != nil {
		return err
	}
	handler, err := newRouter(hub, provider, cfg, logger)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              ":" + cfg.port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		// No WriteTimeout: streams are long-lived; httpx bounds each write instead.
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	about := cfg.about()
	logger.Info("chat: listening", "port", cfg.port, "store", cfg.storeKind,
		"llm_provider", about.Provider, "llm_model", about.Model) // never the key

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	logger.Info("chat: shutting down")
	// Let running generations finish (or end them with server_shutdown after 30 s), so that the
	// open streams receive their terminal event, then stop the HTTP server.
	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := hub.Close(closeCtx); err != nil {
		logger.Warn("chat: hub close", "error", err)
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("chat: shutdown: %w", err)
	}
	return nil
}

// openStore returns the configured store and a function releasing it.
func openStore(cfg config) (store.Store, func(), error) {
	switch cfg.storeKind {
	case "memory":
		s := memory.New()
		return s, func() { _ = s.Close() }, nil // closing an in-memory store cannot fail usefully
	case "redis":
		client := goredis.NewClient(&goredis.Options{
			Addr: cfg.redisAddr,
			// At most half of the pool waits in XREAD BLOCK (redis.WithMaxBlockingReads); the other
			// half serves appends, heartbeats and reads.
			PoolSize: 256,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Ping(ctx).Err(); err != nil {
			return nil, nil, fmt.Errorf("chat: redis %s: %w", cfg.redisAddr, err)
		}
		s, err := redisstore.New(client)
		if err != nil {
			return nil, nil, err
		}
		release := func() {
			_ = s.Close()      // wakes blocked reads; cannot fail
			_ = client.Close() // the process is exiting
		}
		return s, release, nil
	default:
		return nil, nil, fmt.Errorf("chat: REBOUND_STORE must be memory or redis, got %q", cfg.storeKind)
	}
}

// newRouter wires the HTTP routes.
func newRouter(hub *rebound.Hub, provider llm.Provider, cfg config, logger *slog.Logger) (http.Handler, error) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), requestLog(logger))
	streams := httpx.NewHandler(hub)

	r.GET("/healthz", func(c *gin.Context) {
		about := cfg.about()
		c.JSON(http.StatusOK, gin.H{
			"store": cfg.storeKind, "instance": cfg.instance,
			"provider": about.Provider, "model": about.Model,
		})
	})
	r.POST("/chat", chatHandler(hub, provider, cfg.instance, cfg.about()))
	r.GET("/streams/:id", ginx.Stream(streams, "id"))
	r.POST("/streams/:id/cancel", ginx.Cancel(streams, "id"))
	// Test support, opt-in: the deterministic producer of the conformance contract (docs/06 §1).
	if cfg.conformance {
		r.POST("/conformance/streams", gin.WrapH(conformance.StartHandler(hub)))
	}

	// The UI: index.html, app.js and the built @bailalayoub/rebound-client (web/vendor, see make
	// client-build).
	web, err := fs.Sub(webFiles, "web")
	if err != nil {
		return nil, fmt.Errorf("chat: web files: %w", err)
	}
	files := http.FileServer(http.FS(web))
	r.NoRoute(func(c *gin.Context) {
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
			return
		}
		files.ServeHTTP(c.Writer, c.Request)
	})
	return r, nil
}

// requestLog logs one line per request. The instance name (on the logger) and the Last-Event-ID
// make reconnections, and which instance served them, visible in multi-instance deployments.
func requestLog(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		if c.Request.URL.Path == "/healthz" {
			return
		}
		logger.Info("request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"last_event_id", c.GetHeader("Last-Event-ID"),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}
}
