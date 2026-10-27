SHELL := /bin/bash
GO ?= go
PKGS := $(shell $(GO) list ./... 2>/dev/null | grep -v -E '/(cmd|examples)/' || echo ./...)
REDIS_ADDR ?= localhost:6379
REDIS_CLUSTER_ADDR ?= localhost:7000
# Pinned tool versions (Go 1.26+, ADR 0003).
GOLANGCI_LINT_VERSION ?= v2.14.0
GOFUMPT_VERSION ?= v0.12.0
GOIMPORTS_VERSION ?= v0.52.0
GOVULNCHECK_VERSION ?= v1.8.0

.PHONY: help tidy fmt fmt-check vet lint test test-race test-integration cover check \
        no-sleep-check conformance conformance-redis client-build client-test demo demo-redis \
        readme-check tools ci playground playground-down         stress-up stress-down load load-baseline soak spike storm faults slow

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-20s\033[0m %s\n",$$1,$$2}'

tools: ## Install dev tools pinned versions
	$(GO) install mvdan.cc/gofumpt@$(GOFUMPT_VERSION)
	$(GO) install golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION)
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	$(GO) install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

tidy: ## go mod tidy
	$(GO) mod tidy

fmt: ## Format code
	gofumpt -l -w .
	goimports -local github.com/bailalayoub/rebound -w .

fmt-check: ## Fail if code is not formatted
	@test -z "$$(gofumpt -l .)" || (echo "gofumpt: files need formatting:"; gofumpt -l .; exit 1)

vet: ## go vet
	$(GO) vet ./...

lint: ## golangci-lint
	golangci-lint run ./...

test: ## Unit tests
	$(GO) test ./...

test-race: ## Unit tests with the race detector (what CI runs)
	$(GO) test -race -count=1 ./...

test-integration: ## Integration tests (needs Redis at $(REDIS_ADDR) and a cluster-mode Redis at $(REDIS_CLUSTER_ADDR))
	REDIS_ADDR=$(REDIS_ADDR) REDIS_CLUSTER_ADDR=$(REDIS_CLUSTER_ADDR) $(GO) test -race -count=1 -timeout 20m -tags=integration ./...

no-sleep-check: ## Forbid time.Sleep in unit tests (integration files excluded)
	@bad=$$(grep -rln --include='*_test.go' 'time\.Sleep' . | xargs -r grep -L 'go:build integration' || true); \
	if [ -n "$$bad" ]; then echo "time.Sleep found in unit tests:"; echo "$$bad"; exit 1; fi

cover: ## Coverage with per-package thresholds (see docs/07)
	$(GO) test -race -count=1 -coverprofile=coverage.out -covermode=atomic $(PKGS)
	@$(GO) tool cover -func=coverage.out | tail -n 1
	@./scripts/coverage-gate.sh coverage.out

check: fmt-check vet lint no-sleep-check test-race ## Everything that must be green before a commit
	@echo "check: OK"

ci: check cover ## What CI runs (plus integration/conformance in dedicated jobs)

conformance: ## Conformance tester against examples/chat with the memory store
	$(GO) build -o bin/rebound-chat ./examples/chat
	$(GO) build -o bin/rebound-conformance ./cmd/rebound-conformance
	./scripts/conformance-memory.sh

conformance-redis: ## Conformance against 2 instances + redis + nginx (docker compose)
	docker compose -f deploy/docker-compose.yml up -d --build
	./scripts/wait-for.sh http://localhost:8080/healthz 60
	$(GO) run ./cmd/rebound-conformance --base-url http://localhost:8080 --runs 200
	docker compose -f deploy/docker-compose.yml down

client-build: ## Build the TypeScript client and copy it into the example UI
	cd clients/typescript && npm ci && npm run build
	mkdir -p examples/chat/web/vendor/rebound
	cp clients/typescript/dist/*.js examples/chat/web/vendor/rebound/

client-test: ## Test the TypeScript client, and check that the package ships its build
	cd clients/typescript && npm ci && npm test
	cd clients/typescript && npm pack --dry-run 2>&1 | grep -q 'dist/index.js' || (echo "npm pack ships no dist/"; exit 1)

demo: client-build ## Run the example chat on :8080 (memory store, fake LLM)
	$(GO) run ./examples/chat

PLAYGROUND := docker compose -f playground/docker-compose.yml --env-file playground/.env

playground: ## Chat UI on a real LLM: 2 instances + Redis + nginx, settings in playground/.env (see playground/README.md)
	@test -f playground/.env || (echo "playground/.env is missing: cp playground/.env.example playground/.env, then edit it"; exit 1)
	$(PLAYGROUND) up --build

playground-down: ## Stop and remove the playground containers
	$(PLAYGROUND) down

demo-redis: client-build ## Run the multi-instance demo
	docker compose -f deploy/docker-compose.yml up --build

readme-check: ## The README quick start is examples/readme-check/main.go, and it builds in a fresh module
	@awk '/<!-- quickstart:start -->/{f=1;next} /<!-- quickstart:end -->/{f=0} f' README.md | sed '1d;$$d' 		| diff -u - examples/readme-check/main.go || (echo "README quick start differs from examples/readme-check/main.go"; exit 1)
	@# Follow the README in a module outside this one; the replace stands in for the published module.
	@tmp=$$(mktemp -d) && trap 'rm -rf "$$tmp"' EXIT && cp examples/readme-check/main.go "$$tmp/" && cd "$$tmp" 		&& $(GO) mod init example.com/chat >/dev/null 2>&1 		&& $(GO) mod edit -require=github.com/bailalayoub/rebound@v0.0.0 -replace=github.com/bailalayoub/rebound=$(CURDIR) 		&& $(GO) mod tidy >/dev/null 2>&1 		&& $(GO) build -o /dev/null . && echo "readme-check: OK (fresh module: go mod tidy, go build)"

# Stress campaign (docs/STRESS-v0.1.0.md): never part of check or ci. Each target runs
# cmd/rebound-load in a container against the deploy/ stack and keeps its outputs under
# docs/stress-v0.1.0/raw. The harness is built without -race: these are throughput measurements.
STRESS := ./scripts/stress/run.sh

stress-up: ## Stress: start deploy/ with the stress overlay (CPU pinning, DEBUG_ADDR)
	$(STRESS) up

stress-down: ## Stress: stop it
	$(STRESS) down

load: ## Stress test 1: ramp 50-2000 streams through nginx (NAME=ramp-1)
	$(STRESS) ramp $(or $(NAME),ramp)

load-baseline: ## Stress test 1: the same ramp on one instance with the memory store
	$(STRESS) baseline $(or $(NAME),baseline)

soak: ## Stress test 2: 2 h at STREAMS streams with 30 % connection drops
	STREAMS=$(STREAMS) $(STRESS) soak $(or $(NAME),soak)

spike: ## Stress test 3: idle to 5x STREAMS in 10 s, then back
	STREAMS=$(STREAMS) $(STRESS) spike $(or $(NAME),spike)

storm: ## Stress test 4: readers reconnecting every 1-5 events (DROP=ms:5-50 for the timed variant)
	$(STRESS) storm $(or $(NAME),storm)

faults: ## Stress test 5: Redis restart and pause, kill app-1, graceful stop of app-2, under load
	$(STRESS) fault-redis
	$(STRESS) fault-redis-pause
	$(STRESS) fault-kill
	$(STRESS) fault-stop

slow: ## Stress test 6: readers that stop reading, next to a normal load, and its control run
	$(STRESS) slow-control
	$(STRESS) slow
