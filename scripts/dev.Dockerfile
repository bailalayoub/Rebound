# Dev/CI-parity toolchain: Go 1.26 (the go.mod floor, ADR 0003), gcc for -race, pinned lint tools.
# Used by scripts/in-docker.sh on machines without make/gcc (e.g. Windows).
FROM golang:1.26-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends bc python3 \
 && rm -rf /var/lib/apt/lists/*
COPY Makefile /tmp/tools.mk
RUN make -f /tmp/tools.mk tools && rm -rf /root/.cache/go-build /go/pkg/mod
ENV GOFLAGS=-buildvcs=false
