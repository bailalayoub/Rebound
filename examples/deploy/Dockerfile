# Image of the example chat server (examples/chat). Build context: the repository root.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rebound-chat ./examples/chat

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rebound-chat /rebound-chat
EXPOSE 8080
ENTRYPOINT ["/rebound-chat"]
