package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/store/memory"
)

func main() {
	hub, err := rebound.New(memory.New()) // store/redis for several instances
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	// Start the generation, detached from this request; reply with its id at once.
	mux.HandleFunc("POST /chat", func(w http.ResponseWriter, r *http.Request) {
		st, err := hub.Start(r.Context(), func(ctx context.Context, out rebound.Writer) error {
			for _, token := range []string{"Hello", ", ", "world"} { // your LLM tokens here
				if err := out.WriteJSON(map[string]string{"text": token}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, st.ID+"\n")
	})
	// Follow, and resume with Last-Event-ID, any stream.
	mux.Handle("GET /streams/{id}", httpx.NewHandler(hub).Stream(httpx.PathValue("id")))
	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
