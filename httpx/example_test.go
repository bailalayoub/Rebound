package httpx_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/store/memory"
)

// A resumable chat endpoint with net/http: POST /chat starts a generation and returns its id;
// GET /streams/{id} follows it and resumes with Last-Event-ID; POST /streams/{id}/cancel stops it.
func ExampleNewHandler() {
	hub, err := rebound.New(memory.New())
	if err != nil {
		log.Fatal(err)
	}
	streams := httpx.NewHandler(hub)

	mux := http.NewServeMux()
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
			http.Error(w, "could not start", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(map[string]string{"stream_id": st.ID, "url": "/streams/" + st.ID}); err != nil {
			log.Print(err)
		}
	})
	mux.Handle("GET /streams/{id}", streams.Stream(httpx.PathValue("id")))
	mux.Handle("POST /streams/{id}/cancel", streams.Cancel(httpx.PathValue("id")))
	srv := httptest.NewServer(mux) // in production: http.ListenAndServe(":8080", mux)

	// Try it: start a generation, then follow it from the beginning.
	body, err := startAndFollow(context.Background(), srv.URL)
	srv.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(body)
	// Output:
	// retry: 1000
	//
	// id: 1
	// event: data
	// data: {"text":"Hello"}
	//
	// id: 2
	// event: data
	// data: {"text":", "}
	//
	// id: 3
	// event: data
	// data: {"text":"world"}
	//
	// id: 4
	// event: done
	// data: {}
}

// startAndFollow posts to /chat, then reads the whole stream it returns. Send "Last-Event-ID: n"
// with the GET to resume after event n.
func startAndFollow(ctx context.Context, baseURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/chat", http.NoBody)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	var created struct {
		URL string `json:"url"`
	}
	err = json.NewDecoder(resp.Body).Decode(&created)
	_ = resp.Body.Close() // fully read
	if err != nil {
		return "", err
	}
	if req, err = http.NewRequestWithContext(ctx, http.MethodGet, baseURL+created.URL, http.NoBody); err != nil {
		return "", err
	}
	if resp, err = http.DefaultClient.Do(req); err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }() // fully read
	body, err := io.ReadAll(resp.Body)       // the server closes the response after the terminal event
	return string(body), err
}
