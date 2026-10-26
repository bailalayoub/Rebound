package ginx_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/gin-gonic/gin"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/ginx"
	"github.com/bailalayoub/rebound/httpx"
	"github.com/bailalayoub/rebound/store/memory"
)

// A resumable chat endpoint with Gin: POST /chat starts a generation and returns its id;
// GET /streams/:id follows it and resumes with Last-Event-ID; POST /streams/:id/cancel stops it.
func ExampleStream() {
	hub, err := rebound.New(memory.New())
	if err != nil {
		log.Fatal(err)
	}
	streams := httpx.NewHandler(hub)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New() // gin.Default() also logs every request
	r.POST("/chat", func(c *gin.Context) {
		st, err := hub.Start(c.Request.Context(), func(ctx context.Context, out rebound.Writer) error {
			for _, token := range []string{"Hello", ", ", "world"} { // your LLM tokens here
				if err := out.WriteJSON(gin.H{"text": token}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not start"})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"stream_id": st.ID, "url": "/streams/" + st.ID})
	})
	r.GET("/streams/:id", ginx.Stream(streams, "id"))
	r.POST("/streams/:id/cancel", ginx.Cancel(streams, "id"))
	srv := httptest.NewServer(r) // in production: r.Run(":8080")

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
