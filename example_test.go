package rebound_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/bailalayoub/rebound"
	"github.com/bailalayoub/rebound/store/memory"
)

func ExampleNew() {
	hub, err := rebound.New(memory.New(),
		rebound.WithMaxDuration(5*time.Minute),
		rebound.WithRetention(30*time.Minute),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := hub.Close(context.Background()); err != nil {
			log.Print(err)
		}
	}()
	fmt.Println("hub ready")
	// Output: hub ready
}

// Start runs the producer detached from the caller; any number of readers can then follow the
// stream from any cursor.
func ExampleHub_Start() {
	ctx := context.Background()
	hub, err := rebound.New(memory.New())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := hub.Close(ctx); err != nil {
			log.Print(err)
		}
	}()

	stream, err := hub.Start(ctx, func(_ context.Context, w rebound.Writer) error {
		for _, word := range []string{"Bon", "jour"} {
			if err := w.WriteJSON(map[string]string{"text": word}); err != nil {
				return err
			}
		}
		return nil // ends the stream with a "done" event
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	sub, err := hub.Subscribe(ctx, stream.ID, 0)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := sub.Close(); err != nil {
			log.Print(err)
		}
	}()
	for {
		ev, err := sub.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%d %s %s\n", ev.Seq, ev.Kind, ev.Data)
	}
	// Output:
	// 1 data {"text":"Bon"}
	// 2 data {"text":"jour"}
	// 3 done {}
}

// A client reads two events, loses its connection, and resumes from the last seq it received:
// it gets exactly what it missed, whether the producer is still running or already done.
func ExampleHub_Subscribe() {
	ctx := context.Background()
	hub, err := rebound.New(memory.New())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := hub.Close(ctx); err != nil {
			log.Print(err)
		}
	}()
	stream, err := hub.Start(ctx, func(_ context.Context, w rebound.Writer) error {
		for _, chunk := range []string{"a", "b", "c", "d"} {
			if err := w.Write([]byte(chunk)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	first, err := hub.Subscribe(ctx, stream.ID, 0)
	if err != nil {
		fmt.Println(err)
		return
	}
	var lastEventID uint64
	for range 2 {
		ev, err := first.Next(ctx)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%d %s %s\n", ev.Seq, ev.Kind, ev.Data)
		lastEventID = ev.Seq
	}
	if err := first.Close(); err != nil { // the connection drops
		fmt.Println(err)
		return
	}
	fmt.Println("-- reconnect with Last-Event-ID:", lastEventID)

	resumed, err := hub.Subscribe(ctx, stream.ID, lastEventID)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := resumed.Close(); err != nil {
			log.Print(err)
		}
	}()
	for {
		ev, err := resumed.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%d %s %s\n", ev.Seq, ev.Kind, ev.Data)
	}
	// Output:
	// 1 data a
	// 2 data b
	// -- reconnect with Last-Event-ID: 2
	// 3 data c
	// 4 data d
	// 5 done {}
}
