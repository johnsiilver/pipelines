package etoe_test

import (
	"context"
	"fmt"
	"log"
	"sort"
	"testing"

	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin/client"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin/etoe/testplugin/sm"

	"github.com/go-json-experiment/json"
)

const testpluginPath = "testplugin/testplugin"

// TestSuccess tests that the plugin can be created and run that has no unexpected errors.
func TestSuccess(t *testing.T) {
	const count = 1000

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := client.New(testpluginPath)
	if err != nil {
		t.Fatalf("cannot create plugin: %s", err)
	}
	defer p.Quit(ctx)

	cb, err := json.Marshal(&sm.RGConfig{})
	if err != nil {
		t.Fatalf("failed to marshal JSON config object: %s", err)
	}

	// Create a new RequestGroup.
	rg, err := p.RequestGroup(ctx, cb)
	if err != nil {
		t.Fatalf("cannot create request group: %s", err)
	}

	counter := 0
	done := make(chan struct{})
	got := []sm.Data{}
	go func() {
		defer close(done)

		for out := range rg.Output() {
			counter++
			if out.Err != nil {
				panic(fmt.Sprintf("pipeline returned an error: %s", out.Err))
			}
			var d sm.Data
			if err := json.Unmarshal(out.Data, &d); err != nil {
				panic(fmt.Sprintf("failed to unmarshal JSON data object: %s", err))
			}
			got = append(got, d)
		}
	}()

	// Send a series of requests to the pipeline.
	for i := 0; i < count; i++ {
		d := sm.Data{
			Item: i,
		}
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("failed to marshal JSON data object: %s", err)
		}
		if err := rg.Submit(client.Request{Data: b}); err != nil {
			t.Fatalf("failed to submit request to pipeline: %s", err)
		}
	}
	if err := rg.Close(); err != nil {
		t.Fatalf("failed to close request group: %s", err)
	}

	<-done

	if counter != count {
		t.Fatalf("did not receive all responses: got %d responses, want %d responses", counter, count)
	}

	sort.Slice(got, func(i, j int) bool {
		return got[i].Item < got[j].Item
	})

	for i := 0; i < count; i++ {
		if got[i].Item != i {
			t.Errorf("got %d, want %d", got[i].Item, i)
		}
	}
}

// TestPanic tests how the client handles a panic in the plugin.
func TestPanic(t *testing.T) {
	const count = 1000

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := client.New(testpluginPath)
	if err != nil {
		t.Fatalf("cannot create plugin: %s", err)
	}
	defer p.Kill(ctx)

	cb, err := json.Marshal(&sm.RGConfig{})
	if err != nil {
		t.Fatalf("failed to marshal JSON config object: %s", err)
	}

	// Create a new RequestGroup.
	rg, err := p.RequestGroup(ctx, cb)
	if err != nil {
		t.Fatalf("cannot create request group: %s", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		for out := range rg.Output() {
			if out.Err != nil {
				log.Println(out.Err)
			}
		}
	}()

	// Send a series of requests to the pipeline.
	for i := 0; i < count; i++ {
		d := sm.Data{
			Item: i,
		}
		if i == count/2 {
			d.Panic = true
		}

		b, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("failed to marshal JSON data object: %s", err)
		}
		if err := rg.Submit(client.Request{Data: b}); err != nil {
			t.Fatalf("failed to submit request to pipeline: %s", err)
		}
	}
	if err := rg.Close(); err == nil {
		t.Fatal("expected error, got nil")
	}

	<-done
}
