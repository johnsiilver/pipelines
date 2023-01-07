package stagedpipe_test

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johnsiilver/pipelines/stagedpipe"
	"github.com/johnsiilver/pipelines/stagedpipe/testing/client"
)

// SM implements stagedpipe.StateMachine.
type SM struct {
	// idClient is a client for querying for information based on an ID.
	idClient *client.ID
}

// NewSM creates a new stagepipe.StateMachine.
func NewSM(cli *client.ID) *SM {
	sm := &SM{
		idClient: cli,
	}
	return sm
}

// Close stops all running goroutines. This is only safe after all entries have
// been processed.
func (s *SM) Close() {}

// Start implements stagedpipe.StateMachine.Start().
func (s *SM) Start(req stagedpipe.Request[[]client.Record]) stagedpipe.Request[[]client.Record] {
	// This trims any excess space off of some string attributes.
	// Because "x" is not a pointer, x.recs are not pointers, I need
	// to reassign the changed entry to x.recs[i] .
	for i, rec := range req.Data {
		rec.First = strings.TrimSpace(rec.First)
		rec.Last = strings.TrimSpace(rec.Last)
		rec.ID = strings.TrimSpace(rec.ID)

		switch {
		case rec.First == "":
			req.Err = fmt.Errorf("Record.First cannot be empty")
			return req
		case rec.Last == "":
			req.Err = fmt.Errorf("Record.Last cannot be empty")
			return req
		case rec.ID == "":
			req.Err = fmt.Errorf("Record.ID cannot be empty")
			return req
		}
		req.Data[i] = rec
	}

	req.Next = s.IdVerifier
	return req
}

// IdVerifier takes a Request and adds it to a bulk request to be sent to the
// identity service. This is the last stage of this pipeline.
func (s *SM) IdVerifier(req stagedpipe.Request[[]client.Record]) stagedpipe.Request[[]client.Record] {
	ctx, cancel := context.WithTimeout(req.Ctx, 2*time.Second)
	defer cancel()

	recs, err := s.idClient.Call(ctx, req.Data)
	if err != nil {
		req.Err = err
		return req
	}
	req.Data = recs
	req.Next = nil
	return req
}

type gen struct {
	lastID int
}

func (g *gen) genRecord(n int) []client.Record {
	recs := make([]client.Record, n)

	for i := 0; i < n; i++ {
		s := strconv.Itoa(g.lastID + 1)
		g.lastID++
		rec := client.Record{First: s, Last: s, ID: s}
		recs[i] = rec
	}
	return recs
}

func (g *gen) genRequests(n int) []stagedpipe.Request[[]client.Record] {
	reqs := make([]stagedpipe.Request[[]client.Record], n)

	for i, req := range reqs {
		req.Data = g.genRecord(10)
		reqs[i] = req
	}
	return reqs
}

const day = 24 * time.Hour

func TestPipelines(t *testing.T) {
	t.Parallel()

	g := gen{}
	rs1 := g.genRequests(1)
	g = gen{}
	rs1000 := g.genRequests(1000)

	tests := []struct {
		desc     string
		requests []stagedpipe.Request[[]client.Record]
	}{
		{
			desc:     "1 entry only",
			requests: rs1,
		},

		{
			desc:     "1000 entries",
			requests: rs1000,
		},
	}

	sm := NewSM(&client.ID{})
	p, err := stagedpipe.New("test statemachine", 10, stagedpipe.StateMachine[[]client.Record](sm))
	if err != nil {
		panic(err)
	}
	defer p.Close()

	for _, test := range tests {
		ctx := context.Background()

		g := p.NewRequestGroup()
		expectedRecs := make([]bool, len(test.requests)*10)

		go func() {
			ch := g.Out()
			tick := time.NewTicker(10 * time.Second)
			for {
				select {
				case req, ok := <-ch:
					if !ok {
						return
					}
					for _, rec := range req.Data {
						id, _ := strconv.Atoi(rec.ID)
						expectedRecs[id-1] = true
						if rec.Birth.IsZero() {
							log.Fatalf("TestPipeline(%s): requests are not being processed", test.desc)
						}
						wantBirth := time.Time{}.Add(time.Duration(id) * day)
						if !rec.Birth.Equal(wantBirth) {
							log.Fatalf("TestPipeline(%s): requests are not being processed correctly, ID %d gave Birthday of %v, want %v", test.desc, id, rec.Birth, wantBirth)
						}
					}
				case <-tick.C:
					log.Println("nothing came out after 10 seconds, pipeline is probably stuck")
				}
				tick.Reset(10 * time.Second)
			}
		}()

		for _, req := range test.requests {
			req.Ctx = ctx
			if err := g.Submit(req); err != nil {
				panic(err)
			}
		}
		// Wait for all the Record in the RecordSet to finish processing.
		g.Close(false)

		for i := 0; i < len(expectedRecs); i++ {
			if !expectedRecs[i] {
				t.Errorf("TestPipelines(%s): an expected client.Record[%d] was not set", test.desc, i)
			}
		}
	}
}

func BenchmarkPipeline(b *testing.B) {
	b.ReportAllocs()

	gen := gen{}
	reqs := gen.genRequests(100000)
	sm := NewSM(&client.ID{})

	p, err := stagedpipe.New("test", runtime.NumCPU(), stagedpipe.StateMachine[[]client.Record](sm))
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := p.NewRequestGroup()
		go func() {
			// For this exercise we need to drain the Out channel so things continue
			// to process.
			for range g.Out() {
			}
		}()

		for _, req := range reqs {
			req.Ctx = context.Background()
			if err := g.Submit(req); err != nil {
				panic(err)
			}
		}
		g.Close(false)
	}
}
