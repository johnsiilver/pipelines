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

// Request represents our request object. You can find a blank one
// to fill out in file "requestTemplate" located in this directory.
type Request struct {
	id uint64

	recs []client.Record

	// err holds an error if processing the Request had a problem.
	err error

	nextStage stagedpipe.Stage
}

// NewRequest creates a new Request.
func NewRequest(recs []client.Record) Request {
	return Request{
		recs: recs,
	}
}

// Pre implements stagedpipe.Request.Pre().
func (r Request) Pre() {
}

// Post implements stagedpipe.Request.Post().
func (r Request) Post() {
}

// Error implements stagedpipe.Request.Error().
func (r Request) Error() error {
	return r.err
}

// SetError implements stagedpipe.Request.SetError().
func (r Request) SetError(e error) stagedpipe.Request {
	r.err = e
	return r
}

// Next implements stagedpipe.Request.Next().
func (r Request) Next() stagedpipe.Stage {
	return r.nextStage
}

// Setnext implements stagedpipe.Request.SetNext().
func (r Request) SetNext(stage stagedpipe.Stage) stagedpipe.Request {
	r.nextStage = stage
	return r
}

func (r Request) GroupNum() uint64 {
	return r.id
}

func (r Request) SetGroupNum(id uint64) stagedpipe.Request {
	r.id = id
	return r
}

// ToConcrete converts a statepipe.Request to our concrete Request object.
func ToConcrete(r stagedpipe.Request) (Request, error) {
	x, ok := r.(Request)
	if !ok {
		return Request{}, fmt.Errorf("unexpected type %T, expecting Request", r)
	}
	return x, nil
}

// MustConcrete is the same as ToConcrete expect an error causes a panic.
func MustConcrete(r stagedpipe.Request) Request {
	x, err := ToConcrete(r)
	if err != nil {
		panic(err)
	}
	return x
}

// SM implements stagedpipe.StateMachine.
type SM struct {
	// idClient is a client for querying for information based on an ID.
	idClient *client.ID
}

// NewSM creates a new stagepipe.StateMachine.
func NewSM(client *client.ID) *SM {
	sm := &SM{
		idClient: client,
	}
	return sm
}

// Close stops all running goroutines. This is only safe after all entries have
// been processed.
func (s *SM) Close() {}

// Start implements stagedpipe.StateMachine.Start().
func (s *SM) Start(ctx context.Context, req stagedpipe.Request) stagedpipe.Request {
	x, err := ToConcrete(req)
	if err != nil {
		return req.SetError(fmt.Errorf("unexpected type %T, expecting Request", req))
	}

	// This trims any excess space off of some string attributes.
	// Because "x" is not a pointer, x.recs are not pointers, I need
	// to reassign the changed entry to x.recs[i] .
	for i, rec := range x.recs {
		rec.First = strings.TrimSpace(rec.First)
		rec.Last = strings.TrimSpace(rec.Last)
		rec.ID = strings.TrimSpace(rec.ID)

		switch {
		case rec.First == "":
			return req.SetError(fmt.Errorf("Record.First cannot be empty"))
		case rec.Last == "":
			return req.SetError(fmt.Errorf("Record.Last cannot be empty"))
		case rec.ID == "":
			return req.SetError(fmt.Errorf("Record.ID cannot be empty"))
		}
		x.recs[i] = rec
	}

	return req.SetNext(s.IdVerifier)
}

// IdVerifier takes a Request and adds it to a bulk request to be sent to the
// identity service. This is the last stage of this pipeline.
func (s *SM) IdVerifier(ctx context.Context, req stagedpipe.Request) stagedpipe.Request {
	x := MustConcrete(req)

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := s.idClient.Call(ctx, x.recs); err != nil {
		return x.SetError(err)
	}

	return req.SetNext(nil)
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

func (g *gen) genRequests(n int) []Request {
	reqs := make([]Request, n)

	for i := 0; i < n; i++ {
		reqs[i] = NewRequest(g.genRecord(10))
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
		requests []Request
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
	p, err := stagedpipe.New(10, []stagedpipe.StateMachine{sm})
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
					for _, rec := range MustConcrete(req).recs {
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
			if err := g.Submit(ctx, req); err != nil {
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
	ctx := context.Background()
	sm := NewSM(&client.ID{})

	p, err := stagedpipe.New(runtime.NumCPU(), []stagedpipe.StateMachine{sm})
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
			if err := g.Submit(ctx, req); err != nil {
				panic(err)
			}
		}
		g.Close(false)
	}
}
