/*
Package stagedpipe offers a generic, concurrent and parallel pipeline based on a
statemachine to process work. For N number of Stages in the StateMachine, N Stages
can concurrently be processed. You can run pipelines in parallel. So X Pipelines
with N Stages will have X*N Stages processing.

This library requires working knowledge of both the specific type of Go statemachine
implementation and basic Go pipelining.

You can view the statemachine concepts here: https://www.youtube.com/watch?v=HxaD_trXwRE .
You can ignore how the parser works he is implementing.

You can see basic Go pipelining here: https://www.youtube.com/watch?v=oV9rvDllKEg .

Every pipeline will receive a Request, which contains the data to be manipulated.
Each Request is designed to be stack allocated, meaning the data should not be a pointer
unless absolutely necessary.

You define StateMachine objects that satisfy the StateMachine interface. These states
represent the stages of the pipeline. A single StateMachine will process a single
Request at a time, allowing use of your StateMachine's internal objects without mutexes.
All StateMachine methods that implement a Stage MUST BE PUBLIC.

A RequestGroup represents a set of related Request(s) that should be processed together.
A new RequestGroup can be created with Pipelines.NewRequestGroup().

Requests enter the Pipelines via the RequestGroup.Submit() method. Requests are received
with RequestGroup.Out(), which returns a channel of Request(s).

Multiple RequestGroup(s) can send into the Pipelines for processing, as everything is
muxed into the Pipelines and demuxed out to the RequestGroup.Out() channel.

Setup example:

	type Record struct {
		// First is the first name of the person.
		First string
		// Last is the last name of the person.
		Last string
		// ID is the ID
		ID string

		// Birth is the time the person was born.
		Birth time.Time
		// BirthTown is what town the person was born in.
		BirthTown string
		// State is the state the person was born in.
		BirthState string

		// Err is a data error on this Record.
		Err error
	}

	// SM implements stagedpipe.StateMachine.
	type SM[T []Record] struct {
		// idClient is a client for querying for information based on an ID.
		idClient *client.ID
	}


	// NewSM creates a new stagepipe.StateMachine.
	func NewSM(cli *client.ID) *SM[[]Record] {
		sm := &SM[[]Record]{
			idClient: cli,
		}
		return sm
	}

	// Close implements stagedpipe.StateMachine. It shuts down resources in the
	// StateMachine that are no longer needed. This is only safe after all entries
	// have been processed.
	func (s *SM[T]) Close() {// We don't need to do anything}

	// Start implements stagedpipe.StateMachine.Start().
	func (s *SM[T]) Start(ctx context.Context, req stagedpipe.Request[T]) stagedpipe.Request[T] {
		// This trims any excess space off of some string attributes.
		for i, rec := range req.Data {
			rec.First = strings.TrimSpace(rec.First)
			rec.Last = strings.TrimSpace(rec.Last)
			rec.ID = strings.TrimSpace(rec.ID)

			switch {
			case rec.First == "":
				req.Error = fmt.Errorf("Record.First cannot be empty")
				return req
			case rec.Last == "":
				req.Error = fmt.Errorf("Record.Last cannot be empty")
				return req
			case rec.ID == "":
				req.Error = fmt.Errorf("Record.ID cannot be empty")
				return req
			}
			// req.Data is []Record not []*Record, so we need to do a reassignment.
			req.Data[i] = rec
		}

		req.Next = s.IdVerifier
		return req
	}

	// IdVerifier takes a Request and adds it to a bulk request to be sent to the
	// identity service. This is the last stage of this pipeline.
	func (s *SM[T]) IdVerifier(ctx context.Context, req stagedpipe.Request[T]) stagedpipe.Request[T] {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		if err := s.idClient.Call(ctx, req.Data); err != nil {
			req.Error = err
			return req
		}
		req.Next = nil
		return req
	}

Above is a pipeline that takes in requests that contain a user record, cleans up the
record, and calls out to a identity service (faked) to get birth information.

To run the pipeline above is simple:

	sm := NewSM(&client.ID{})
	// Creates 10 pipelines that have concurrent processing for each stage of the
	// pipeline defined in "sm".
	p, err := stagedpipe.New(10, stagedpipe.StateMachine[[].Record](sm))
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// Make a new RequestGroup that I can send requests on.
	g := p.NewRequestGroup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start processing output.
	go func() {
		for rec := range g.Out() {
			if rec.Err != nil {
				cancel()
				g.Close(true)
				return
			}
			WriteToDB(rec)
		}
	}()

	// Read all the data from some source and write it to the pipeline.
	for _, recs := range ReadDataFromSource() {
		if ctx.Err() != nil {
			break
		}
		if err := g.Submit(ctx, Request{Data: recs}); err != nil {
			cancel()
			p.Close(true)
			// do something like return or panic()
		}
	}
	if ctx.Err() != nil {
		p.Close(true)
	}
	g.Close()

	// If this is the only use for the pipeline, then we can also call p.Close().
	// But we could be using this with multiple RequestGroup(s) or this is
	// processing for a server and in those cases we would not call p.Close().
*/
package stagedpipe

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johnsiilver/dynamics/demux"
	"github.com/johnsiilver/dynamics/method"
)

// Requests is a Request to be processed by a pipeline.
type Request[T any] struct {
	// Data is data that is processed in this Request.
	Data T

	// Err, if set, is an error for the Request. This type of error is for unrecoverable
	// errors in processing, not errors for the data being processed. For example, if it
	// can't communicate with a database or RPC service. For errors with the data itself,
	// add the error to the underlying data type as a separate error.
	Err error

	// Next is the next stage to be executed. Must be set at each stage of a StateMachine.
	// If set to nil, exits the pipeline.
	Next Stage[T]

	// groupNum is used to track what RequestGroup this Request belongs to for routing.
	groupNum uint64

	ctx                   context.Context
	queueTime, ingestTime time.Time
}

// StateMachine represents a state machine where the methods that implement Stage
// are the States and execution starts with the Start() method.
type StateMachine[T any] interface {
	// Start is the starting Stage of the StateMachine.
	Start(ctx context.Context, req Request[T]) Request[T]
	// Close stops the StateMachine.
	Close()
}

// Stage represents a function that executes at a given state.
type Stage[T any] func(ctx context.Context, req Request[T]) Request[T]

// PreProcessor is called before each Stage. If req.Err is set
// execution of the Request in the StateMachine stops.
type PreProcesor[T any] func(ctx context.Context, req Request[T]) Request[T]

// Pipelines provides access to a set of Pipelines that processes DBD information.
type Pipelines[T any] struct {
	in  chan Request[T]
	out chan Request[T]

	pipelines     []*pipeline[T]
	preProcessors []PreProcesor[T]
	sm            StateMachine[T]

	wg *sync.WaitGroup

	requestGroupNum atomic.Uint64
	demux           *demux.Demux[uint64, Request[T]]

	stats *stats
}

// Option is an option for the New() constructor.
type Option[T any] func(p *Pipelines[T]) error

// PreProcessors provides a set of functions that are called in order
// at each stage in the StateMachine. This is used to do work that is common to
// each stage instead of having to call the same code. Similar to http.HandleFunc
// wrapping techniques.
func PreProcessors[T any](p ...PreProcesor[T]) Option[T] {
	return func(pipelines *Pipelines[T]) error {
		pipelines.preProcessors = append(pipelines.preProcessors, p...)
		return nil
	}
}

// resetNext is a Preprocessor we use to reset req.Next at each stage. This prevents
// accidental infinite loop scenarios.
func resetNext[T any](ctx context.Context, req Request[T]) Request[T] {
	req.Next = nil
	return req
}

// New creates a new Pipelines object with "num" pipelines running in parallel.
// Each underlying pipeline runs concurrently for each stage. The first StateMachine.Start()
// in the list is the starting place for executions
func New[T any](num int, sm StateMachine[T], options ...Option[T]) (*Pipelines[T], error) {
	if num < 1 {
		return nil, fmt.Errorf("num must be > 0")
	}
	if sm == nil {
		return nil, fmt.Errorf("must provide a valid StateMachine")
	}

	in := make(chan Request[T], 1)
	out := make(chan Request[T], 1)
	stats := newStats()

	d, err := demux.New(
		out,
		func(r Request[T]) uint64 {
			return r.groupNum
		},
		func(r Request[T], err error) {
			log.Fatalf("bug: received %#+v and got demux error: %s", r, err)
		},
	)
	if err != nil {
		return nil, err
	}

	p := &Pipelines[T]{
		in:    in,
		out:   out,
		wg:    &sync.WaitGroup{},
		sm:    sm,
		stats: stats,
		demux: d,
		preProcessors: []PreProcesor[T]{
			resetNext[T],
		},
	}

	for _, o := range options {
		if err := o(p); err != nil {
			return nil, err
		}
	}

	pipelines := make([]*pipeline[T], 0, num)
	for i := 0; i < num; i++ {
		args := pipelineArgs[T]{
			in:            in,
			out:           out,
			num:           num,
			sm:            sm,
			preProcessors: p.preProcessors,
			stats:         stats,
		}

		_, err := newPipeline(args)
		if err != nil {
			close(in)
			return nil, err
		}
	}
	p.pipelines = pipelines

	return p, nil
}

// Close closes the ingestion of the Pipeline. No further Submit calls should be made.
// If called more than once Close will panic.
func (p *Pipelines[T]) Close() {
	close(p.in)

	go func() {
		p.wg.Wait()
		close(p.out)
		p.sm.Close()
	}()
}

// RequestGroup provides in and out channels to send a group of related data into
// the Pipelines and receive the processed data. This allows multiple callers to
// multiplex onto the same Pipelines. A RequestGroup is created with Pipelines.NewRequestGroup().
type RequestGroup[T any] struct {
	// id is the ID of the RequestGroup.
	id uint64
	// out is the channel the demuxer will use to send us output.
	out chan Request[T]
	// user is the channel that we give the user to receive output. We do a little
	// processing between receiveing on "out" and sending to "user".
	user chan Request[T]
	// wg is used to know when it is safe to close the output channel.
	wg sync.WaitGroup
	// p is the Pipelines object this RequestGroup is tied to.
	p *Pipelines[T]
}

// Close signals that the input is done and will wait for all Request objects to
// finish proceessing, then close the output channel. If drain is set, all requests currently still processing
// will be dropped. This should only be done if you receive an error and no longer intend
// on processing any output.
func (r *RequestGroup[T]) Close(drain bool) {
	if drain {
		go func() {
			for range r.out {
				r.wg.Done()
			}
		}()
	}
	r.wg.Wait()
	r.p.demux.RemoveReceiver(r.id)
}

// Submit submits a new Request into the Pipelines.
func (r *RequestGroup[T]) Submit(ctx context.Context, req Request[T]) error {
	// This let's the Pipelines object know it is receiving a new Request to process.
	r.p.wg.Add(1)
	// This tracks the request in the RequestGroup.
	r.wg.Add(1)

	req.groupNum = r.id
	req.ctx = ctx
	req.queueTime = time.Now()

	r.p.in <- req
	return nil
}

// Out returns a channel to receive Request(s) that have been processed. It is
// unsafe to close the output channel. Instead, use .Close() when all input has
// been sent and the output channel will close once all data has been processed.
// You MUST get all data from Out() until it closes, even if you run into an error.
// Otherwise the pipelines become stuck.
func (r *RequestGroup[T]) Out() <-chan Request[T] {
	return r.user
}

// NewRequestGroup returns a RequestGroup that can be used to process requests
// in this set of Pipelines.
func (p *Pipelines[T]) NewRequestGroup() *RequestGroup[T] {
	id := p.requestGroupNum.Add(1)
	r := RequestGroup[T]{
		id:   id,
		out:  make(chan Request[T], 1),
		user: make(chan Request[T], 1),
		p:    p,
	}
	p.demux.AddReceiver(id, r.out)

	go func() {
		defer close(r.user)
		for req := range r.out {
			r.wg.Done()
			r.user <- req
		}
	}()

	return &r
}

// Stats returns stats about all the running Pipelines.
func (p *Pipelines[T]) Stats() Stats {
	return p.stats.toStats()
}

// pipeline processes DBD entries.
type pipeline[T any] struct {
	sm            StateMachine[T]
	preProcessors []PreProcesor[T]

	stats *stats

	in          chan Request[T]
	out         chan Request[T]
	concurrency int
}

type pipelineArgs[T any] struct {
	in            chan Request[T]
	out           chan Request[T]
	num           int
	sm            StateMachine[T]
	preProcessors []PreProcesor[T]
	stats         *stats
}

// newPipeline creates a new Pipeline. A new Pipeline should be created for a new set of related
// requests.
func newPipeline[T any](args pipelineArgs[T]) (*pipeline[T], error) {
	p := &pipeline[T]{
		in:            args.in,
		out:           args.out,
		preProcessors: args.preProcessors,
		stats:         args.stats,
		sm:            args.sm,
	}

	p.concurrency = numStages(args.sm)

	if p.concurrency == 0 {
		return nil, fmt.Errorf("did not find any Public methods that implement Stages")
	}

	for i := 0; i < p.concurrency; i++ {
		go p.runner()
	}

	return p, nil
}

// Submit submits a request for processing.
func (p *pipeline[T]) runner() {
	for r := range p.in {
		r = p.processReq(r)
		p.out <- r
		p.calcExitStats(r)
	}
}

// processReq processes a single request through the pipeline.
func (p *pipeline[T]) processReq(r Request[T]) Request[T] {
	// Stat colllection.
	r.ingestTime = time.Now()
	queuedTime := time.Since(r.queueTime)

	p.stats.running.Add(1)
	setMin(&p.stats.ingestStats.min, int64(queuedTime))
	setMax(&p.stats.ingestStats.max, int64(queuedTime))
	p.stats.ingestStats.avgTotal.Add(int64(queuedTime))

	// Loop through all our states starting with p.sms[0].Start until we
	// get either an error or the Request.Next == nil
	// which indicates that the statemachine is done processing.
	stage := p.sm.Start
	for {
		// If the context has been cancelled, stop processing.
		if r.ctx.Err() != nil {
			r.Err = r.ctx.Err()
			return r
		}

		for _, pp := range p.preProcessors {
			r = pp(r.ctx, r)
			if r.Err != nil {
				return r
			}
		}
		r = stage(r.ctx, r)
		if r.Err != nil {
			return r
		}
		stage = r.Next

		if stage == nil {
			return r
		}
	}
}

// calcExitStats calculates the final stats when a Request exits the Pipeline.
func (p *pipeline[T]) calcExitStats(r Request[T]) {
	runTime := time.Since(r.ingestTime)

	p.stats.running.Add(-1)
	p.stats.completed.Add(1)

	setMin(&p.stats.min, int64(runTime))
	setMax(&p.stats.max, int64(runTime))
	p.stats.avgTotal.Add(int64(runTime))
}

func numStages[T any](sm StateMachine[T]) int {
	var sig Stage[T]
	count := 0
	for range method.MatchesSignature(reflect.ValueOf(sm), reflect.ValueOf(sig)) {
		count++
	}
	return count
}
