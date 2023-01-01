/*
Package stagedpipe offers a generic, concurrent and parallel pipeline based on a
statemachine to process work. For N number of Stages in the StateMachine, N Stages
can concurrently be processed. You can run pipelines in parallel. So X Pipelines
with N Stages will have X*N Stages processing.

While useful for data processing, this library requires working knowledge of both
the a specific type of Go statemachine implementation and basic Go pipelining.

You can view the statemachine concepts here: https://www.youtube.com/watch?v=HxaD_trXwRE .
You can ignore how the parser works he is implementing.

You can see basic Go pipelining here: https://www.youtube.com/watch?v=oV9rvDllKEg .

Every pipeline will receive a Request, which is an interface type that must
implement our Request interface. Each Request is designed to be stack allocateable, meaning
it should not be implemented with a pointer. All relevant methods return a Request object
on any change that can be passed through the next states.

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

	// This would be in package client.  This is here for clarity
	type Record struct {
		// see testing/client.Record for the all the attributes.
	}

	// Request implements stagedpipe.Request.
	type Request struct {
		id uint64

		// rec is the Record for this Request that will be processed.
		rec client.Record

		// err holds an error if processing the Request had a problem.
		err error

		next stagepipe.
	}

	// Pre implements stagedpipe.Request.Pre().
	func (r Request) Pre() {}

	// Post implements stagedpipe.Request.Post().
	func (r Request) Post() {}

	// Error implements stagedpipe.Request.Error().
	func (r Request) Error() error {
		return r.err
	}

	// SetError implements stagedpipe.Request.SetError(). I also use this
	// to set the r.next to nil, meaning it will exit the pipeline, as I don't
	// want any processing after an error.
	func (r Request) SetError(e error) stagedpipe.Request {
		r.err = e
		r.next = nil
		return r
	}

	func (r Request) Next() statepipe.Stage {
		return r.next
	}

	func (r Request) SetNext(n statepipe.Stage) Request {
		r.next = n
		return r
	}

	func (r Request) GroupNum() uint64 {
		return r.id
	}

	func (r Request) SetGroupNum(u uint64) stagedpipe.Request {
		r.id = u
	}

	// ToConcrete converts a statepipe.Request to our concrete Request object.
	func ToConcrete(r stagedpipe.Request) (Request, error) {
		x, ok := r.(Request)
		if !ok {
			return Request{}, fmt.Errorf("unexpected type %T, expecting Request", r)
		}
		return x, nil
	}

	// MustConcrete is the same as ToConcrete except an error causes a panic.
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
		return &SM{idClient:   client,}
	}

	// Start implements stagedpipe.StateMachine.Start().
	func (s *SM) Start(ctx context.Context, req stagedpipe.Request) stagedpipe.Request {
		x, err := ToConcrete(req)
		if err != nil {
			req.SetError(fmt.Errorf("unexpected type %T, expecting Request", req))
			return req
		}

		x.rec.First = strings.TrimSpace(x.rec.First)
		x.rec.Last = strings.TrimSpace(x.rec.Last)
		x.rec.ID = strings.TrimSpace(x.rec.ID)

		switch {
		case x.rec.First == "":
			return x.SetError(fmt.Errorf("Record.First cannot be empty"))
		case x.rec.Last == "":
			return x.SetError(fmt.Errorf("Record.Last cannot be empty"))
		case x.rec.ID == "":
			return x.SetError(fmt.Errorf("Record.ID cannot be <= 0"))
		}

		x.SetNext(s.IdVerifier)
		return x
	}

	// IdVerifier takes a Request and adds it to a bulk request to be sent to the
	// identity service. This is the last stage of this pipeline.
	func (s *SM) IdVerifier(ctx context.Context, req Request) stagedpipe.Stage[R] {
		x := MustConcrete(req)

		idRec, err := s.idClient.Call(ctx, x.rec.ID)
		if err != nil {
			return x.SetError(err)
		}
		// I could do some more data verification, but this is getting long in the tooth.
		x.Birth = idRec.Birth
		x.BirthTown = idRec.BirthTown
		x.BirthState = idRec.BirthState

		x.SetNext(nil)
		return x
	}

Above is a pipeline that takes in requests that contain a user record, cleans up the
record, and calls out to a identity service (faked) to get birth information.

To run the pipeline above is simple:

	client := &client.ID{} // The identity service client
	sm := NewSM(client) // The statemachine created above

	// Creates 100 pipelines that have concurrent processing for each stage of the
	// pipeline defined in "sm".
	p, err := stagedpipe.New(100, []stagedpipe.StateMachine[Request]{sm})
	if err != nil {
		// Do something
	}

	// Make a new RequestGroup that I can send requests on.
	g0 := p.NewRequestGroup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start processing output.
	waitOut := wg.WaitGroup{}
	waitOut.Add(1)
	go func() {
		defer waitOut.Done()
		for rec := range g0.Out() {
			if rec.Err != nil {
				cancel()
				p.Close(true)
				return
			}
			WriteToDB(rec)
		}
	}()

	// Read all the data from some source and write it to the pipeline.
	for _, rec := ReadDataFromSource() {
		if ctx.Err() != nil {
			break
		}
		if err := g0.Submit(ctx, Request{rec: rec}); err != nil {
			cancel()
			p.Close(true)
			// do something like return or panic()
		}
	}
	if ctx.Err() != nil {
		p.Close(true)
	}
	g0.Close()

	waitOut.Wait()

	// If this is the only use for the pipeline, then we can also call p.Close().
	// But we could be using this with multiple RequestGroup(s) or this is
	// processing for a server and in those cases we would not call p.Close().

stagedpipe is very flexible, allowing for all kinds of combinations for functionality.
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
	"github.com/johnsiilver/pools/goroutines/pooled"
)

// Requests is a type constraint on any type that needs to be processed by
// the Pipeline.
type Request interface {
	// Pre is a function that must run before the Request is processed.
	// Normally used to lock a Mutex or increase a WaitGroup representing
	// a group of requests that are being processed. It is unsafe to
	// alter your data here.
	// This should process fast to avoid latency on submitting a Request.
	Pre()

	// Post is a function that must run after the Request is processed and
	// sent back on the channel returned by Pipelines.Out().Normally used to unlock
	// a Mutex or decrease a WaitGroup representing a group of requests that are
	// being processed.
	Post()

	// GroupNum returns the Request's group number. This is used internally to route
	// the Request out to the expected output channel.
	GroupNum() uint64

	// SetGroupNum set's the Request's gropu number. This is used internally to
	// set which output channel to send the Request.
	SetGroupNum(n uint64) Request

	// Error returns an error for the request. This type of error is for unrecoverable
	// or errors in processing, not errors for the data being processed. For example,
	// can't communicate with a database or RPC service. For errors with the data itself,
	// add the error to the underlying data type as a separate error.
	// Depending on stage layout, this may need to have a Mutex or similar protection.
	Error() error

	// SetErr sets an error for the request and returns a new Request object with
	// the error set. Depending on stage layout, this may need to have a Mutex or
	// similar protection.
	SetError(e error) Request

	// Next returns the next stage to be executed.
	Next() Stage

	// SetNext sets the next stage to be executed.
	SetNext(stage Stage) Request
}

// SetRequestsErr can be used to set a slice of Request objects to have the same error.
func SetRequestsErr(reqs []Request, e error) {
	for i, r := range reqs {
		r.SetError(e)
		reqs[i] = r
	}
}

type request struct {
	ctx context.Context
	req Request

	queueTime, ingestTime time.Time
}

// StateMachine represents a state machine where the methods that implement Stage
// are the States and execution starts with the Start() method.
type StateMachine interface {
	// Start is the starting Stage of the StateMachine.
	Start(ctx context.Context, req Request) Request
	// Close stops the StateMachine.
	Close()
}

// Stage represents a function that executes at a given state.
type Stage func(ctx context.Context, req Request) Request

// PreProcessor is called before each Stage. If error != nil, req.SetError() is called
// and execution of the Request in the StateMachine stops.
type PreProcesor func(ctx context.Context, req Request) error

// Pipelines provides access to a set of Pipelines that processes DBD information.
type Pipelines struct {
	in  chan request
	out chan Request

	pipelines     []*pipeline
	preProcessors []PreProcesor
	sms           []StateMachine

	wg *sync.WaitGroup

	requestGroupNum atomic.Uint64
	demux           *demux.Demux[uint64, Request]

	stats *stats
}

// Option is an option for the New() constructor.
type Option func(p *Pipelines) error

// PreProcessors provides a set of functions that are called in order
// at each stage in the StateMachine. This is used to do work that is common to
// each stage instead of having to call the same code. Similar to http.HandleFunc
// wrapping techniques.
func PreProcessors(p ...PreProcesor) Option {
	return func(pipelines *Pipelines) error {
		if pipelines.preProcessors != nil {
			return fmt.Errorf("cannot call StagePreProcessors() more than once")
		}
		pipelines.preProcessors = p
		return nil
	}
}

// New creates a new Pipelines object with "num" pipelines running in parallel.
// Each underlying pipeline runs concurrently for each stage. The first StateMachine.Start()
// in the list is the starting place for executions
func New(num int, sms []StateMachine, options ...Option) (*Pipelines, error) {
	if num < 1 {
		return nil, fmt.Errorf("num must be > 0")
	}
	if len(sms) == 0 {
		return nil, fmt.Errorf("must provide at least 1 StateMachine")
	}

	in := make(chan request, 1)
	out := make(chan Request, 1)
	stats := newStats()

	d, err := demux.New(
		out,
		func(r Request) uint64 {
			return r.GroupNum()
		},
		func(r Request, err error) {
			log.Fatalf("bug: received %#+v and got demux error: %s", r, err)
		},
	)
	if err != nil {
		return nil, err
	}

	p := &Pipelines{
		in:    in,
		out:   out,
		wg:    &sync.WaitGroup{},
		sms:   sms,
		stats: stats,
		demux: d,
	}

	for _, o := range options {
		if err := o(p); err != nil {
			return nil, err
		}
	}

	pipelines := make([]*pipeline, 0, num)
	for i := 0; i < num; i++ {
		args := pipelineArgs{
			pipelinesWG:   p.wg,
			in:            in,
			out:           out,
			num:           num,
			sms:           sms,
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
func (p *Pipelines) Close() {
	close(p.in)

	go func() {
		p.wg.Wait()
		close(p.out)
		for _, sm := range p.sms {
			sm.Close()
		}
	}()
}

// RequestGroup provides in and out channels to send a group of related data into
// the Pipelines and receive the processed data. This allows multiple callers to
// multiplex onto the same Pipelines. A RequestGroup is created with Pipelines.NewRequestGroup().
type RequestGroup struct {
	// id is the ID of the RequestGroup.
	id uint64
	// out is the channel the demuxer will use to send us output.
	out chan Request
	// user is the channel that we give the user to receive output. We do a little
	// processing between receiveing on "out" and sending to "user".
	user chan Request
	// wg is used to know when it is safe to close the output channel.
	wg sync.WaitGroup
	// p is the Pipelines object this RequestGroup is tied to.
	p *Pipelines
}

// Close signals that the input is done and will wait for all Request objects to
// finish proceessing, then close the output channel. If drain is set, all requests currently still processing
// will be dropped. This should only be done if you receive an error and no longer intend
// on processing any output.
func (r *RequestGroup) Close(drain bool) {
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
func (r *RequestGroup) Submit(ctx context.Context, req Request) error {
	if req == nil {
		return fmt.Errorf("Submit(): the Request cannot be nil")
	}
	// This let's the Pipelines object know it is receiving a new Request to process.
	r.p.wg.Add(1)
	// This tracks the request in the RequestGroup.
	r.wg.Add(1)

	ireq := request{
		ctx:       ctx,
		req:       req.SetGroupNum(r.id),
		queueTime: time.Now(),
	}

	ireq.req.Pre()
	r.p.in <- ireq
	return nil
}

// Out returns a channel to receive Request(s) that have been processed. It is
// unsafe to close the output channel. Instead, use .Close() when all input has
// been sent and the output channel will close once all data has been processed.
// You MUST get all data from Out() until it closes, even if you run into an error.
// Otherwise the pipelines become stuck.
func (r *RequestGroup) Out() <-chan Request {
	return r.user
}

// NewRequestGroup returns a RequestGroup that can be used to process requests
// in this set of Pipelines.
func (p *Pipelines) NewRequestGroup() *RequestGroup {
	id := p.requestGroupNum.Add(1)
	r := RequestGroup{
		id:   id,
		out:  make(chan Request, 1),
		user: make(chan Request, 1),
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
func (p *Pipelines) Stats() Stats {
	return p.stats.toStats()
}

// pipeline processes DBD entries.
type pipeline struct {
	pool *pooled.Pool

	// pipelinesWG is a waitgroup shared between all pipeline instances so that
	// Pipelines knows when all have nothing processing. This is decremented in
	// two places, runner() and waitForPromises().
	pipelinesWG *sync.WaitGroup

	sms           []StateMachine
	preProcessors []PreProcesor

	stats *stats

	in  chan request
	out chan Request
}

type pipelineArgs struct {
	in            chan request
	out           chan Request
	num           int
	pipelinesWG   *sync.WaitGroup
	sms           []StateMachine
	preProcessors []PreProcesor
	stats         *stats
}

// newPipeline creates a new Pipeline. A new Pipeline should be created for a new set of related
// requests.
func newPipeline(args pipelineArgs) (*pipeline, error) {
	p := &pipeline{
		in:            args.in,
		out:           args.out,
		preProcessors: args.preProcessors,
		pipelinesWG:   args.pipelinesWG,
		stats:         args.stats,
		sms:           args.sms,
	}

	n := 0
	for _, sm := range args.sms {
		n += numStages(sm)
	}

	if n == 0 {
		return nil, fmt.Errorf("did not find any Public methods that implement Stages")
	}

	// We are going to count how many stages we have and that is going to be our
	// static goroutine limit.
	var err error
	p.pool, err = pooled.New(n)
	if err != nil {
		return nil, err
	}

	go p.runner()

	return p, nil
}

// Submit submits a request for processing.
func (p *pipeline) runner() {
	defer func() {
		// Wait for all work to be done.
		p.pipelinesWG.Wait()
	}()

	wg := sync.WaitGroup{}
	for r := range p.in {
		r := r // Escape: would need to use Generics again.

		wg.Add(1)
		p.pool.Submit(
			r.ctx,
			func(ctx context.Context) {
				defer wg.Done()
				defer p.calcExitStats(r)
				r = p.processReq(r)

				p.out <- r.req
				p.pipelinesWG.Done()
			},
		)
	}

	wg.Wait()
}

// processReq processes a single request through the pipeline.
func (p *pipeline) processReq(r request) request {
	// Stat colllection.
	r.ingestTime = time.Now()
	queuedTime := time.Since(r.queueTime)

	p.stats.running.Add(1)
	setMin(&p.stats.ingestStats.min, int64(queuedTime))
	setMax(&p.stats.ingestStats.max, int64(queuedTime))
	p.stats.ingestStats.avgTotal.Add(int64(queuedTime))

	// Loop through all our states starting with p.sms[0].Start until we
	// get either we receive a Context error or the nextStateFn == nil
	// which indicates that the statemachine is done processing.
	stage := p.sms[0].Start
	for {
		// If the context has been cancelled, stop processing.
		if r.ctx.Err() != nil {
			x := r.req
			x.SetError(r.ctx.Err())
			r.req = x
			return r
		}

		for _, pp := range p.preProcessors {
			if err := pp(r.ctx, r.req); err != nil {
				x := r.req
				x.SetError(r.ctx.Err())
				r.req = x
				return r
			}
		}
		req := stage(r.ctx, r.req)
		if req == nil {
			panic("a Stage in your pipeline is broken, expected a stagepipe.Request, got nil")
		}
		r.req = req
		stage = r.req.Next()

		if stage == nil {
			return r
		}
	}
}

// calcExitStats calculates the final stats when a Request exits the Pipeline.
func (p *pipeline) calcExitStats(r request) {
	runTime := time.Since(r.ingestTime)

	p.stats.running.Add(-1)
	p.stats.completed.Add(1)

	setMin(&p.stats.min, int64(runTime))
	setMax(&p.stats.max, int64(runTime))
	p.stats.avgTotal.Add(int64(runTime))
}

func numStages(sm StateMachine) int {
	var sig Stage
	count := 0
	for range method.MatchesSignature(reflect.ValueOf(sm), reflect.ValueOf(sig)) {
		count++
	}
	return count
}

// BulkHolder is used to store objects up until we reach some number of them
// for a bulk submit operation. This is a utility function and is not required.
type BulkHolder[A any] struct {
	n      int
	mu     sync.Mutex
	holder []A
}

// NewBulkHolder creates a new BulkHolder that stores up to n values. n must be > 0.
// onAddReturn is called every time .Add() returns a bulk
func NewBulkHolder[A any](n int) *BulkHolder[A] {
	if n < 1 {
		panic("NewBulkHolder() cannot have a 0 value")
	}
	return &BulkHolder[A]{
		n:      n,
		holder: make([]A, 0, n),
	}
}

// Add adds an entry. If the internal []A has reached N entries, it
// returns []A and resets the internal []A. Otherwise it returns nil.
// This is thread-safe.
func (i *BulkHolder[A]) Add(a A) []A {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.holder = append(i.holder, a)

	if len(i.holder) == i.n {
		return i.get()
	}
	return nil
}

// Get gets the bulk Requests.
func (i *BulkHolder[A]) Get() []A {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.get()
}

// get returns the internal set of bulk Requests.
func (i *BulkHolder[A]) get() []A {
	b := i.holder
	i.holder = make([]A, 0, i.n)
	return b
}
