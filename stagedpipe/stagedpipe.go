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

Every pipeline will receive a Request, which is a generic type you define that must
implement our Request constraint.

Requests enter to the Pipelines via the Pipelines.Submit() method. That method routes
your Request to concurrent StateMachine objects.

StateMachine objects are a generic type you define that satisfies our
StateMachine[R Request] constraint.

You receive your Request objects once they have processed via the channel returned by
the Pipelines.Out() method.

Setup example:

	// see testing/client.Record for this.
	type Record struct {
		...
	}

	// Request implements stagedpipe.Request.
	type Request struct {
		// rec is the Record for this Request that will be processed.
		rec *client.Record

		// err holds an error if processing the Request had a problem.
		err error

		wg *sync.WaitGroup
	}

	// Pre implements stagedpipe.Request.Pre().
	func (r Request) Pre() {
		r.wg.Add(1)
	}

	// Post implements stagedpipe.Request.Post().
	func (r Request) Post() {
		r.wg.Done()
	}

	// HasWaiters implements stagedpipe.Request.HasWaiters().
	func (r Request) HasWaiters() bool {
		return false
	}

	// Wait implements stagedpipe.Request.Wait().
	func (r Request) Wait() {}

	// Error implements stagedpipe.Request.Error().
	func (r Request) Error() error {
		return r.err
	}

	// SetError implements stagedpipe.Request.SetError().
	func (r Request) SetError(e error) stagedpipe.Request {
		r.err = e
		return r
	}

	// ToConcrete converts a statepipe.Request to our concrete Request object.
	func ToConcrete[R stagedpipe.Request](r R) (Request, error) {
		x, ok := any(r).(Request)
		if !ok {
			return Request{}, fmt.Errorf("unexpected type %T, expecting Request", r)
		}
		return x, nil
	}

	// MustConcrete is the same as ToConcrete except an error causes a panic.
	func MustConcrete[R stagedpipe.Request](r R) Request {
		x, err := ToConcrete(r)
		if err != nil {
			panic(err)
		}
		return x
	}

	// SM implements stagedpipe.StateMachine.
	type SM[R stagedpipe.Request] struct {
		// idClient is a client for querying for information based on an ID.
		idClient *client.ID
	}

	// NewSM creates a new stagepipe.StateMachine.
	func NewSM[R stagedpipe.Request](client *client.ID) *SM[R] {
		return &SM[R]{idClient:   client,}
	}

	// Start implements stagedpipe.StateMachine.Start().
	func (s *SM[R]) Start(ctx context.Context, req R) stagedpipe.Stage[R] {
		x, err := ToConcrete(req)
		if err != nil {
			req.SetError(fmt.Errorf("unexpected type %T, expecting Request", req))
			return nil
		}

		x.rec.First = strings.TrimSpace(x.rec.First)
		x.rec.Last = strings.TrimSpace(x.rec.Last)
		x.rec.ID = strings.TrimSpace(x.rec.ID)

		switch {
		case x.rec.First == "":
			x.SetError(fmt.Errorf("Record.First cannot be empty"))
			return nil
		case x.rec.Last == "":
			x.SetError(fmt.Errorf("Record.Last cannot be empty"))
			return nil
		case x.rec.ID == "":
			x.SetError(fmt.Errorf("Record.ID cannot be <= 0"))
			return nil
		}

		return s.IdVerifier
	}

	// IdVerifier takes a Request and adds it to a bulk request to be sent to the
	// identity service. This is the last stage of this pipeline.
	func (s *SM[R]) IdVerifier(ctx context.Context, req R) stagedpipe.Stage[R] {
		x := MustConcrete(req)

		idRec, err := s.idClient.Call(ctx, x.rec.ID)
		if err != nil {
			x.SetError(err)
		}
		// I could do some more data verification, but this is getting long in the tooth.
		rec.Birth = idRec.Birth
		rec.BirthTown = idRec.BirthTown
		rec.BirthState = idRec.BirthState

		return nil
	}

Above is a pipeline that takes in requests that contain a user record, cleans up the
record, and calls out to a identity service (faked) to get birth information. Any errors
are recorded into the Request.rec. If we have an error contacting the service, it is
recorded directly into the Request.

To run the pipeline above is simple:

	client := &client.ID{} // The identity service client
	sm := NewSM[Request](client) // The statemachine created above

	// Creates 100 pipelines that have concurrent processing for each stage of the
	// pipeline defined in "sm".
	p, err := stagedpipe.New(100, []stagedpipe.StateMachine[Request]{sm})
	if err != nil {
		// Do something
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for rec := range p.Out() {
			if rec.Err != nil {
				cancel()
				p.Close()
				p.Drain()
				return
			}
			WriteToDB(rec)
		}
	}()

	// Read all the data from some source,
	wg := sync.WaitGroup{}

	for _, rec := ReadDataFromSource() {
		if ctx.Err() != nil {
			break
		}
		r := Request{rec: rec, &wg}
		p.Submit(ctx, rec)
	}
	if ctx.Err() != nil {
		p.Close()
	}

	wg.Wait()

Above we use .Close() mechanics, but that isn't strictly necessary. It is possible
to construct a Pipeline's that can multiplex information from multiple sources and
those can be routed back to their origin and never needs to be closed.
It is all about what information is included in the Request.

A more complicated setup that handles pipelines doing bulk requests to services
and that deals with []Request that need to know when all Request objects have exited the
pipelines: see the stagedpipe_test.go file. Note: this is not for the feint of heart,
it will take some time to understand this.

Pipelines is very flexible, allowing for all kinds of combinations for functionality.
*/
package stagedpipe

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/johnsiilver/dynamics/method"
	"github.com/johnsiilver/pipelines/stagedpipe/internal/queue"
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

	// HasWaiters indicates if the Request has any promises. If so it exits
	// the pipeline into a queue to wait for the promises to finish.
	HasWaiters() bool

	// Wait is called on items in our exit queue to finish processing. This is
	// only called if HasWaiters() returns true.
	Wait()

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
}

// SetRequestsErr can be used to set a slice of Request objects to have the same error.
func SetRequestsErr(reqs []Request, e error) {
	for i, r := range reqs {
		reqs[i] = r.SetError(e)
	}
}

type request[R Request] struct {
	ctx context.Context
	req R

	queueTime, ingestTime time.Time
}

// StateMachine represents a state machine where the methods that implement Stage
// are the States and execution starts with the Start() method.
type StateMachine[R Request] interface {
	// Start is the starting Stage of the StateMachine.
	Start(ctx context.Context, req R) Stage[R]
	// Close stops the StateMachine.
	Close()
}

// Stage represents a function that executes at a given state.
type Stage[R Request] func(ctx context.Context, req R) Stage[R]

// PreProcessor is called before each Stage. If error != nil, req.SetError() is called
// and execution of the Request in the StateMachine stops.
type PreProcesor[R Request] func(ctx context.Context, req R) error

// Pipelines provides access to a set of Pipelines that processes DBD information.
type Pipelines[R Request] struct {
	in  chan request[R]
	out chan R

	pipelines     []*pipeline[R]
	preProcessors []PreProcesor[R]
	sms           []StateMachine[R]

	wg *sync.WaitGroup

	stats *stats
}

// Option is an option for the New() constructor.
type Option[R Request] func(p *Pipelines[R]) error

// PreProcessors provides a set of functions that are called in order
// at each stage in the StateMachine. This is used to do work that is common to
// each stage instead of having to call the same code. Similar to http.HandleFunc
// wrapping techniques.
func PreProcessors[R Request](p ...PreProcesor[R]) Option[R] {
	return func(pipelines *Pipelines[R]) error {
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
func New[R Request](num int, sms []StateMachine[R], options ...Option[R]) (Pipelines[R], error) {
	if num < 1 {
		return Pipelines[R]{}, fmt.Errorf("num must be > 0")
	}
	if len(sms) == 0 {
		return Pipelines[R]{}, fmt.Errorf("must provide at least 1 StateMachine")
	}

	in := make(chan request[R], num)
	out := make(chan R, num)
	stats := newStats()

	p := Pipelines[R]{
		in:    in,
		out:   out,
		wg:    &sync.WaitGroup{},
		sms:   sms,
		stats: stats,
	}

	for _, o := range options {
		if err := o(&p); err != nil {
			return Pipelines[R]{}, err
		}
	}

	pipelines := make([]*pipeline[R], 0, num)
	for i := 0; i < num; i++ {
		args := pipelineArgs[R]{
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
			return Pipelines[R]{}, err
		}
	}
	p.pipelines = pipelines

	return p, nil
}

// Close closes the ingestion of the Pipeline. No further Submit calls should be made.
// If called more than once Close will panic.
func (p Pipelines[R]) Close() {
	close(p.in)

	go func() {
		p.wg.Wait()
		close(p.out)
		for _, sm := range p.sms {
			sm.Close()
		}
	}()
}

// Drain is used to drain the out channel in case of some error in processing.
// This will only work if .Close() is called, otherwise this will block forever.
func (p *Pipelines[R]) Drain() {
	for range p.out {
	}
}

// Submit submits a request to the processing Pipelines.
func (p Pipelines[R]) Submit(ctx context.Context, req R) {
	p.wg.Add(1)

	r := request[R]{
		ctx:       ctx,
		req:       req,
		queueTime: time.Now(),
	}

	r.req.Pre()
	p.in <- r
}

// Out returns a channel that you receive the processed Request on.
func (p Pipelines[R]) Out() chan R {
	return p.out
}

// Stats returns stats about all the running Pipelines.
func (p Pipelines[R]) Stats() Stats {
	return p.stats.toStats()
}

// pipeline processes DBD entries.
type pipeline[R Request] struct {
	pool *pooled.Pool

	// pipelinesWG is a waitgroup shared between all pipeline instances so that
	// Pipelines knows when all have nothing processing. This is decremented in
	// two places, runner() and waitForPromises().
	pipelinesWG *sync.WaitGroup

	sms           []StateMachine[R]
	preProcessors []PreProcesor[R]

	// promiseQuee holds Request objects until all promises have been completed
	// before pushing to the .out channel.
	promiseQueue *queue.Queue[request[R]]

	stats *stats

	in  chan request[R]
	out chan R
}

type pipelineArgs[R Request] struct {
	in            chan request[R]
	out           chan R
	num           int
	qSize         int
	pipelinesWG   *sync.WaitGroup
	sms           []StateMachine[R]
	preProcessors []PreProcesor[R]
	stats         *stats
}

// newPipeline creates a new Pipeline. A new Pipeline should be created for a new set of related
// requests.
func newPipeline[R Request](args pipelineArgs[R]) (*pipeline[R], error) {
	p := &pipeline[R]{
		in:            args.in,
		out:           args.out,
		promiseQueue:  queue.New[request[R]](args.qSize),
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

// Submit submits a DBD for processing.
func (p *pipeline[R]) runner() {
	go p.waitForPromises()
	defer p.promiseQueue.Close()

	wg := sync.WaitGroup{}
	for r := range p.in {
		r := r

		wg.Add(1)
		p.pool.Submit(
			r.ctx,
			func(ctx context.Context) {
				defer wg.Done()
				r = p.processReq(r)
				switch {
				case r.req.HasWaiters():
					p.promiseQueue.Push(r)
				default:
					p.out <- r.req
					p.pipelinesWG.Done()
				}
			},
		)
	}

	wg.Wait()
}

// processReq processes a single request through the pipeline.
func (p *pipeline[R]) processReq(r request[R]) request[R] {
	// Stat colllection.
	r.ingestTime = time.Now()
	queuedTime := time.Since(r.queueTime)

	p.stats.running.Add(1)
	setMin(p.stats.ingestStats.min, int64(queuedTime))
	setMax(p.stats.ingestStats.max, int64(queuedTime))
	p.stats.ingestStats.avgTotal.Add(int64(queuedTime))

	// Loop through all our states starting with p.Clean until we
	// get either we receive a Context error or the nextStateFn == nil
	// which indicates that the statemachine is done processing.
	stateFn := p.sms[0].Start
	for {
		// If the context has been cancelled, stop processing.
		if r.ctx.Err() != nil {
			r.req = any(r.req.SetError(r.ctx.Err())).(R)
			return r
		}

		for _, pp := range p.preProcessors {
			if err := pp(r.ctx, r.req); err != nil {
				r.req = any(r.req.SetError(err)).(R)
				return r
			}
		}

		nextStateFn := stateFn(r.ctx, r.req)
		if nextStateFn == nil {
			return r
		}
		stateFn = nextStateFn
	}
}

// calcExitStats calculates the final stats when a Request exits the Pipeline.
func (p *pipeline[R]) calcExitStats(r request[R]) {
	runTime := time.Since(r.ingestTime)

	p.stats.running.Add(-1)
	p.stats.completed.Add(1)

	setMin(p.stats.min, int64(runTime))
	setMax(p.stats.max, int64(runTime))
	p.stats.avgTotal.Add(int64(runTime))
}

// waitForPromises() waits for all Request promises to finish and then sends
// the results to the .out channel.
func (p *pipeline[R]) waitForPromises() {
	for {
		r, ok := p.promiseQueue.Pop()
		if !ok {
			return
		}

		func() { // Simply here to force a defer.
			defer p.pipelinesWG.Done()

			r.req.Wait()

			p.calcExitStats(r)
			p.out <- r.req
			r.req.Post()
		}()
	}
}

func numStages[R Request](sm StateMachine[R]) int {
	var sig Stage[R]
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
