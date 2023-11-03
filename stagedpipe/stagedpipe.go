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

You define a StateMachine object that satisfies the StateMachine interface. These states
represent the stages of the pipeline. A single StateMachine will process a single
Request at a time, allowing use of your StateMachine's internal objects without mutexes.
All StateMachine methods that implement a Stage MUST BE PUBLIC.

A RequestGroup represents a set of related Request(s) that should be processed together.
A new RequestGroup can be created with Pipelines.NewRequestGroup().

Requests enter the Pipelines via the RequestGroup.Submit() method. Requests are received
with RequestGroup.Out(), which returns a channel of Request(s).

Multiple RequestGroup(s) can send into the Pipelines for processing, as everything is
muxed into the Pipelines and demuxed out to the RequestGroup.Out() channel.

There is a provided CLI application called `stagedpipe-cli“ located in the `tools/` directory
that can be used to generate all the boilerplate you see below for a working example.

Here is an example (you can run it here: https://go.dev/play/p/ZOUzWgRb1oh:

	// Data is the data we will pass on a Request into our Pipelines. We could pass
	// just []client.Record, but after using this package in practice it was found
	// that using a Struct wrapper that could be expanded is a best practice that
	// can save hours of pipeline and test restructure. By convention, we call this
	// Data.
	type Data struct {
		Records []client.Record
	}

	// NewRequest returns a new stagedpipe.Request object for use in the Pipelines.
	// By convention we always have NewRequest() function.  If NewRequest can return
	// an error, we also include a MustNewRequest().
	func NewRequest(ctx context.Context, data Data) stagedpipe.Request[Data] {
		return stagedpipe.Request[Data]{
			Ctx:  ctx,
			Data: data,
		}
	}

	// SM implements stagedpipe.StateMachine. It holds all our states for the pipeline.
	type SM struct {
		// idClient is a client for querying for information based on an ID.
		idClient *client.ID
	}

	// NewSM creates a new stagepipe.StateMachine from SM.
	func NewSM(cli *client.ID) *SM {
		sm := &SM{
			idClient: cli,
		}
		return sm
	}

	// Close implements stagedpipe.StateMachine.Close(). It shuts down resources in the
	// StateMachine that are no longer needed. This is only safe after all entries
	// have been processed.
	func (s *SM) Close() {
		//We don't need to do anything
	}

	// Start implements stagedpipe.StateMachine.Start(). It accepts a Request and
	// does some fixes on the names and ID to remove spaces
	// and then sends it on to the IDVerifier stage.
	func (s *SM) Start(req stagedpipe.Request[Data]) stagedpipe.Request[Data] {
		for i, rec := range req.Data.Records {
			// This trims any excess space off of some string attributes.
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
			req.Data.Records[i] = rec
		}

		req.Next = s.IdVerifier // Next Stage to go to.
		return req
	}

	// IdVerifier is a stage takes a Request and adds it to a request to be sent to the
	// identity service. This is the last stage of this pipeline.
	func (s *SM) IdVerifier(req stagedpipe.Request[Data]) stagedpipe.Request[Data] {
		ctx, cancel := context.WithTimeout(req.Ctx, 2*time.Second)
		defer cancel()

		recs, err := s.idClient.Call(ctx, req.Data.Records)
		if err != nil {
			req.Err = err
			return req
		}
		req.Data.Records = recs

		req.Next = nil // Signifies there are no more stages.
		return req
	}

	// RunPipline creates and starts 10 Pipelines using our StateMachine "SM" defined above.
	// Because there are 2 stages, 2 * 10 goroutines will be created.
	func RunPipeline() (*stagedpipe.Pipelines[Data], error) {
		sm := NewSM(&client.ID{})
		return stagedpipe.New("example pipeline", 10, stagedpipe.StateMachine[Data](sm))
	}

Above is a pipeline that takes in requests that contain user records, cleans up the
records, and calls out to a identity service (faked) to get birth information.

To run the pipeline above is simple:

	func main() {
		// Setup our pipelines.
		p, err := RunPipeline()
		if err != nil {
			panic(err)
		}
		defer p.Close()

		// Make a new RequestGroup that to send requests on.
		g := p.NewRequestGroup()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start processing output that will be read in and write it to our Database(faked).
		go func() {
			for req := range g.Out() {
				if req.Err != nil {
					cancel()
					g.Close()
					continue
				}
				WriteToDB(req.Data.Records)
			}
		}()

		// Read all the data from some source and send it into the pipeline.
		for recs := range ReadDataFromSource() {
			if ctx.Err() != nil {
				break
			}
			if err := g.Submit(NewRequest(ctx, Data{Records: recs})); err != nil {
				cancel()
				g.Close()
				panic(err)
			}
		}
		g.Close()
	}

	// WriteToDB is just a standin for writing output to a DB.
	func WriteToDB(recs []client.Record) {
		for _, rec := range recs {
			log.Printf("Wrote: %+v", rec)
		}
	}

	// ReadDataFromSource is just a standin for some storage of data (disk, RPC, DB).
	func ReadDataFromSource() chan []client.Record {
		ch := make(chan []client.Record, 1)
		go func() {
			defer close(ch)

			nameGenerator := namegenerator.NewNameGenerator(time.Now().UTC().UnixNano())

			idSrc := 0
			for i := 0; i < 100; i++ {
				recs := []client.Record{}
				for x := 0; x < 100; x++ {
					id := strconv.Itoa(idSrc)
					idSrc++
					rec := client.Record{First: nameGenerator.Generate(), Last: nameGenerator.Generate(), ID: id}
					recs = append(recs, rec)
				}
				ch <- recs
			}
		}()

		return ch
	}
*/
package stagedpipe

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johnsiilver/dynamics/demux"
	"github.com/johnsiilver/dynamics/method"
)

const (
	cyclicErr = "cyclic"
)

// Error represents a typed error that this package can return.
// Not all errors are of this type.
type Error struct {
	// Type is the type of error.
	Type string
	// Msg is the message of the error.
	Msg string
}

// Error returns the Error type and message.
func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Msg)
}

// IsErrCyclic returns true if the error is a cyclic error. A cyclic error is when
// a stage is called more than once in a single Request. This is only returned
// if the DAG() option is set.
func IsErrCyclic(err error) bool {
	if err == nil {
		return false
	}
	t, ok := err.(Error)
	if !ok {
		return false
	}
	return t.Type == cyclicErr
}

var seenStagesPool = sync.Pool{
	New: func() any {
		return &seenStages{}
	},
}

type seenStages []string

func (s *seenStages) seen(stage string) bool {
	log.Printf("seenStages.seen(): %s", stage)
	for _, st := range *s {
		if st == stage {
			log.Println(true)
			return true
		}
	}

	n := append(*s, stage)
	*s = n
	log.Println(false)
	return false
}

func (s *seenStages) callTrace() string {
	out := strings.Builder{}
	for i, st := range *s {
		if i != 0 {
			out.WriteString(" -> ")
		}
		out.WriteString(st)
	}
	return out.String()
}

func (s *seenStages) reset() *seenStages {
	n := (*s)[:0]
	s = &n
	return s
}

// Requests is a Request to be processed by a pipeline.
type Request[T any] struct {
	// queueTime and ingestTime hold the times when the Request was queued and ingested.
	queueTime, ingestTime time.Time

	// Ctx is a Context scoped for this requestor set of requests.
	Ctx context.Context

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

	// seenStages tracks what stages have been called in this Request. This is used to
	// detect cyclic errors. If nil, cyclic errors are not checked.
	seenStages *seenStages

	// groupNum is used to track what RequestGroup this Request belongs to for routing.
	groupNum uint64
}

// StateMachine represents a state machine where the methods that implement Stage
// are the States and execution starts with the Start() method.
type StateMachine[T any] interface {
	// Start is the starting Stage of the StateMachine.
	Start(req Request[T]) Request[T]
	// Close stops the StateMachine.
	Close()
}

// Stage represents a function that executes at a given state.
type Stage[T any] func(req Request[T]) Request[T]

// PreProcessor is called before each Stage. If req.Err is set
// execution of the Request in the StateMachine stops.
type PreProcesor[T any] func(req Request[T]) Request[T]

// Pipelines provides access to a set of Pipelines that processes DBD information.
type Pipelines[T any] struct {
	name string

	in  chan Request[T]
	out chan Request[T]

	pipelines     []*pipeline[T]
	preProcessors []PreProcesor[T]
	sm            StateMachine[T]
	// subStages is used to record the number of stages in objects that aren't the
	// StateMachine.
	subStages    int
	delayWarning time.Duration

	wg *sync.WaitGroup

	requestGroupNum atomic.Uint64
	demux           *demux.Demux[uint64, Request[T]]

	stats *stats
	ss    bool
}

// Option is an option for the New() constructor.
type Option[T any] func(p *Pipelines[T]) error

// DAG makes the StateMachine a Directed Acyllic Graph. This means that no Stage
// can be called more than once in a single Request. If a Stage is called more than
// once, the request will exit with a cyclic error that can be detected with IsErrCyclic().
func DAG[T any]() Option[T] {
	return func(p *Pipelines[T]) error {
		p.ss = true
		return nil
	}
}

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

// DelayWarning will send a log message whenever pushing entries to the out channel
// takes longer than the supplied time.Duration. Not setting this results will result
// in no warnings. Useful when chaining Pipelines and figuring out where something is stuck.
func DelayWarning[T any](d time.Duration) Option[T] {
	return func(pipelines *Pipelines[T]) error {
		if d < 0 {
			return fmt.Errorf("cannot provide a DelayWarning < 0")
		}
		pipelines.delayWarning = d
		return nil
	}
}

// CountSubStages is used when the StateMachine object does not hold all the Stage(s).
// This allows you to design multiple pipleines that use the same data object but will
// be executed as a single pipeline. CountSubStages is used to correctly calculate
// the concurrency. Without this, only stages in the StateMachine object will be counted
// toward the concurrency count.
func CountSubStages[T any](subStageObj ...T) Option[T] {
	return func(pipelines *Pipelines[T]) error {
		for _, obj := range subStageObj {
			pipelines.subStages += numStages[T](obj)
		}
		return nil
	}
}

// resetNext is a Preprocessor we use to reset req.Next at each stage. This prevents
// accidental infinite loop scenarios.
func resetNext[T any](req Request[T]) Request[T] {
	req.Next = nil
	return req
}

// New creates a new Pipelines object with "num" pipelines running in parallel.
// Each underlying pipeline runs concurrently for each stage. The first StateMachine.Start()
// in the list is the starting place for executions
func New[T any](name string, num int, sm StateMachine[T], options ...Option[T]) (*Pipelines[T], error) {
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
		name:  name,
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
			name:          name,
			id:            i,
			in:            in,
			out:           out,
			num:           num,
			sm:            sm,
			subStages:     p.subStages,
			preProcessors: p.preProcessors,
			stats:         stats,
			delayWarning:  p.delayWarning,
			ss:            p.ss,
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
	// out is the channel the demuxer will use to send us output.
	out chan Request[T]
	// user is the channel that we give the user to receive output. We do a little
	// processing between receiveing on "out" and sending to "user".
	user chan Request[T]
	// p is the Pipelines object this RequestGroup is tied to.
	p *Pipelines[T]
	// wg is used to know when it is safe to close the output channel.
	wg sync.WaitGroup
	// id is the ID of the RequestGroup.
	id uint64
}

// Close signals that the input is done and will wait for all Request objects to
// finish proceessing, then close the output channel. The owner of the RequestGroup
// is still required to pull all entries out of the RequestGroup via .Out() and until
// that occurs, Close() will not return.
func (r *RequestGroup[T]) Close() {
	r.wg.Wait()
	r.p.demux.RemoveReceiver(r.id) // This closes the input channel into the Pipelines object
}

// Submit submits a new Request into the Pipelines. A Request with a nil Context will
// cause a panic.
func (r *RequestGroup[T]) Submit(req Request[T]) error {
	if req.Ctx == nil {
		return errors.New("Request.Ctx cannot be nil")
	}

	req.groupNum = r.id
	req.queueTime = time.Now()

	// This let's the Pipelines object know it is receiving a new Request to process.
	r.p.wg.Add(1)
	// This tracks the request in the RequestGroup.
	r.wg.Add(1)
	select {
	case <-req.Ctx.Done():
		r.p.wg.Done()
		r.wg.Done()
		return req.Ctx.Err()
	case r.p.in <- req:
	}

	return nil
}

// Out returns a channel to receive Request(s) that have been processed. It is
// unsafe to close the output channel. Instead, use .Close() when all input has
// been sent and the output channel will close once all data has been processed.
// You MUST get all data from Out() until it closes, even if you run into an error.
// Otherwise the pipelines become stuck.
func (r *RequestGroup[T]) Out() chan Request[T] {
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
	name string
	id   int

	sm            StateMachine[T]
	preProcessors []PreProcesor[T]

	ss    bool
	stats *stats

	in           chan Request[T]
	out          chan Request[T]
	concurrency  int
	delayWarning time.Duration
}

type pipelineArgs[T any] struct {
	sm            StateMachine[T]
	in            chan Request[T]
	out           chan Request[T]
	stats         *stats
	ss            bool
	name          string
	preProcessors []PreProcesor[T]
	id            int
	num           int
	subStages     int
	delayWarning  time.Duration
}

// newPipeline creates a new Pipeline. A new Pipeline should be created for a new set of related
// requests.
func newPipeline[T any](args pipelineArgs[T]) (*pipeline[T], error) {
	p := &pipeline[T]{
		name:          args.name,
		id:            args.id,
		in:            args.in,
		out:           args.out,
		preProcessors: args.preProcessors,
		stats:         args.stats,
		sm:            args.sm,
		ss:            args.ss,
		delayWarning:  args.delayWarning,
	}

	p.concurrency = numStages[T](args.sm) + args.subStages

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
	id := fmt.Sprintf("%s-%d", p.name, p.id)
	var tick *time.Ticker
	if p.delayWarning != 0 {
		tick = time.NewTicker(p.delayWarning)
	}
	for r := range p.in {
		r = p.processReq(r)
		p.calcExitStats(r)
		if p.delayWarning != 0 {
			for {
				tick.Reset(p.delayWarning)
				select {
				case p.out <- r:
				case <-tick.C:
					log.Printf("pipeline(%s) is having output delays exceeding %v", id, p.delayWarning)
					continue
				}
				break
			}
		} else {
			p.out <- r
		}
	}
}

// processReq processes a single request through the pipeline.
func (p *pipeline[T]) processReq(r Request[T]) Request[T] {
	// Stat colllection.
	r.ingestTime = time.Now()
	queuedTime := time.Since(r.queueTime)
	if p.ss {
		r.seenStages = seenStagesPool.Get().(*seenStages).reset()
		defer func() {
			seenStagesPool.Put(r.seenStages)
			r.seenStages = nil
		}()
	}

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
		if r.Ctx.Err() != nil {
			r.Err = r.Ctx.Err()
			return r
		}

		if r.seenStages != nil {
			if r.seenStages.seen(methodName(stage)) {
				r.Err = Error{Type: cyclicErr, Msg: r.seenStages.callTrace()}
				return r
			}
		}

		for _, pp := range p.preProcessors {
			r = pp(r)
			if r.Err != nil {
				return r
			}
		}
		r = stage(r)
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

func numStages[T any](sm any) int {
	var sig Stage[T]
	count := 0
	for range method.MatchesSignature(reflect.ValueOf(sm), reflect.ValueOf(sig)) {
		count++
	}
	return count
}

func methodName(method any) string {
	return runtime.FuncForPC(reflect.ValueOf(method).Pointer()).Name()
}
