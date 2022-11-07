package stagedpipe_test

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johnsiilver/pipelines/stagedpipe"
	"github.com/johnsiilver/pipelines/stagedpipe/testing/client"
)

// RecordSet is a set of records that belong together.
type RecordSet struct {
	Records []*client.Record

	wg *sync.WaitGroup
}

// Wait waits for all records in a RecordSet to be processed.
func (r RecordSet) Wait() {
	r.wg.Wait()
}

// NewRequests returns a bunch of Request objects for all Record(s) in RecordSet.
func (r RecordSet) NewRequests() []Request {
	reqs := make([]Request, 0, len(r.Records))
	for _, rec := range r.Records {
		reqs = append(reqs, NewRequest(rec, r.wg))
	}
	return reqs
}

// Request represents our request object. You can find a blank one
// to fill out in file "requestTemplate" located in this directory.
type Request struct {
	// rec is the Record for this Request that will be processed.
	rec *client.Record

	// err holds an error if processing the Request had a problem.
	err error

	// recordSetWait is used to track this Record Request as part of
	// a RecordSet. This allows the RecordSet.Wait() to wait till all
	// records part of the group have finished processing.
	recordSetWait *sync.WaitGroup

	// idVerifierWait is used to wait on a bulk request for identification
	// information from another service.
	idVerifierWait *sync.WaitGroup
}

// NewRequest creates a new Request.
func NewRequest(rec *client.Record, recordSetWait *sync.WaitGroup) Request {
	return Request{
		rec:            rec,
		recordSetWait:  recordSetWait,
		idVerifierWait: &sync.WaitGroup{},
	}
}

// Pre implements stagedpipe.Request.Pre().
func (r Request) Pre() {
	r.recordSetWait.Add(1)
}

// Post implements stagedpipe.Request.Post().
func (r Request) Post() {
	r.recordSetWait.Done()
}

// HasWaiters implements stagedpipe.Request.HasWaiters().
func (r Request) HasWaiters() bool {
	return true
}

// Wait implements stagedpipe.Request.Wait().
func (r Request) Wait() {
	r.idVerifierWait.Wait()
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

// ToConcrete converts a statepipe.Request to our concrete Request object.
func ToConcrete[R stagedpipe.Request](r R) (Request, error) {
	x, ok := any(r).(Request)
	if !ok {
		return Request{}, fmt.Errorf("unexpected type %T, expecting Request", r)
	}
	return x, nil
}

// MustConcrete is the same as ToConcrete expect an error causes a panic.
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

	bulk *stagedpipe.BulkHolder[Request]

	closeOnce  sync.Once
	closed     chan struct{}
	bulkSendIn chan []Request
}

// NewSM creates a new stagepipe.StateMachine.
func NewSM[R stagedpipe.Request](client *client.ID) *SM[R] {
	sm := &SM[R]{
		idClient:   client,
		bulk:       stagedpipe.NewBulkHolder[Request](1000),
		closed:     make(chan struct{}),
		bulkSendIn: make(chan []Request, 1),
		closeOnce:  sync.Once{},
	}
	go sm.bulkSender()
	return sm
}

// Close stops all running goroutines. This is only safe after all entries have
// been processed.
func (s *SM[R]) Close() {
	s.closeOnce.Do(func() { close(s.closed) })
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
	x.idVerifierWait.Add(1)
	bulkReady := s.bulk.Add(x)

	if bulkReady != nil {
		go s.sendIDs(bulkReady)
	}

	return nil
}

// bulkSender sits in the background and calls sendIDs() if we have
// 2 Milliseconds go by or we have 1000 entries.
func (s *SM[R]) bulkSender() {
	const tickerTime = 2 * time.Millisecond
	t := time.NewTicker(tickerTime)
	for {
		t.Reset(tickerTime)
		select {
		// We have 1000 entries to process.
		case reqs := <-s.bulkSendIn:
			s.sendIDs(reqs)
		// Close() was called.
		case <-s.closed:
			reqs := s.bulk.Get()
			if len(reqs) > 0 {
				s.sendIDs(reqs)
			}
			return
		// Timer hit, so let's see if we have any bulk entries
		// and if so, let's send them. This features allows us to
		// use the pipeline without ever closing it so we send < 1000
		// entries if we have reached the end of the data stream and
		// let's us multi-plex multiple streams for processing.
		case <-t.C:
			reqs := s.bulk.Get()
			if len(reqs) > 0 {
				s.sendIDs(reqs)
			}
		}
	}
}

// sendIDs sends a bulk query to the identity service and updates all the Request
// objects.
func (s *SM[R]) sendIDs(bulk []Request) {
	defer func() {
		for _, r := range bulk {
			r.idVerifierWait.Done()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ids := make([]string, 0, len(bulk))
	for _, r := range bulk {
		ids = append(ids, r.rec.ID)
	}

	idRecs, err := s.idClient.Bulk(ctx, ids)
	if err != nil {
		setRequestsErr(bulk, err)
		return
	}

	if len(idRecs) != len(bulk) {
		setRequestsErr(bulk, fmt.Errorf("idClient.Call() did not return expected results"))
		return
	}

	for i := 0; i < len(idRecs); i++ {
		id := idRecs[i]
		rec := bulk[i].rec

		if id.Err != nil {
			if id.Err == client.ErrNotFound {
				rec.Err = client.ErrNotFound
				continue
			}
			bulk[i].SetError(err)
			continue
		}

		// I could do some more data verification, but this is getting long in the tooth.
		rec.Birth = id.Birth
		rec.BirthTown = id.BirthTown
		rec.BirthState = id.BirthState
	}
}

// setRequestsErr can be used to set a slice of Request objects to have the same error.
func setRequestsErr(reqs []Request, e error) {
	for i, r := range reqs {
		reqs[i] = r.SetError(e).(Request)
	}
}

func genRequests(n int) RecordSet {
	rs := RecordSet{
		Records: make([]*client.Record, 0, n),
		wg:      &sync.WaitGroup{},
	}

	for i := 0; i < n; i++ {
		s := strconv.Itoa(i + 1)
		rec := client.Record{First: s, Last: s, ID: s}
		rs.Records = append(rs.Records, &rec)
	}
	return rs
}

const day = 24 * time.Hour

func TestPipelines(t *testing.T) {
	t.Parallel()

	rs1 := genRequests(1)
	rs1000 := genRequests(1000)

	tests := []struct {
		desc      string
		recordSet RecordSet
		fc        *client.ID
	}{
		{
			desc:      "1 entry only",
			recordSet: rs1,
			fc:        &client.ID{},
		},

		{
			desc:      "1000 entries",
			recordSet: rs1000,
			fc:        &client.ID{},
		},
	}

	for _, test := range tests {
		ctx := context.Background()
		sm := NewSM[Request](test.fc)
		p, err := stagedpipe.New(10, []stagedpipe.StateMachine[Request]{sm})
		if err != nil {
			panic(err)
		}
		defer p.Drain() // Must be after close.
		defer p.Close()

		go func() {
			// For this exercise we need to drain the Out channel so things continue
			// to process, but we can just wait for the RecordSet as a whole to finish
			// instead of doing things with each Request.
			for range p.Out() {
			}
		}()

		for _, req := range test.recordSet.NewRequests() {
			p.Submit(ctx, req)
		}

		// Wait for all the Record in the RecordSet to finish processing.
		test.recordSet.Wait()

		for _, rec := range test.recordSet.Records {
			if rec.Birth.IsZero() {
				t.Fatalf("TestPipeline(%s): requests are not being processed", test.desc)
			}
			numID, err := strconv.Atoi(rec.ID)
			if err != nil {
				panic(err)
			}

			wantBirth := time.Time{}.Add(time.Duration(numID) * day)
			if !rec.Birth.Equal(wantBirth) {
				t.Fatalf("TestPipeline(%s): requests are not being processed correctly, ID %d gave Birthday of %v, want %v", test.desc, numID, rec.Birth, wantBirth)
			}
		}
	}
}

func BenchmarkPipeline(b *testing.B) {
	b.ReportAllocs()

	rs := genRequests(100000)
	fc := &client.ID{}

	ctx := context.Background()
	sm := NewSM[Request](fc)

	b.StopTimer()
	func() {
		p, err := stagedpipe.New(runtime.NumCPU(), []stagedpipe.StateMachine[Request]{sm})
		if err != nil {
			panic(err)
		}

		defer func() {
			p.Close()
			p.Drain() // Must be after close.
		}()

		go func() {
			// For this exercise we need to drain the Out channel so things continue
			// to process, but we can just wait for the RecordSet as a whole to finish
			// instead of doing things with each Request.
			for range p.Out() {
			}
		}()

		b.ResetTimer()
		b.StartTimer()
		for _, req := range rs.NewRequests() {
			p.Submit(ctx, req)
		}

		// Wait for all the Record in the RecordSet to finish processing.
		rs.Wait()
		b.StopTimer()
	}()
}
