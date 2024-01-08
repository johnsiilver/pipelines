// Package client provides a client to a worker node.
package client

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/go-json-experiment/json"
	"google.golang.org/grpc"

	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/worker/proto"
)

// Worker is a client to a worker node.
type Worker struct {
	conn   *grpc.ClientConn
	client pb.WorkerClient
}

// New creates a new Worker client. Use GetRequestGroup() to get a RequestGroup to the worker
// for a specific plugin.
func New(addrPort netip.AddrPort) (*Worker, error) {
	conn, err := grpc.Dial(addrPort.String(), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	client := pb.NewWorkerClient(conn)

	return &Worker{conn: conn, client: client}, nil
}

// GetRequestGroup requests a pipeline RequestGroup to be processed by the worker.
func GetRequestGroup[D any](ctx context.Context, worker *Worker, plugin string, config any) (*RequestGroup[D], error) {
	workerClient, err := worker.client.RequestGroup(ctx)
	if err != nil {
		return nil, err
	}

	return newRequestGroup[D](workerClient, plugin, config)
}

// RequestGroup is a stream to a worker's RequestGroup call.
type RequestGroup[D any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	err    atomic.Value // error

	client pb.Worker_RequestGroupClient
	recvCh chan StreamMsg[D]
	sendMu sync.Mutex
	// rDataPtr is set to true if R is a pointer type.
	rDataPtr bool
	// recvClosed is closed when the recvLoop() is done.
	recvClosed  chan struct{}
	closeCalled bool // protected by sendMu

	// counter is used to count how many messages have been sent and received.
	// Every time we send, this is incremented.  Every time we receive, this is
	// decremented.  When everything is done sending and the connection breaks,
	// this should be 0.
	counter atomic.Int64
}

func newRequestGroup[D any](client pb.Worker_RequestGroupClient, plugin string, config any) (*RequestGroup[D], error) {
	ctx, cancel := context.WithCancel(context.Background())

	rg := &RequestGroup[D]{
		ctx:        ctx,
		cancel:     cancel,
		client:     client,
		recvCh:     make(chan StreamMsg[D], 1),
		recvClosed: make(chan struct{}),
	}

	var rType D
	rg.rDataPtr = reflect.TypeOf(rType).Kind() == reflect.Ptr

	b, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal config into JSON: %w", err)
	}

	err = client.Send(
		&pb.RequestGroupReq{
			Message: &pb.RequestGroupReq_OpenGroup{
				OpenGroup: &pb.OpenGroupReq{
					Plugin: plugin,
					Config: b,
				},
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to send OpenGroup message: %w", err)
	}
	// log.Println("client: sent OpenGroup message")
	// time.Sleep(2 * time.Second)
	go rg.recvLoop()

	return rg, nil
}

func (r *RequestGroup[D]) setErr(err error) error {
	if err == nil {
		return nil
	}
	r.cancel()
	r.err.CompareAndSwap(nil, err)
	return err
}

func (r *RequestGroup[D]) getErr() error {
	e := r.err.Load()
	if e == nil {
		return nil
	}
	return e.(error)
}

// Close closes the stream to the worker.
func (r *RequestGroup[D]) Close(ctx context.Context) error {
	// log.Println("client: Close() called")
	if err := r.getErr(); err != nil {
		// log.Println("client: Close() returning error:", err)
		return err
	}

	// log.Println("client: Close() before sendMu.Lock()")
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	defer r.cancel()

	r.closeCalled = true

	// log.Println("client: Close() before client.CloseSend()")
	if err := r.client.CloseSend(); err != nil {
		// log.Println("client: Close() returning error:", err)
		return r.setErr(fmt.Errorf("failed to close stream: %w", err))
	}
	// log.Println("client: Close() after client.CloseSend()")

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.recvClosed:
		if r.counter.Load() != 0 {
			return fmt.Errorf("did not receive all messages sent to worker")
		}
		return nil
	}
}

// Send sends data to the worker. This is a blocking call.
func (r *RequestGroup[D]) Send(seqNum uint32, data D) error {
	if err := r.getErr(); err != nil {
		return err
	}

	r.sendMu.Lock()
	defer r.sendMu.Unlock()

	if r.closeCalled {
		return r.setErr(fmt.Errorf("cannot Send() on a closed stream"))
	}

	b, err := json.Marshal(data)
	if err != nil {
		return r.setErr(fmt.Errorf("cannot marshal data into JSON: %w", err))
	}

	r.counter.Add(1)
	err = r.client.Send(
		&pb.RequestGroupReq{
			Message: &pb.RequestGroupReq_Data{
				Data: &pb.Data{Seq: seqNum, Data: b},
			},
		},
	)
	if err != nil {
		return r.setErr(fmt.Errorf("failed to send data message: %w", err))
	}
	return nil
}

// StreamMsg is a message received from the worker.
type StreamMsg[T any] struct {
	// Msg is the message received from the worker.
	Msg T
	// Err is an error received from the worker.
	Err error
}

// Recv returns a channel that will receive messages from the worker.
func (r *RequestGroup[D]) Recv() chan StreamMsg[D] {
	return r.recvCh
}

func (r *RequestGroup[D]) recvLoop() {
	defer close(r.recvClosed)
	defer close(r.recvCh)
	for {
		msg, err := r.client.Recv()
		if err != nil {
			if err == io.EOF {
				return
			}
			err = r.setErr(fmt.Errorf("failed to receive message: %w", err))
			r.recvCh <- StreamMsg[D]{Err: err}
			return
		}

		var data D
		if r.rDataPtr {
			if err := json.Unmarshal(msg.GetData().GetData(), data); err != nil {
				err = r.setErr(fmt.Errorf("failed to unmarshal data: %w", err))
				r.recvCh <- StreamMsg[D]{Err: err}
			}
		} else {
			if err := json.Unmarshal(msg.GetData().GetData(), &data); err != nil {
				err = r.setErr(fmt.Errorf("failed to unmarshal data: %w", err))
				r.recvCh <- StreamMsg[D]{Err: err}
			}
		}
		r.counter.Add(-1)
		r.recvCh <- StreamMsg[D]{Msg: data}
	}
}
