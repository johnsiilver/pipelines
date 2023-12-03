package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/gopherfs/fs/io/mem/simple"
	"github.com/johnsiilver/broadcaster"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/internal/p2c"
	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/overseer/proto"
)

func TestHandleRegWorker(t *testing.T) {
	memFS := simple.New()
	// WTF: Why is visual studio continuing to add a "o" whenever I save a file.
	memFS.WriteFile("/path/to/plugin1", []byte("plugin1"), 0o770)
	memFS.WriteFile("/path/to/plugin2", []byte("plugin2"), 0o770)
	memFS.RO()

	tests := []struct {
		desc         string
		pluginDir    string
		init         *pb.RegisterInit
		fakeStream   *fakeStream
		expectedSend []*pb.SubscribeOut
		err          bool
	}{
		{
			desc: "Failure: init.Address doesn't parse",
			init: &pb.RegisterInit{
				Address: "1992.168.0.1", // This is wrong
				Type:    pb.NodeType_NT_WORKER,
			},
			fakeStream: &fakeStream{},
			err:        true,
		},
		{
			desc:      "Failure: pluginDir is not valid",
			pluginDir: "/this/does/not/exist",
			init: &pb.RegisterInit{
				Address: "192.168.0.1:7070",
				Type:    pb.NodeType_NT_WORKER,
			},
			fakeStream: &fakeStream{},
			err:        true,
		},
		{
			desc:      "Failure: Receive non-fin message from Worker",
			pluginDir: "/path/to/",
			init: &pb.RegisterInit{
				Address: "192.168.0.1:7070",
				Type:    pb.NodeType_NT_WORKER,
			},
			fakeStream: &fakeStream{
				ctx: context.Background(),
				fromWorker: []any{
					&pb.SubscribeIn{
						Message: &pb.SubscribeIn_Register{}, // Wrong message type.
					},
				},
			},
			err: true,
		},
		{
			desc:      "Success",
			pluginDir: "/path/to/",
			init: &pb.RegisterInit{
				Address: "192.168.0.1:7070",
				Type:    pb.NodeType_NT_WORKER,
			},
			fakeStream: &fakeStream{
				ctx: context.Background(),
				fromWorker: []any{
					&pb.SubscribeIn{
						Message: &pb.SubscribeIn_Register{
							Register: &pb.RegisterReq{
								Message: &pb.RegisterReq_Fin{
									Fin: &pb.RegisterFin{},
								},
							},
						},
					},
				},
			},
			expectedSend: []*pb.SubscribeOut{
				{
					Message: &pb.SubscribeOut_Register{
						Register: &pb.RegisterResp{
							Message: &pb.RegisterResp_LoadPlugin{
								LoadPlugin: &pb.LoadPlugin{
									Name:   "plugin1",
									Plugin: []byte("plugin1"),
								},
							},
						},
					},
				},
				{
					Message: &pb.SubscribeOut_Register{
						Register: &pb.RegisterResp{
							Message: &pb.RegisterResp_LoadPlugin{
								LoadPlugin: &pb.LoadPlugin{
									Name:   "plugin2",
									Plugin: []byte("plugin2"),
								},
							},
						},
					},
				},
				{
					Message: &pb.SubscribeOut_Register{
						Register: &pb.RegisterResp{
							Message: &pb.RegisterResp_Fin{
								Fin: &pb.RegisterFin{},
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		workerPool := p2c.New()

		s := &Server{
			pluginDir: test.pluginDir,
			plugFS:    memFS,
			workers:   workerPool,
			inTest:    true,
		}

		err := s.handleRegWorker(test.fakeStream, test.init)
		switch {
		case test.err && err == nil:
			t.Errorf("Test(%s): Expected err != nil", test.desc)
			continue
		case !test.err && err != nil:
			t.Errorf("Test(%s): Expected err == nil, got err == %v", test.desc, err)
			continue
		case test.err:
			continue
		}

		if len(test.fakeStream.sent) != len(test.expectedSend) {
			t.Errorf("Test(%s): expected %d messages to be sent, got %d", test.desc, len(test.expectedSend), len(test.fakeStream.sent))
			continue
		}
		for i := 0; i < len(test.fakeStream.sent); i++ {
			if diff := cmp.Diff(test.fakeStream.sent[i], test.expectedSend[i], protocmp.Transform()); diff != "" {
				t.Errorf("Test(%s): sent message(%d): -want/+got: %s", test.desc, i, diff)
			}
		}
	}
}

func TestHandleWorkerUpdates(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	origAck := ackTimeout
	ackTimeout = 2 * time.Second
	defer func() {
		ackTimeout = origAck
	}()

	tests := []struct {
		desc       string
		fakeStream *fakeExpectStream
		err        bool
	}{
		{
			desc: "Failure: context deadline",
			fakeStream: &fakeExpectStream{
				ctx: canceledCtx,
			},
			err: true,
		},

		{
			desc: "Failure: worker failed to ack the plugin update on time",
			fakeStream: &fakeExpectStream{
				ctx: context.Background(),
				expect: []*pb.SubscribeOut{
					{
						Message: &pb.SubscribeOut_UpdateWorker{
							UpdateWorker: &pb.UpdateWorker{
								Id: 0,
								Message: &pb.UpdateWorker_Plugin{
									Plugin: &pb.LoadPlugin{
										Name:   "plugin1",
										Plugin: []byte("plugin1"),
									},
								},
							},
						},
					},
				},
				resp: []any{
					nil,
				},
			},
			err: true,
		},

		{
			desc: "Failure: update was a zero value",
			fakeStream: &fakeExpectStream{
				ctx: context.Background(),
				expect: []*pb.SubscribeOut{
					{
						Message: &pb.SubscribeOut_UpdateWorker{
							UpdateWorker: &pb.UpdateWorker{
								Id: 0,
								// Missing message here, which means it will be a zero value.
							},
						},
					},
				},
				resp: []any{
					nil,
				},
			},
			err: true,
		},
		{
			desc: "Success",
			fakeStream: &fakeExpectStream{
				ctx: context.Background(),
				expect: []*pb.SubscribeOut{
					{
						Message: &pb.SubscribeOut_UpdateWorker{
							UpdateWorker: &pb.UpdateWorker{
								Id: 1,
								Message: &pb.UpdateWorker_Plugin{
									Plugin: &pb.LoadPlugin{
										Name:   "plugin1",
										Plugin: []byte("plugin1"),
									},
								},
							},
						},
					},
				},
				resp: []any{
					&pb.SubscribeIn{
						Message: &pb.SubscribeIn_Ack{
							Ack: &pb.Ack{Id: 1},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		s := &Server{
			updateWorkers: broadcaster.New[*pb.UpdateWorker](),
			inTest:        true,
		}
		if test.fakeStream != nil {
			test.fakeStream.Init()
		}

		var updateErr error
		var sendErr error
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer log.Println("handleWorkerUpdates done")
			updateErr = s.handleWorkerUpdates(test.fakeStream)
		}()

		time.Sleep(200 * time.Millisecond) // Give time for the broadcaster time to setup a receiver
		for _, out := range test.fakeStream.expect {
			msg, err := broadcaster.NewMessage[*pb.UpdateWorker](workerUpdatePlugs, out.GetUpdateWorker())
			if err != nil {
				panic(err)
			}

			log.Printf("before send: %v", msg.Data())
			if sendErr = s.updateWorkers.Send(msg); sendErr != nil {
				log.Println("Send error: ", sendErr)
				break
			}

			log.Println("broadcast update: ", msg, sendErr)
		}
		// Give time for everything to be sent or we will get a "send on closed channel".
		// TODO(jdoak): This and the time.Sleep above are hacky, replace at some point.
		time.Sleep(200 * time.Millisecond)
		test.fakeStream.Close()

		wg.Wait()
		switch {
		case updateErr == nil && test.err:
			t.Errorf("Test(%s): got err == nil, want err != nil", test.desc)
		case updateErr != nil && !test.err:
			t.Errorf("Test(%s): got err == %s, want err == nil", test.desc, updateErr)
		case updateErr != nil:
			continue
		}
	}
}

var pluginContent = []byte("plugin")

func TestGetPlugin(t *testing.T) {
	memFS := simple.New()
	memFS.WriteFile("/path/to/valid/plugin", pluginContent, 0o770)
	memFS.RO()

	tests := []struct {
		desc string
		path string
		err  bool
	}{
		{
			desc: "Valid Plugin",
			path: "/path/to/valid/plugin",
		},
		{
			desc: "Invalid Plugin",
			path: "/path/to/invalid/plugin",
			err:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			lp, err := getPlugin(memFS, test.path)
			switch {
			case err == nil && test.err:
				t.Errorf("Test(%s): got err == nil, want err != nil", test.desc)
				return
			case err != nil && !test.err:
				t.Errorf("Test(%s): got err == %s, want err == nil", test.desc, err)
				return
			case err != nil:
				return
			}

			if lp.Name != "plugin" {
				t.Errorf("Test(%s): got lp.Name == %s, want lp.Name == plugin", test.desc, lp.Name)
			}
			if !bytes.Equal(lp.Plugin, pluginContent) {
				t.Errorf("Test(%s): got lp.Plugin == %s, want lp.Data == pluginContent", test.desc, lp.Plugin)
			}
		})
	}
}

func TestGetPlugins(t *testing.T) {
	memFS := simple.New()
	memFS.WriteFile("/path/to/plugin1", []byte("plugin1"), 0o770)
	memFS.WriteFile("/path/to/plugin2", []byte("plugin2"), 0o770)
	memFS.WriteFile("/path/to/README", []byte("README"), 0o770)
	memFS.WriteFile("/path/to/filewith.ext", []byte("ext"), 0o770)
	// ../file doesn't matter, just using it to create the dir.
	memFS.WriteFile("path/to/subdir/file", []byte("subdir"), 0o770)
	memFS.RO()

	want := []*pb.LoadPlugin{
		{Name: "plugin1", Plugin: []byte("plugin1")},
		{Name: "plugin2", Plugin: []byte("plugin2")},
	}

	ch, err := getPlugins(memFS, "/path/to")
	if err != nil {
		t.Fatalf("getPlugins returned an unexpected error: %v", err)
	}

	got := []*pb.LoadPlugin{}
	for resp := range ch {
		if resp.Err != nil {
			t.Fatalf("Received an unexpected error: %v", resp.Err)
		}
		got = append(got, resp.Data)
	}

	if len(got) != len(want) {
		t.Fatalf("got len(got) == %d, want len(got) == %d", len(got), len(want))
	}

	for i := 0; i < len(got); i++ {
		if diff := cmp.Diff(got[i], want[i], protocmp.Transform()); diff != "" {
			t.Errorf("Test(%d): got diff (-got +want):\n%s", i, diff)
		}
	}
}

type fakeExpectStream struct {
	ctx    context.Context
	expect []*pb.SubscribeOut
	resp   []any
	pos    int

	recvCh chan any

	pb.Overseer_SubscribeServer
}

func (f *fakeExpectStream) Init() {
	if f.recvCh == nil {
		f.recvCh = make(chan any, 1)
	}
	if len(f.expect) != len(f.resp) {
		panic("cannot have a fakeExpectStream that has f.expect and f.resp of different lengths")
	}
}

func (f *fakeExpectStream) Send(msg *pb.SubscribeOut) error {
	want := f.expect[f.pos]
	f.pos++

	if !msg.EqualVT(want) {
		diff := cmp.Diff(want, msg, protocmp.Transform())
		panic(fmt.Sprintf("fakeExpectStream.Send: -want/+got:\n%s", diff))
	}
	f.recvCh <- f.resp[f.pos-1]
	log.Println("fakeExpectStream: message sent")

	return nil
}

// Recv is used to receive a message from the worker.
func (f *fakeExpectStream) Recv() (*pb.SubscribeIn, error) {
	log.Println("waiting inside Recv()")
	msg, ok := <-f.recvCh
	log.Println("message received")
	if !ok {
		log.Println("sent eof")
		return nil, io.EOF
	}
	if msg == nil {
		log.Println("saw nil msg")
		<-f.ctx.Done()
		return nil, f.ctx.Err()
	}

	switch v := msg.(type) {
	case *pb.SubscribeIn:
		return v, nil
	case error:
		return nil, v
	}

	panic(fmt.Sprintf("unknown type: %T", msg))
}

func (f *fakeExpectStream) Context() context.Context {
	return f.ctx
}

func (f *fakeExpectStream) Close() error {
	if f.recvCh != nil {
		close(f.recvCh)
	}

	return nil
}

// matcher is used to match an incoming value and if we see it to return an error.
type matcher[T proto.Message] struct {
	data     T
	matchErr error
}

// fakeStream is used to fake the stream from a worker to the overseer.
type fakeStream struct {
	ctx context.Context

	sendMatchers []matcher[*pb.SubscribeOut]

	sent       []*pb.SubscribeOut
	fromWorker []any // *pb.SubscribeIn or error
	fwCounter  atomic.Int64

	pb.Overseer_SubscribeServer
}

// Send is used to send a message to the worker. It is then comparted to a list of matchers.
// If it matches, the matcher's error is returned. If not, nil is returned.  We capture
// the sent message in f.sent.
func (f *fakeStream) Send(msg *pb.SubscribeOut) error {
	for _, m := range f.sendMatchers {
		if cmp.Equal(m.data, msg, protocmp.Transform()) {
			return m.matchErr
		}
	}
	f.sent = append(f.sent, msg)

	return nil
}

// Recv is used to receive a message from the worker.
func (f *fakeStream) Recv() (*pb.SubscribeIn, error) {
	count := int(f.fwCounter.Add(1)) - 1
	if count > len(f.fromWorker) {
		panic("test broken")
	}
	msg := f.fromWorker[count]
	switch v := msg.(type) {
	case *pb.SubscribeIn:
		return v, nil
	case error:
		return nil, v
	}

	panic(fmt.Sprintf("unknown type: %T", msg))
}

func (f *fakeStream) Context() context.Context {
	return f.ctx
}

func (f *fakeStream) Close() error {
	return nil
}
