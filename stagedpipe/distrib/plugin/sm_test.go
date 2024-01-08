package plugin

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/conns"
	messages "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"
)

type Data struct {
	MsgNum uint64 // Set by rgConfig.Setup()
	Item   uint64
}

type rgConfig struct {
	num *atomic.Uint64

	Conn bool // Simply used to test it gets set.
}

func (r rgConfig) Init(ctx context.Context) error {
	return nil
}

func (r rgConfig) Setup(ctx context.Context, data Data) (Data, error) {
	data.MsgNum = r.num.Add(1)
	return data, nil
}

func (r rgConfig) Close() error {
	return nil
}

func TestRecvMsgs(t *testing.T) {
	tests := []struct {
		desc         string
		inputMsgs    []*messages.Message
		expectedMsgs []*messages.Message
		runErr       bool
		expectedErr  bool
	}{
		{
			desc: "Error: first message is not a control message",
			inputMsgs: []*messages.Message{
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":1}`),
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":2}`),
					},
				},
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type: messages.ControlType_CTFin,
					},
				},
			},
			runErr: true,
		},
		{
			desc: "Error: first message is not CTConfig message",
			inputMsgs: []*messages.Message{
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type: messages.ControlType_CTFin,
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":1}`),
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":2}`),
					},
				},
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type: messages.ControlType_CTFin,
					},
				},
			},
			runErr: true,
		},
		{
			desc: "Error: first message has malformed JSON config",
			inputMsgs: []*messages.Message{
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type:   messages.ControlType_CTConfig,
						Config: []byte(`{"Conn":true`), // missing closing brace
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":1}`),
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":2}`),
					},
				},
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type: messages.ControlType_CTFin,
					},
				},
			},
			runErr: true,
		},
		{
			desc: "Error: cancel message in the middle of the stream",
			inputMsgs: []*messages.Message{
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type:   messages.ControlType_CTConfig,
						Config: []byte(`{"Conn":true}`),
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":1}`),
					},
				},
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type: messages.ControlType_CTCancel,
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":2}`),
					},
				},
			},
			runErr: true,
		},
		{
			desc: "Error: finish message in the middle of the stream",
			inputMsgs: []*messages.Message{
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type:   messages.ControlType_CTConfig,
						Config: []byte(`{"Conn":true}`),
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":1}`),
					},
				},
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type: messages.ControlType_CTFin,
					},
				},
				// This message will not be received because Fin closes the stream.
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":2}`),
					},
				},
			},
			expectedMsgs: []*messages.Message{
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":1}`),
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":2}`),
					},
				},
			},
			expectedErr: true,
		},
		{
			desc: "Success",
			inputMsgs: []*messages.Message{
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type:   messages.ControlType_CTConfig,
						Config: []byte(`{"Conn":true}`),
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":1}`),
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":2}`),
					},
				},
				{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type: messages.ControlType_CTFin,
					},
				},
			},
			expectedMsgs: []*messages.Message{
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":1}`),
					},
				},
				{
					Type: messages.MessageType_MTData,
					Req: &messages.Request{
						Data: []byte(`{"Item":2}`),
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			connect := conns.Connect{
				Input:    make(chan *messages.Message, 1),
				Output:   make(chan *messages.Message, 1),
				ConnType: messages.ConnType_CNTRequestGroup,
			}

			newConfig := func() RGConfig[Data] {
				return &rgConfig{num: &atomic.Uint64{}}
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sm := newRecvMsgs[Data](newConfig, connect.Input)

			wg := sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, msg := range test.inputMsgs {
					select {
					case <-ctx.Done():
						return
					case connect.Input <- msg:
					}
					log.Println("sent input")
				}
				close(connect.Input)
			}()

			got := []*messages.Message{}
			wg.Add(1)
			go func() {
				defer wg.Done()

				// We do this because the Ouptut is not terminated in the state machine.
				for msg := range sm.Msgs {
					got = append(got, msg)
				}
			}()

			err := sm.Run(ctx)
			switch {
			case err == nil && test.runErr:
				t.Fatalf("Test(%s): got err == nil, want err != nil", test.desc)
			case err != nil && !test.runErr:
				t.Fatalf("Test(%s): got err == %s, want err == nil", test.desc, err)
			case err != nil:
				cancel()
				return
			}
			wg.Wait()

			if len(test.expectedMsgs) != len(got) {
				if test.expectedErr {
					return
				}
				t.Fatalf("Test(%s): got len(got) == %d, want len(got) == %d", test.desc, len(got), len(test.expectedMsgs))
			}

			for i, msg := range test.expectedMsgs {
				if diff := cmp.Diff(msg, got[i], protocmp.Transform()); diff != "" {
					t.Fatalf("Test(%s): msg %d: diff: %s", test.desc, i, diff)
				}
			}
		})
	}
}
