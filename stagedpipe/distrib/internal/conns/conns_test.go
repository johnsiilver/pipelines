package conns

import (
	"log"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	messages "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"

	"github.com/johnsiilver/golib/ipc/uds"
	stream "github.com/johnsiilver/golib/ipc/uds/highlevel/proto/stream"
	"google.golang.org/protobuf/proto"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func TestHandleSendRecv(t *testing.T) {
	tests := []struct {
		desc       string
		inputMsgs  []proto.Message
		outputMsgs []*messages.Message
		writeErr   bool
		errMsg     bool
	}{
		{
			desc: "Error: first message is not a Connect message",
			inputMsgs: []proto.Message{
				&messages.Message{Req: &messages.Request{Data: []byte(`{"Item":1}`)}},
				&messages.Message{Req: &messages.Request{Data: []byte(`{"Item":2}`)}},
				&messages.Message{Req: &messages.Request{Data: []byte(`{"Item":3}`)}},
				&messages.Message{Req: &messages.Request{Data: []byte(`{"Item":4}`)}},
			},
			writeErr: true,
		},
		{
			desc: "Success: type ConnType_CNTRequestGroup",
			inputMsgs: []proto.Message{
				&messages.Connect{Type: messages.ConnType_CNTRequestGroup},
				&messages.Message{
					Type: messages.MessageType_MTControl,
					Control: &messages.Control{
						Type:   messages.ControlType_CTConfig,
						Config: []byte{},
					},
				},
				&messages.Message{Type: messages.MessageType_MTData, Req: &messages.Request{Data: []byte(`{"Item":1}`)}},
				&messages.Message{Type: messages.MessageType_MTData, Req: &messages.Request{Data: []byte(`{"Item":2}`)}},
				&messages.Message{Type: messages.MessageType_MTData, Req: &messages.Request{Data: []byte(`{"Item":3}`)}},
				&messages.Message{Type: messages.MessageType_MTData, Req: &messages.Request{Data: []byte(`{"Item":4}`)}},
				&messages.Message{Type: messages.MessageType_MTControl, Control: &messages.Control{Type: messages.ControlType_CTFin}},
			},
			outputMsgs: []*messages.Message{
				{Type: messages.MessageType_MTData, Req: &messages.Request{Data: []byte(`{"Item":1,"MsgNum":1}`)}},
				{Type: messages.MessageType_MTData, Req: &messages.Request{Data: []byte(`{"Item":2,"MsgNum":2}`)}},
				{Type: messages.MessageType_MTData, Req: &messages.Request{Data: []byte(`{"Item":3,"MsgNum":3}`)}},
				{Type: messages.MessageType_MTData, Req: &messages.Request{Data: []byte(`{"Item":4,"MsgNum":4}`)}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			log.Println("Running test: ", test.desc)

			p := path.Join(os.TempDir(), "stagedpipe_conns_test.socket"+strconv.Itoa(os.Getpid()))
			conns, err := New(p, os.Getuid(), os.Getgid())
			if err != nil {
				t.Fatalf("Test(%s): New() failed: %s", test.desc, err)
			}
			defer os.Remove(p)

			wg := sync.WaitGroup{}
			got := []*messages.Message{}
			var errMsg *messages.Message

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer log.Println("conns.Connect() closed")
				for conn := range conns.Connect() {
					log.Println("got conn")
					conn := conn

					wg.Add(1)
					go func() {
						defer wg.Done()
						defer log.Println("conn.Input closed")
						for msg := range conn.Input {
							if msg.Type == messages.MessageType_MTControl && msg.GetControl().GetType() == messages.ControlType_CTError {
								errMsg = msg
								continue
							}
							got = append(got, msg)
						}
					}()

					// Send message to the client.
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer close(conn.Output)
						for _, msg := range test.outputMsgs {
							conn.Output <- msg
						}
					}()
				}
			}()

			client, err := uds.NewClient(p, os.Getuid(), os.Getgid(), []os.FileMode{0770})
			if err != nil {
				t.Fatalf("Test(%s): uds.NewClient() failed: %s", test.desc, err)
			}
			defer client.Close()

			sc, err := stream.New(client, stream.MaxSize(100*1024*1024))
			if err != nil {
				t.Fatalf("Test(%s): stream.New() failed: %s", test.desc, err)
			}

			// Read messages from the server.
			wg.Add(1)
			clientGot := []*messages.Message{}
			go func() {
				defer wg.Done()
				for {
					msg := &messages.Message{}
					if err := sc.Read(msg); err != nil {
						return
					}
					clientGot = append(clientGot, msg)
				}
			}()

			var writeErr error
			for _, msg := range test.inputMsgs {
				writeErr = sc.Write(msg)
				if writeErr != nil {
					break
				}
				// Give the server time to close the connection.
				time.Sleep(100 * time.Millisecond)
			}
			log.Println("wrote all messages")
			client.Close()

			log.Println("waiting for sync.Wait()")
			conns.Close()
			wg.Wait()
			log.Println("done waiting for sync.Wait()")

			switch {
			case writeErr == nil && test.writeErr:
				t.Fatalf("Test(%s): got writeErr == nil, want writeErr != nil", test.desc)
			case writeErr != nil && !test.writeErr:
				t.Fatalf("Test(%s): got writeErr == %s, want writeErr == nil", test.desc, err)
			case writeErr != nil:
				return
			}

			switch {
			case errMsg != nil && !test.errMsg:
				t.Fatalf("Test(%s): got errMsg != %s, want errMsg == nil", test.desc, errMsg)
			case errMsg == nil && test.errMsg:
				t.Fatalf("Test(%s): got errMsg == nil, want errMsg != nil", test.desc)
			case test.errMsg:
				return
			}

			if len(got) != len(test.inputMsgs)-1 {
				t.Fatalf("Test(%s): got len(got) == %d, want len(got) == %d", test.desc, len(got), len(test.inputMsgs))
			}

			for i, msg := range test.inputMsgs[1:] {
				want := msg.(*messages.Message)
				got := got[i]
				if got.Type != want.Type {
					t.Fatalf("Test(%s): got got[%d].Type == %s, want got[%d].Type == %s", test.desc, i, got.Type, i, want.Type)
				}
			}

			for i, msg := range test.outputMsgs {
				want := msg
				got := clientGot[i]
				if got.Type != want.Type {
					t.Fatalf("Test(%s): got clientGot[%d].Type == %s, want clientGot[%d].Type == %s", test.desc, i, got.Type, i, want.Type)
				}
			}
		})
	}
}
