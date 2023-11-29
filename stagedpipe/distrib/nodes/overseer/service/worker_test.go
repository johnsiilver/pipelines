package service

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/overseer/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/gopherfs/fs/io/mem/simple"
)

func TestHandleRegWorker(t *testing.T) {
	tests := []struct {
		desc       string
		fakeStream *fakeStream
		error      bool
	}{}
}

var pluginContent = []byte("plugin")

func TestGetPlugin(t *testing.T) {
	memFS := simple.New()
	memFS.WriteFile("/path/to/valid/plugin", pluginContent, 0770)
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
	memFS.WriteFile("/path/to/plugin1", []byte("plugin1"), 0770)
	memFS.WriteFile("/path/to/plugin2", []byte("plugin2"), 0770)
	memFS.WriteFile("/path/to/README", []byte("README"), 0770)
	memFS.WriteFile("/path/to/filewith.ext", []byte("ext"), 0770)
	// ../file doesn't matter, just using it to create the dir.
	memFS.WriteFile("path/to/subdir/file", []byte("subdir"), 0770)
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

// matcher is used to match an incoming value and if we see it to return an error.
type matcher[T proto.Message] struct {
	data     T
	matchErr error
}

// recvSend is used to match an incoming value and if we see it to stream of
// values back.
type recvSend struct {
	// workerRecv is the value we are looking for.
	workerRecv *pb.SubscribeOut
	// workerSend is the values we will stream back.
	workerSend []*pb.SubscribeIn
}

// fakeStream is used to fake the stream from a worker to the overseer.
type fakeStream struct {
	ctx context.Context

	sendMatchers []matcher[*pb.SubscribeOut]
	recvErr      matcher[*pb.SubscribeOut]
	recvSend     recvSend

	sent       []*pb.SubscribeOut
	fromWorker chan any // *pb.SubscribeIn or error

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

// Recv is used to receive a message from the worker.  It is then compared to a list of matchers.
func (f *fakeStream) Recv() (*pb.SubscribeIn, error) {
	msg := <-f.fromWorker
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

}
