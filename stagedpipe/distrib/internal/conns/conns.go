package conns

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/johnsiilver/golib/ipc/uds"
	"github.com/johnsiilver/golib/ipc/uds/highlevel/chunk"
	stream "github.com/johnsiilver/golib/ipc/uds/highlevel/proto/stream"
	messages "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/version"
)

var bufPool = chunk.NewPool(100)

// Connect represents a connection made to the plugin from the controller.
type Connect struct {
	// OutputErr indicates that an error occurred in the connection.
	outputErr *atomic.Value // error
	// Input is the input channel for the pipeline. This is closed when the connection is closed
	// by the controller.
	Input chan *messages.Message
	// Output is the output channel for the pipeline. The sender must close this to close the connection
	// to the controller.
	Output chan *messages.Message
	// ConnType is the type of connection.
	ConnType messages.ConnType
}

// OutputErr returns an error if one occurred in sending on the connection.
func (c Connect) OutputErr() error {
	v := c.outputErr.Load()
	if v == nil {
		return nil
	}
	return v.(error)
}

// Conns is the input/output object for communicating with the pipeline sender.
// After calling New(), you can range over Connect() to receive new connections and use
// the Connect object to send and receive messages.
type Conns struct {
	server  *uds.Server
	connect chan Connect
}

// New creates a new Conns ojbect.
func New(socketPath string, uid, gid int) (*Conns, error) {
	if uid < 0 {
		return nil, fmt.Errorf("uid must be set")
	}
	if gid < 0 {
		return nil, fmt.Errorf("gid must be set")
	}

	cred, _, err := uds.Current()
	if err != nil {
		return nil, err
	}

	udsServer, err := uds.NewServer(socketPath, cred.UID.Int(), cred.GID.Int(), 0770)
	if err != nil {
		return nil, err
	}
	log.Println("uds server created at: ", socketPath)

	c := &Conns{
		server:  udsServer,
		connect: make(chan Connect, 1),
	}

	go func() {
		defer udsServer.Close()
		defer close(c.connect)

		for conn := range udsServer.Conn() {
			go c.handleConn(cred, conn)
		}
	}()

	return c, nil
}

func (c *Conns) Close() error {
	return c.server.Close()
}

// handleConn takes a new connection to the server and handles the input and output.
func (c *Conns) handleConn(cred uds.Cred, conn *uds.Conn) {
	defer conn.Close()

	streamer, err := stream.New(conn, stream.SharedPool(bufPool), stream.MaxSize(100*1024*1024))
	if err != nil {
		log.Printf("failed to create streamer: %s", err)
		return
	}

	if conn.Cred.UID.Int() != cred.UID.Int() && conn.Cred.UID.Int() != 0 {
		conn.WriteTimeout(2 * time.Second)
		sendError(streamer, errorf(messages.ErrorCode_ECNotAuthorized, true, "unauthorized user: user UID is %d, expect %d", conn.Cred.UID.Int(), cred.UID.Int()))
	}
	if conn.Cred.GID.Int() != cred.GID.Int() && conn.Cred.GID.Int() != 0 {
		conn.WriteTimeout(2 * time.Second)
		sendError(streamer, errorf(messages.ErrorCode_ECNotAuthorized, true, "unauthorized group: user GID is %d, expect %d", conn.Cred.GID.Int(), cred.GID.Int()))
		return
	}

	connMsg := &messages.Connect{}
	if err := streamer.Read(connMsg); err != nil {
		log.Printf("failed to read connect message: %s", err)
		conn.WriteTimeout(2 * time.Second)
		sendError(streamer, errorf(messages.ErrorCode_ECInternal, true, "initial message was not a Connect message"))
		return
	}

	// Send back our version.
	connMsg.Version = version.Semantic
	if err := streamer.Write(connMsg); err != nil {
		log.Printf("failed to write connect message: %s", err)
		conn.WriteTimeout(2 * time.Second)
		sendError(streamer, errorf(messages.ErrorCode_ECInternal, true, "failed to write connect message"))
		return
	}

	// Create a Connect and sent it our plugin to receive messages.
	connect := Connect{
		Input:     make(chan *messages.Message, 1),
		Output:    make(chan *messages.Message, 1),
		outputErr: &atomic.Value{},
	}

	switch connMsg.Type {
	case messages.ConnType_CNTRequestGroup:
		connect.ConnType = messages.ConnType_CNTRequestGroup
		c.connect <- connect
	default:
		log.Printf("invalid connection type: %v", connMsg.Type)
		close(connect.Input)
		return
	}

	ctx := context.Background()

	// Handle receiving messages from the controller.
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		defer close(connect.Input)
		c.handleRecv(ctx, streamer, connect.Input)
	}()
	c.handleSend(ctx, streamer, connect)
	<-recvDone
}

// handleRecv handles receiving messages from the controller and sending them to connect.Input.
func (c *Conns) handleRecv(ctx context.Context, streamer *stream.Client, writeTo chan<- *messages.Message) {
	for {
		m := &messages.Message{}
		if err := streamer.Read(m); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if errors.Is(err, io.EOF) {
				return
			}
			e := fmt.Errorf("failed to read message from controller: %w", err)
			writeTo <- errorf(messages.ErrorCode_ECInternal, true, e.Error())
			return
		}
		writeTo <- m
	}
}

func (c *Conns) handleSend(ctx context.Context, streamer *stream.Client, connect Connect) {
	for out := range connect.Output {
		// If we have an error, we need to drain the output channel until the caller stops sending.
		if connect.OutputErr() != nil {
			continue
		}

		if err := streamer.Write(out); err != nil {
			log.Println(fmt.Errorf("failed to write message to controller: %s", err))
			connect.outputErr.Store(err)
			return
		}
	}
}

// Connect returns a channel that will receive a Connect object when a new connection
// is made to the plugin.
func (io *Conns) Connect() <-chan Connect {
	return io.connect
}

// errorf creates an error message to send to the client. If alsoLog is true,
// the error will also be logged.
func errorf(code messages.ErrorCode, alsoLog bool, msg string, args ...any) *messages.Message {
	if alsoLog {
		lineInfo := ""
		_, file, no, ok := runtime.Caller(1)
		if ok {
			lineInfo = fmt.Sprintf("(called from %s#%d): ", file, no)
		}
		log.Printf(lineInfo+msg, args...)
	}

	return &messages.Message{
		Type: messages.MessageType_MTControl,
		Control: &messages.Control{
			Type: messages.ControlType_CTError,
			Error: &messages.Error{
				Code: code,
				Msg:  fmt.Sprintf(msg, args...),
			},
		},
	}
}

// sendError sends an error message to the client on w.
func sendError(streamer *stream.Client, m *messages.Message) error {
	if m.GetType() != messages.MessageType_MTControl || m.Control.GetType() != messages.ControlType_CTError {
		return fmt.Errorf("message type is not control or an error message")
	}

	return streamer.Write(m)
}
