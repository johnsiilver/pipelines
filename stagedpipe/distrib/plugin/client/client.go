// Package client is a client for running plugins. It allows for starting and stopping plugins +
// sending and receiving messages to/from the plugin.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/johnsiilver/golib/ipc/uds"
	"github.com/johnsiilver/golib/ipc/uds/highlevel/chunk"
	stream "github.com/johnsiilver/golib/ipc/uds/highlevel/proto/stream"

	messages "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/version"
)

var bufPool = chunk.NewPool(100)

// connect represents a connection to a plugin.
type connect struct {
	client   *uds.Client
	streamer *stream.Client

	once sync.Once
}

// newConnect creates a new connection to the plugin.
func newConnect(socketPath string, cred uds.Cred, connMsg *messages.Connect, configMsg *messages.Message) (conn *connect, err error) {
	switch connMsg.Type {
	case messages.ConnType_CNTRequestGroup:
	default:
		return nil, fmt.Errorf("unknown connection type: %v", connMsg.Type)
	}

	client, err := uds.NewClient(socketPath, cred.UID.Int(), cred.GID.Int(), []os.FileMode{0o770})
	if err != nil {
		return nil, fmt.Errorf("had problem connecting to the plugin socket: %w", err)
	}
	defer func() {
		if err != nil {
			client.Close()
		}
	}()

	streamer, err := stream.New(client, stream.SharedPool(bufPool), stream.MaxSize(100*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("had problem creating streamer: %w", err)
	}

	ctx := context.Background()
	if err := streamer.Write(ctx, connMsg); err != nil {
		return nil, fmt.Errorf("failed to write connection message: %w", err)
	}

	connMsg = &messages.Connect{}
	if err := streamer.Read(ctx, connMsg); err != nil {
		return nil, fmt.Errorf("failed to read connection message: %w", err)
	}

	if !version.CanUse(connMsg.Version) {
		return nil, fmt.Errorf("plugin version %v is incompatible with controller version %v", connMsg.Version, version.Semantic)
	}

	if err := streamer.Write(ctx, configMsg); err != nil {
		return nil, fmt.Errorf("failed to write config message: %w", err)
	}

	return &connect{
		client:   client,
		streamer: streamer,
	}, nil
}

// Close closes the connection to the plugin.
func (c *connect) Close() error {
	var err error
	c.once.Do(func() {
		err = c.client.Close()
	})
	return err
}

// Read reads a message from the plugin.
func (c *connect) Read(ctx context.Context, msg *messages.Message) error {
	return c.streamer.Read(ctx, msg)
}

// Write writes a message to the plugin.
func (c *connect) Write(ctx context.Context, msg *messages.Message) error {
	return c.streamer.Write(ctx, msg)
}

// Plugin represents a plugin client.
type Plugin struct {
	cmd        *exec.Cmd
	socketPath string
	exit       []chan error
}

// New starts a plugin located at pluginPath.
func New(pluginPath string) (*Plugin, error) {
	uid := os.Getuid()
	gid := os.Getgid()

	pluginName := filepath.Base(pluginPath)
	socketName := fmt.Sprintf("%s%d.socket", pluginName, time.Now().UnixNano())

	socketPath := filepath.Join(os.TempDir(), socketName)

	cmd := exec.Command(pluginPath, "--connFile", socketPath, "--uid", strconv.Itoa(uid), "--gid", strconv.Itoa(gid))

	cmd.Stdout = log.Default().Writer()
	cmd.Stderr = log.Default().Writer()

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start plugin: %w", err)
	}

	p := &Plugin{
		cmd: cmd,
		exit: []chan error{
			make(chan error, 1),
			make(chan error, 1),
		},
		socketPath: socketPath,
	}

	if err := p.waitForSocket(); err != nil {
		if kErr := cmd.Process.Kill(); kErr != nil {
			log.Println("failed to kill plugin process: ", kErr)
		}
		return nil, fmt.Errorf("plugin did not create a socket within timeout: %w", err)
	}

	go p.monitorSubProcess()

	return p, nil
}

// waitForSocket waits for the socket to be created by the plugin. It will timeout
// after 10 seconds.
func (p *Plugin) waitForSocket() error {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	after := time.After(time.Second * 10)
	for {
		select {
		case <-ticker.C:
			if _, err := os.Stat(p.socketPath); err == nil {
				return nil
			}
		case <-after:
			return fmt.Errorf("timed out waiting for socket")
		}
	}
}

// monitorSubProcess monitors the plugin process. It will send the an error if the process
// exits. It will send nil if the process exits normally.
func (p *Plugin) monitorSubProcess() {
	defer func() {
		for _, ch := range p.exit {
			close(ch)
		}
	}()
	v := p.cmd.Wait()
	for _, ch := range p.exit {
		ch <- v
	}
}

// Wait blocks until the plugin process exits. If the process exits normally, it will return nil.
// THIS CAN ONLY BE CALLED ONCE.
func (p *Plugin) Wait() error {
	return <-p.exit[1]
}

// Quit will send a SIGQUIT to the plugin process. It will wait for the process to exit.
// If the context is cancelled, it will return ctx.Err().
func (p *Plugin) Quit(ctx context.Context) error {
	p.cmd.Process.Signal(syscall.SIGQUIT)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.exit[0]:
		return nil
	}
}

// Kill will kill the plugin process. Generally you should call Quit() unless it is not responding.
func (p *Plugin) Kill(ctx context.Context) error {
	if err := p.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("failed to kill plugin process: %w", err)
	}
	return nil
}

// Request represents a request to the plugin.
type Request struct {
	// Err is the error returned by the plugin. It should not be set by the user.
	// If this is set, Data will be nil and the RequestGroup will be closed, as
	// the plugin has disconnected.
	Err error
	// Data is the data sent to the plugin.
	Data []byte
	// Seq is the sequence number of the request. It is set by the caller.
	Seq uint32
}

// RequestGroup represents a request group that is talking to the plugin.
type RequestGroup struct {
	connect *connect
	out     chan Request
	recvFin chan bool
}

func newRequestGroup(connect *connect) *RequestGroup {
	r := &RequestGroup{
		connect: connect,
		out:     make(chan Request, 1),
		recvFin: make(chan bool, 1),
	}

	go r.handleRecv()

	return r
}

func (r *RequestGroup) handleRecv() (err error) {
	defer close(r.out)
	defer func() {
		defer close(r.recvFin)
		if err != nil {
			r.connect.Close()
			r.recvFin <- false
			return
		}
		r.recvFin <- true
	}()

	for {
		msg := &messages.Message{}
		if err = r.connect.Read(context.Background(), msg); err != nil {
			if errors.Is(err, net.ErrClosed) {
				r.out <- Request{Err: fmt.Errorf("connection unexpectantly closed: %w", err)}
				return err
			}
			if errors.Is(err, io.EOF); err != nil {
				r.out <- Request{Err: fmt.Errorf("connection unexpectantly closed: %w", err)}
				return err
			}
			r.out <- Request{Err: fmt.Errorf("failed to read message: %w", err)}
			return err
		}

		switch msg.Type {
		case messages.MessageType_MTControl:
			if msg.Control.Type == messages.ControlType_CTFin {
				return nil
			}
			r.out <- Request{Err: fmt.Errorf("got unexpected control message, got %v", msg.Control.Type)}
			return
		case messages.MessageType_MTData:
			r.out <- Request{Data: msg.Req.Data}
		default:
			req := Request{Err: fmt.Errorf("got unexpected message type, got %v", msg.Type)}
			r.out <- req
			return req.Err
		}
	}
}

// Close closes the RequestGroup. An error on close indicates there was a problem.
func (r *RequestGroup) Close() error {
	defer r.connect.Close()

	msg := &messages.Message{
		Type: messages.MessageType_MTControl,
		Control: &messages.Control{
			Type: messages.ControlType_CTFin,
		},
	}
	if err := r.connect.Write(context.Background(), msg); err != nil {
		return fmt.Errorf("failed to send fin message: %w", err)
	}
	log.Println("waiting for fin")
	fin := <-r.recvFin
	log.Println("received fin")
	if !fin {
		return fmt.Errorf("failed to close request group, did not receive fin message")
	}
	return nil
}

// Submit submits a request to the plugin. jsonData is the encoded Data object for the pipeline.
func (r *RequestGroup) Submit(req Request) error {
	msg := &messages.Message{
		Type: messages.MessageType_MTData,
		Req: &messages.Request{
			Data: req.Data,
			Seq:  req.Seq,
		},
	}
	return r.connect.Write(context.Background(), msg)
}

// Output returns the output channel for the RequestGroup.
func (r *RequestGroup) Output() <-chan Request {
	return r.out
}

// RequestGroup creates a new RequestGroup to talk to the plugin. config is the configuration
// for the plugin in bytes (interpreted by the plugin).
func (p *Plugin) RequestGroup(ctx context.Context, config []byte) (*RequestGroup, error) {
	connMsg := &messages.Connect{
		Type:    messages.ConnType_CNTRequestGroup,
		Version: version.Semantic,
	}
	configMsg := &messages.Message{
		Type: messages.MessageType_MTControl,
		Control: &messages.Control{
			Type:   messages.ControlType_CTConfig,
			Config: config,
		},
	}

	cred := uds.Cred{
		PID: uds.ID(os.Getpid()),
		UID: uds.ID(os.Getuid()),
		GID: uds.ID(os.Getgid()),
	}

	connect, err := newConnect(p.socketPath, cred, connMsg, configMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to plugin: %w", err)
	}

	return newRequestGroup(connect), nil
}
