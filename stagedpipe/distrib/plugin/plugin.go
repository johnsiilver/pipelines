// Package plugin defines the plugin interface for a stagedpipe pipeline plugin.
package plugin

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sync"
	"syscall"

	"github.com/go-json-experiment/json"

	"github.com/johnsiilver/pipelines/stagedpipe"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/conns"
	messages "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"
)

// fileConn is the path to the unix domain socket file to use for the pipeline.
// EXCEPTION: It is almost always bad to have a flag out of package main. These
// are an exception case.
var (
	fileConn = flag.String("connFile", "", "The path to the unix domain socket file to use for the pipeline.")
	uid      = flag.Int("uid", -1, "The uid to use for the unix domain socket file.")
	gid      = flag.Int("gid", -1, "The gid to use for the unix domain socket file.")
)

// RGConfig is a generic type representing a request group config for a plugin sent from the controller.
// D is the pipeline data type. The Config must be able to do setup for the data
// object if required. This can be a no-op if not needed.
type RGConfig[D any] interface {
	// Init is called to initialize the RGConfig object after it has been read
	// in from the controller.
	Init(ctx context.Context) error
	// Setup is called to prepate the data object before put in the pipeline.
	// This might be adding a database connection, transaction, etc...
	Setup(ctx context.Context, data D) (D, error)

	// Close is called when the RequestGroup is closed.
	Close() error
}

// NewRGConfig creates a new RGConfig object. This must return a type that is a pointer.
type NewRGConfig[D any] func() RGConfig[D]

// Plugin represents a pipeline plugin.
type Plugin[D any] struct {
	pipelines *stagedpipe.Pipelines[D]
	newConfig NewRGConfig[D]

	closeOnce sync.Once
	sigClose  chan struct{}
	wg        sync.WaitGroup

	dataIsPtr bool
}

// NewPlugin creates a new plugin. D is the pipeline data type.
func NewPlugin[D any](newConfig NewRGConfig[D], pipelines *stagedpipe.Pipelines[D]) *Plugin[D] {
	p := &Plugin[D]{
		pipelines: pipelines,
		newConfig: newConfig,
		sigClose:  make(chan struct{}),
	}

	var d D
	switch {
	case reflect.ValueOf(d).Kind() == reflect.Ptr:
		p.dataIsPtr = true
	}
	return p
}

// Run will run the plugin until it is told to stop either by a message from the parent process
// or an os signal. If there was an error, it returns the error message.
func (p *Plugin[D]) Run() error {
	io, err := conns.New(*fileConn, *uid, *gid)
	if err != nil {
		return err
	}

	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)

	for {
		var connect conns.Connect
		var ok bool
		exitCh := make(chan error)

		select {
		case e := <-exitCh:
			return e
		case connect, ok = <-io.Connect():
			if !ok {
				return fmt.Errorf("connections have been terminated, Plugin is exiting")
			}
			if p.sigClose == nil {
				log.Println("received a new connection after being told to close, bug...")
				close(connect.Output)
				continue
			}

			connect := connect
			p.wg.Add(1)

			// Spin off a goroutine to handle the connection.
			go func() {
				defer p.wg.Done()
				route, err := p.routing(connect)
				if err != nil {
					log.Println(err)
					close(connect.Output)
				}
				route()
			}()
		case <-p.sigClose:
			p.sigClose = nil
			log.Println("received close plugin message, waiting for connections to close")
			p.wg.Wait()
			log.Println("all connections have closed, Plugin is exiting")
			return nil
		case sig := <-ch:
			log.Printf("received signal %s, closing with sigClose", sig)
			p.closeOnce.Do(func() {
				close(p.sigClose)
			})
		}
	}
}

// route represents a function that handles a conection.
type route func()

// routing returns a route function for a connection based on the first message that comes in.
// If this is invalid, an error is returned. There is a 1 second timeout on the first message.
func (p *Plugin[D]) routing(connect conns.Connect) (route, error) {
	switch connect.ConnType {
	case messages.ConnType_CNTRequestGroup:
		return func() {
			p.msgRequestGroupHandler(connect)
		}, nil
	}

	return nil, fmt.Errorf("unexpected message type: %s", connect.ConnType)
}

// msgControlHandler handles when the initial message is a control message, which should be the start
// of a data stream from the controller go a stagedpipe.RequestGroup.
func (p *Plugin[D]) msgRequestGroupHandler(connect conns.Connect) {
	ctx := context.Background()
	machine := newRecvMsgs[D](p.newConfig, connect.Input)

	// Start the state machine to decode our message stream.
	go func() {
		if err := machine.Run(ctx); err != nil {
			log.Println(err)
		}
	}()

	defer close(connect.Output)

	rg := p.pipelines.NewRequestGroup()

	done := make(chan struct{})
	// Handle the output of the pipeline.
	go func() {
		defer close(done)

		// Configs come with a close function, so we need to call it when we are done.
		defer func() {
			c := machine.Config()
			if c != nil {
				if err := c.Close(); err != nil {
					log.Println(err)
				}
			}
		}()

		// Process all the output from the pipeline.
		for out := range rg.Out() {
			if out.Err != nil {
				connect.Output <- errorf(messages.ErrorCode_ECPipelineError, true, "pipeline returned an error: %s", out.Err)
				continue
			}
			b, err := json.Marshal(out.Data)
			if err != nil {
				connect.Output <- errorf(messages.ErrorCode_ECInternal, true, "failed to marshal JSON data object: %s", err)
				continue
			}

			connect.Output <- &messages.Message{
				Type: messages.MessageType_MTData,
				Req: &messages.Request{
					Data: b,
				},
			}
		}
	}()

	// Send msgs to the pipeline.
	for msg := range machine.Msgs {
		d, err := p.processData(ctx, msg, machine)
		if err != nil {
			connect.Output <- errorf(messages.ErrorCode_ECInternal, true, err.Error())
			continue
		}
		if err := rg.Submit(stagedpipe.Request[D]{Ctx: ctx, Data: d}); err != nil {
			connect.Output <- errorf(messages.ErrorCode_ECInternal, true, "failed to submit request to pipeline: %s", err)
			continue
		}
	}
	rg.Close()

	// Wait for the pipeline to finish.
	log.Println("before done")
	<-done
	log.Println("after done")

	// Send the fin message to the controller.
	if machine.recvFin {
		connect.Output <- &messages.Message{
			Type: messages.MessageType_MTControl,
			Control: &messages.Control{
				Type: messages.ControlType_CTFin,
			},
		}
		log.Println("sent fin")
	}
}

// processData processes the data object sent from the controller by unmarshaling it and
// calling the config's Setup() method on the data.
func (p *Plugin[D]) processData(ctx context.Context, msg *messages.Message, machine *recvMsgs[D]) (D, error) {
	var err error
	if p.dataIsPtr {
		var d D
		if err = json.Unmarshal(msg.Req.Data, d); err != nil {
			return d, fmt.Errorf("failed to unmarshal JSON data object: %s", err)
		}

		d, err = machine.Config().Setup(ctx, d)
		if err != nil {
			return d, fmt.Errorf("failed to setup data object: %s", err)
		}
		return d, nil
	}
	var d D
	if err = json.Unmarshal(msg.Req.Data, &d); err != nil {
		return d, fmt.Errorf("failed to unmarshal JSON data object: %s", err)
	}

	d, err = machine.Config().Setup(ctx, d)
	if err != nil {
		return d, fmt.Errorf("failed to setup data object: %s", err)
	}
	return d, nil
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
