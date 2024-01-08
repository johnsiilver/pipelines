package plugin

import (
	"context"
	"fmt"

	"github.com/go-json-experiment/json"

	messages "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"
)

// state is a state in the state machine.
type state func(ctx context.Context) (state, *messages.Message)

// recvMsgs is the state machine for processing messages incoming to a plugin
// over a unix domain socket connection.
type recvMsgs[D any] struct {
	config RGConfig[D]
	// Msgs is the channel that messages are sent to the plugin and are processed.
	Msgs      chan *messages.Message
	newConfig NewRGConfig[D]
	input     chan *messages.Message
	recvFin   bool
	cancelled bool
}

// newRecvMsgs creates a new state machine for processing messages incoming to a plugin.
// config is an object that implements RGConfig, which will be cloned for each
// request group connection and unmarshalled into, it should be the zero value. connect is the connection to the controller.
func newRecvMsgs[D any](newConfig NewRGConfig[D], input chan *messages.Message) *recvMsgs[D] {
	return &recvMsgs[D]{
		input:     input,
		newConfig: newConfig,
		Msgs:      make(chan *messages.Message, 1),
	}
}

// Run runs the state machine.
func (s *recvMsgs[D]) Run(ctx context.Context) (err *messages.Message) {
	defer close(s.Msgs)

	state := s.Start
	for state != nil {
		if ctx.Err() != nil {
			return errorf(messages.ErrorCode_ECCancelled, true, "context cancelled before receiving config message")
		}
		state, err = state(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

const (
	fsUnknown   = messages.ControlType_CTUnknown
	fsErrored   = messages.ControlType_CTError
	fsCancelled = messages.ControlType_CTCancel
	fsCompleted = messages.ControlType_CTFin
)

// FinalState returns the final state of the state machine.
func (s *recvMsgs[D]) FinalState() messages.ControlType {
	switch {
	case s.cancelled:
		return fsCancelled
	case s.recvFin:
		return fsCompleted
	}
	return fsErrored
}

// Config returns the config object. This is only available after the Start state.
func (s *recvMsgs[D]) Config() RGConfig[D] {
	return s.config
}

// Start is the first state of the state machine.  It waits for a config message
// from the client with a timeout.  From here we go to the Flow state.
func (s *recvMsgs[D]) Start(ctx context.Context) (st state, err *messages.Message) {
	var msg *messages.Message
	var ok bool
	select {
	case <-ctx.Done():
		e := errorf(messages.ErrorCode_ECCancelled, true, "context cancelled before receiving config message")
		return nil, e
	case msg, ok = <-s.input:
		if !ok {
			e := errorf(messages.ErrorCode_ECInternal, true, "Start did not receive a message from the controller")
			return nil, e
		}
	}

	switch msg.Type {
	case messages.MessageType_MTControl:
		if msg.Control.Type != messages.ControlType_CTConfig {
			e := errorf(messages.ErrorCode_ECInternal, true, fmt.Sprintf("expected config control message on Start, got %v", msg.Control.Type))
			return nil, e
		}

		rgc := s.newConfig()

		if err := json.Unmarshal(msg.Control.Config, rgc); err != nil {
			e := errorf(messages.ErrorCode_ECInternal, true, fmt.Sprintf("failed to unmarshal config object: %s", err))
			return nil, e
		}
		if err := rgc.Init(ctx); err != nil {
			e := errorf(messages.ErrorCode_ECInternal, true, fmt.Sprintf("failed to initialize config object: %s", err))
			return nil, e
		}
		s.config = rgc
		return s.Flow, nil
	}
	e := errorf(messages.ErrorCode_ECInternal, true, fmt.Sprintf("expected config control message on Start, got %v", msg.Type))
	return nil, e
}

// Flow is the state that processes incoming messages from the client after our
// opening config message.  It will either send the message to the data or
// control channel, or if the context is done, it will return the End state.
func (s *recvMsgs[D]) Flow(ctx context.Context) (st state, err *messages.Message) {
	var msg *messages.Message
	var ok bool
	select {
	case <-ctx.Done():
		e := errorf(messages.ErrorCode_ECCancelled, true, ctx.Err().Error())
		return nil, e
	case msg, ok = <-s.input:
	}

	if !ok {
		e := errorf(messages.ErrorCode_ECInternal, true, fmt.Sprintf("bug: combination of states not handled(ok: %v, recvFin: %v, cancelled: %v)", ok, s.recvFin, s.cancelled))
		return nil, e
	}

	switch msg.Type {
	case messages.MessageType_MTControl:
		switch msg.Control.Type {
		case messages.ControlType_CTCancel:
			s.cancelled = true
			e := errorf(messages.ErrorCode_ECCancelled, true, "user cancelled the message")
			return nil, e
		case messages.ControlType_CTFin:
			s.recvFin = true
			return nil, nil
		}
		e := errorf(messages.ErrorCode_ECInternal, true, fmt.Sprintf("received a control message(%v) we don't support", msg.Control.Type))
		return nil, e
	case messages.MessageType_MTData:
		s.Msgs <- msg
	default:
		e := errorf(messages.ErrorCode_ECInternal, true, fmt.Sprintf("received a message(%v) we don't support", msg.Type))
		return nil, e
	}
	return s.Flow, nil
}
