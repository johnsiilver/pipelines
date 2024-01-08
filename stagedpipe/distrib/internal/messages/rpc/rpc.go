// Package rpc implements and RPC client/server on top of the messaging package.
// This simply lets you register different RPC handlers for channels that are differetiated
// by a channel number.  The channel number is sent in the message header.
package rpc

import (
	"fmt"
	"log"
	"net"
	"net/netip"
	"runtime"
	"sync"

	"github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messaging"
)

// StreamMsg is a message sent on a Stream that will be received on an out channel
// and may contain an error.
type StreamMsg[T any] struct {
	// Data is the data sent on the stream.
	Data T
	// Err is the error that occurred when sending the data.
	Err error
}

// StreamPromise is a message sent on a Stream that will be received on an in channel.
// When the message has been received by the remote end, the ErrCh will be sent a nil
// or if there was a problem, an error.
type StreamPromise[T any] struct {
	// Data is the data sent on the stream.
	Data T
	// ErrCh is the channel that will receive the error when the message has been sent
	// or failed. The error will be nil in the case of success.
	ErrCh chan error
}

// Stream implements Stream.
type Stream struct {
	in  chan StreamMsg[messaging.Message]
	out chan StreamPromise[messaging.Message]
}

// NewStream is the constructor for stream.
func NewStream() Stream {
	s := Stream{
		in:  make(chan StreamMsg[messaging.Message], 1),
		out: make(chan StreamPromise[messaging.Message], 1),
	}
	return s
}

// In is the channel that receives messages from the remote end.
func (s Stream) In() chan StreamMsg[messaging.Message] {
	return s.in
}

// Out is the channel that sends messages to the remote end.
func (s Stream) Out() chan StreamPromise[messaging.Message] {
	return s.out
}

// StreamHandler is a function that handles a stream.
type StreamHandler func(stream Stream) error

// streamItems holds the handler and stream for the handler.
type streamItems struct {
	handler StreamHandler
	stream  Stream
	err     error
	mu      *sync.Mutex
}

// work represents a function that will be run in a goroutine.
type work func() error

// streamDemuxer is a demuxer for streams. It takes a messaging.AggClient and demuxes
// the messages to the correct stream handler.
type streamDemuxer struct {
	client *messaging.AggClient
	// streamItems holds streamItems that we received at least one message for.
	streamItems map[uint8]streamItems
	// streamHandlers holds the handlers for each stream.
	streamHandlers map[uint8]StreamHandler
	workIn         chan work

	streamItemsMu *sync.Mutex
}

// newStreamDemuxer is the constructor for streamDemuxer.
func newStreamDemuxer(client *messaging.AggClient, streamHandlers map[uint8]StreamHandler) streamDemuxer {
	sItems := map[uint8]streamItems{}

	d := streamDemuxer{
		client:         client,
		streamItems:    sItems,
		streamHandlers: streamHandlers,
		workIn:         make(chan work, 1),
		streamItemsMu:  &sync.Mutex{},
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for w := range d.workIn {
				if err := w(); err != nil {
					d.client.Close()
				}
			}
		}()
	}

	go d.recv()
	return d
}

// recv receives messages from the remote end and sends them to the correct stream handler.
func (s streamDemuxer) recv() {
	defer s.client.Close()

	for {
		msg, err := s.client.Recv()
		if err != nil {
			if err == net.ErrClosed {
				return
			}
			s.streamItemsMu.Lock()
			for _, item := range s.streamItems {
				item.mu.Lock()
				item.err = err
				item.mu.Unlock()
			}
			s.streamItemsMu.Unlock()

			log.Println("recv error: ", err)
			return
		}
		// Make sure we have a handler for this channel num.
		h, ok := s.streamHandlers[msg.ChannelNum]
		if !ok {
			panic(fmt.Sprintf("channel %d not registered", msg.ChannelNum))
		}

		// If we have not seen this channel num before, create it and spin off
		// a handler to handle it.
		stream, ok := s.streamItems[msg.ChannelNum]
		if !ok {
			stream = streamItems{
				handler: h,
				stream:  NewStream(),
				mu:      &sync.Mutex{},
			}
			// Start the handler for the stream. This runs until the stream is closed.
			go stream.handler(stream.stream)

			// Store the stream so we don't recreate this again.
			s.streamItems[msg.ChannelNum] = stream

			// Read message coming in from the stream.Out() channel and send them to the
			// remote receiver.
			go func() {
				for in := range stream.stream.Out() {
					in.Data.ChannelNum = msg.ChannelNum
					in.ErrCh <- s.client.Send(in.Data)
					select {
					case <-in.ErrCh:
					default:
						panic("promise's error channel is full, bug")
					}
				}
			}()
		}
		// Takes the incoming message and sends it to the correct stream handler.
		f := func() error {
			in := StreamMsg[messaging.Message]{Data: msg}
			stream.stream.In() <- in
			return nil
		}
		// Push the work onto the work queue.
		s.workIn <- f
	}
}

// Server is a server that handles RPC streaming calls.
// Simply call Server.RegisterStreamHandler() for each RPC you want to register a handler for.
// Then call Server.Start() to start the server.
type Server struct {
	streams map[uint8]StreamHandler
	server  *messaging.Server

	started bool
	mu      sync.Mutex
}

// NewServer is the constructor for Server.
func NewServer() *Server {
	return &Server{
		streams: map[uint8]StreamHandler{},
	}
}

// RegisterStreamHandler registers a handler for a channel with chanID. This must be called before
// calling Start(). If chanID is 0, this will panic.
func (s *Server) RegisterStreamHandler(chanID uint8, handler StreamHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if chanID == 0 {
		panic("channel 0 is reserved")
	}

	if s.started {
		panic("can't call RegisterStreamHandler: server already started")
	}
	if s.streams[chanID] != nil {
		panic("channel already registered")
	}

	s.streams[chanID] = handler
}

// Start starts the server listening on addr. This is a blocking call until the server dies.
func (s *Server) Start(addr netip.AddrPort) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}

	var err error
	s.server, err = messaging.NewServer(addr)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.started = true
	s.mu.Unlock()

	for {
		client, err := s.server.Accept()
		if err != nil {
			return err
		}
		newStreamDemuxer(client, s.streams)
	}
}

// Client is a client that handles RPC streaming calls.
type Client struct {
	client *messaging.AggClient
	used   map[uint8]Stream
}

// NewClient is the constructor for Client.
func NewClient(addr netip.AddrPort, numClients int) (*Client, error) {
	c, err := messaging.NewAggClient(addr, numClients)
	if err != nil {
		return nil, err
	}
	client := &Client{client: c}
	go client.recv()
	return client, nil
}

// Call calls the RPC with chanID and returns a Stream that can be used to send and receive
// messages to the remote end.
func (c *Client) Call(chanID uint8) (Stream, error) {
	if chanID == 0 {
		return Stream{}, fmt.Errorf("channel 0 is reserved")
	}

	if _, ok := c.used[chanID]; ok {
		return Stream{}, fmt.Errorf("channel %d already in use", chanID)
	}
	stream := NewStream()

	c.used[chanID] = stream

	// Handle messages being sent to the remote end.
	go func() {
		for out := range stream.Out() {
			out.Data.ChannelNum = chanID

			select {
			case out.ErrCh <- c.client.Send(out.Data):
			default:
				panic("promise's error channel is full, bug")
			}
		}
	}()

	return stream, nil
}

// recv receives messages from the remote end and sends them to the correct Stream.
func (c *Client) recv() {
	for {
		msg, err := c.client.Recv()
		if err != nil {
			if err == net.ErrClosed {
				return
			}
			log.Println("recv error: ", err)
			return
		}
		// Make sure we have a stream for this channel num.
		stream, ok := c.used[msg.ChannelNum]
		if !ok {
			panic(fmt.Sprintf("rpc client: channel %d not registered", msg.ChannelNum))
		}
		stream.in <- StreamMsg[messaging.Message]{Data: msg}
	}
}
