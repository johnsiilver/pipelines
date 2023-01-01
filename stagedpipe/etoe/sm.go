package main

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/johnsiilver/pipelines/stagedpipe"
)

type SM struct {
	idService *SendIDClient
}

// NewSM creates a new stagepipe.StateMachine.
func NewSM() *SM {
	sm := &SM{
		idService: NewSendIDClient(),
	}
	return sm
}

// Close stops all running goroutines. This is only safe after all entries have
// been processed.
func (s *SM) Close() {}

// Start implements stagedpipe.StateMachine.Start().
func (s *SM) Start(ctx context.Context, req stagedpipe.Request) stagedpipe.Request {
	x, err := ToConcrete(req)
	if err != nil {
		return req.SetError(fmt.Errorf("unexpected type %T, expecting Request", req))
	}

	switch {
	case len(x.data) == 0:
		return req.SetError(fmt.Errorf("Request.Data cannot be empty"))
	}

	return req.SetNext(s.ProcID)
}

func (s *SM) ProcID(ctx context.Context, req stagedpipe.Request) stagedpipe.Request {
	x := MustConcrete(req)
	x.procID = uuid.New().String()

	return req.SetNext(s.SendID)
}

func (s *SM) SendID(ctx context.Context, req stagedpipe.Request) stagedpipe.Request {
	s.idService.Send(req)

	return req.SetNext(nil)
}

type SendIDClient struct {
}

func NewSendIDClient() *SendIDClient {
	return &SendIDClient{}
}

func (s *SendIDClient) Send(req stagedpipe.Request) {
	x := MustConcrete(req)

	for _, id := range x.data {
		reverse(id)
	}
}

func reverse(s []byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
