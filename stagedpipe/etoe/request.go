package main

import (
	"crypto/rand"
	"fmt"
	"log"

	"github.com/johnsiilver/pipelines/stagedpipe"
)

// Request represents our request object. You can find a blank one
// to fill out in file "requestTemplate" located in this directory.
type Request struct {
	// data is the data for this Request that will be processed.
	data     [][]byte
	groupNum uint64

	procID string

	// err holds an error if processing the Request had a problem.
	err error

	nextStage stagedpipe.Stage
}

// NewRequest creates a new Request.
func NewRequest() stagedpipe.Request {
	data := make([][]byte, 0, 1000)
	for i := 0; i < 1000; i++ {
		b := make([]byte, 1024)
		_, err := rand.Read(b)
		if err != nil {
			log.Fatalf("error while generating random bytes: %s", err)
		}
		data = append(data, b)
	}

	return Request{
		data: data,
	}
}

// Pre implements stagedpipe.Request.Pre().
func (r Request) Pre() {}

// Post implements stagedpipe.Request.Post().
func (r Request) Post() {}

// Error implements stagedpipe.Request.Error().
func (r Request) Error() error {
	return r.err
}

// SetError implements stagedpipe.Request.SetError().
func (r Request) SetError(e error) stagedpipe.Request {
	r.err = e
	return r
}

// Next implements stagedpipe.Request.Next().
func (r Request) Next() stagedpipe.Stage {
	return r.nextStage
}

// Setnext implements stagedpipe.Request.SetNext().
func (r Request) SetNext(stage stagedpipe.Stage) stagedpipe.Request {
	r.nextStage = stage
	return r
}

func (r Request) GroupNum() uint64 {
	return r.groupNum
}

// Setnext implements stagedpipe.Request.SetNext().
func (r Request) SetGroupNum(n uint64) stagedpipe.Request {
	r.groupNum = n
	return r
}

// ToConcrete converts a statepipe.Request to our concrete Request object.
func ToConcrete(r stagedpipe.Request) (Request, error) {
	x, ok := any(r).(Request)
	if !ok {
		return Request{}, fmt.Errorf("unexpected type %T, expecting Request", r)
	}
	return x, nil
}

// MustConcrete is the same as ToConcrete expect an error causes a panic.
func MustConcrete(r stagedpipe.Request) Request {
	x, err := ToConcrete(r)
	if err != nil {
		panic(err)
	}
	return x
}
