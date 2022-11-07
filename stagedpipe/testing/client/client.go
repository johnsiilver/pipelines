// Package client provides a fake client to a fictional "identity" service
// to use in testing.
package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	// ErrNotFound indicates that the Record for the ID given could not be found.
	ErrNotFound = fmt.Errorf("ID not found")
)

// Record holds records on a person. Before processing, we should have
// "First", "Last", and "ID" set.
type Record struct {
	// First is the first name of the person.
	First string
	// Last is the last name of the person.
	Last string
	// ID is the ID
	ID string

	// Birth is the time the person was born.
	Birth time.Time
	// BirthTown is what town the person was born in.
	BirthTown string
	// State is the state the person was born in.
	BirthState string

	// Err is a data error on this Record.
	Err error
}

// ID is a client to our fake identity service.
type ID struct {
	// callNum is the current call number.
	callNum atomic.Int64
	// v is the number current record number.
	v atomic.Int64
	// errAt indicates if this should error at specific call. errAt 1 will error
	// on the first call. 0 will not error at all.
	errAt int
	// notFoundAt indicates that the "notFoundAt" Record returned should return a NotFoundErr
	// in that Record (not a Call error). This number might not be hit for several Call().
	notFoundAt int
	// err if set will cause this to send this error on every call.
	err error
}

const day = 24 * time.Hour

func (i *ID) Call(ctx context.Context, id string) (Record, error) {
	if i.err != nil {
		return Record{}, errors.New("error")
	}
	if i.errAt == int(i.callNum.Add(1)) {
		return Record{}, errors.New("error")
	}

	n := i.v.Add(1)
	if i.notFoundAt == int(n) {
		return Record{ID: id, Err: ErrNotFound}, nil
	}
	if id == "" {
		return Record{Err: fmt.Errorf("empty ID field")}, nil
	}

	idNum, err := strconv.Atoi(id)
	if err != nil {
		panic(err)
	}

	return Record{
		First:      strconv.Itoa(int(n)),
		Last:       strconv.Itoa(int(n)),
		ID:         id,
		Birth:      time.Time{}.Add(time.Duration(idNum) * day),
		BirthTown:  "Nashville",
		BirthState: "Tennessee",
	}, nil
}

func (i *ID) Bulk(ctx context.Context, ids []string) ([]Record, error) {
	if i.err != nil {
		return nil, errors.New("error")
	}
	if i.errAt == int(i.callNum.Add(1)) {
		return nil, errors.New("error")
	}

	recs := make([]Record, 0, len(ids))
	for _, id := range ids {
		rec, err := i.Call(ctx, id)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	return recs, nil
}
