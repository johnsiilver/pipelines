package sm

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/johnsiilver/pipelines/stagedpipe"
)

// Data is the data that is passed through the pipeline for a request.
// Best practice is to send bulk data and not individual items.
type Data struct {
	// Internals is internals used in the pipeline and set by the RGConfig.
	// It is not transfered between systems. Setting this from a caller will
	// have no effect.
	Internals *Internals `json:"-"`

	// UserName is the user's user name.
	UserName string
	// FirstName is the user's first name.
	FirstName string
	// LastName is the user's last name.
	LastName string
	// UserID is the unique ID for the user. 0 is a zero value, there is no 0 ID.
	UserID uint64
	// GroupID is the unique ID for the user's group. 0 is a zero value, there is no 0 ID.
	GroupID uint64
}

// Internals is used to hold data  that is simply passed through the pipeline.
type Internals struct {
	Pool *pgxpool.Pool
}

// NewRequest returns a new stagedpipe.Request object for use in your pipeline.
func NewRequest(ctx context.Context, data Data) stagedpipe.Request[Data] {
	return stagedpipe.Request[Data]{
		Ctx:  ctx,
		Data: data,
	}
}
