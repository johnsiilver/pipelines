package sm

import (
	"context"

	"github.com/johnsiilver/pipelines/stagedpipe"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin"
)

// RGConfig is the configuration for the RequestGroup used in a distributed pipeline.
type RGConfig struct{}

func (r *RGConfig) Init(ctx context.Context) error {
	return nil
}

func (r *RGConfig) Setup(ctx context.Context, data Data) (Data, error) {
	return data, nil
}

func (r *RGConfig) Close() error {
	return nil
}

// NewConfig implements plugin.NewRGConfig.
func NewConfig() plugin.RGConfig[Data] {
	return &RGConfig{}
}

// Data is the data that is passed through the pipeline for a request.
type Data struct {
	// Item represents our item submission order.
	Item int
	// Panic will cause the pipeline to panic. This is used for testing our plugin.
	Panic bool
}

// NewRequest returns a new stagedpipe.Request object for use in your pipeline.
func NewRequest(ctx context.Context, data Data) stagedpipe.Request[Data] {
	return stagedpipe.Request[Data]{
		Ctx:  ctx,
		Data: data,
	}
}
