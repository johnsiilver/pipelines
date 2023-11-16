package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/johnsiilver/pipelines/stagedpipe"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin/examples/userrec/sm"
)

var (
	connStr = flag.String("connStr", "", "The connection string to your postgres database")
)

var parallelism = runtime.NumCPU()

// RGConfig is a type implementing plugin.RGConfig.
type RGConfig struct {
	internals *sm.Internals

	// ConnStr is the connection string to the postgres database.
	ConnStr string
}

// Init implements plugin.RGConfig.Init().
func (c *RGConfig) Init(ctx context.Context) error {
	// Setup DB connection.
	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dialCancel()

	pool, err := pgxpool.Connect(dialCtx, c.ConnStr)
	if err != nil {
		return fmt.Errorf("cannot connect to Postgres: %s", err)
	}

	c.internals = &sm.Internals{Pool: pool}
	return nil
}

// Setup implements plugin.RGConfig.Setup().
// This might be adding a database connection, transaction, etc...
func (c *RGConfig) Setup(ctx context.Context, data sm.Data) (sm.Data, error) {
	data.Internals = c.internals
	return data, nil
}

// Close implements plugin.RGConfig.Close().
func (c *RGConfig) Close() error {
	c.internals.Pool.Close()
	return nil
}

// NewConfig implements plugin.NewRGConfig.
func NewConfig() plugin.RGConfig[sm.Data] {
	return &RGConfig{}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	// Create the StateMachine implementation that will be used in our pipeline.
	xsm, err := sm.NewSM()
	if err != nil {
		log.Fatalf("cannot start state machine: %s", err)
	}

	// Create our parallel and concurrent Pipeline from our StateMachine.
	pipeline, err := stagedpipe.New[sm.Data]("pipeline name", parallelism, xsm)
	if err != nil {
		log.Fatalf("cannot create a pipeline: %s", err)
	}
	defer pipeline.Close()

	newConfig := func() plugin.RGConfig[sm.Data] {
		return &RGConfig{ConnStr: *connStr}
	}

	plug := plugin.NewPlugin[sm.Data](newConfig, pipeline)

	if err := plug.Run(); err != nil {
		log.Fatalf("plugin.Run() returned an error: %s", err)
	}
}
