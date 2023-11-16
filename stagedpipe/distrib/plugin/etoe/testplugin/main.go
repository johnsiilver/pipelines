package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/johnsiilver/pipelines/stagedpipe"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/version"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin/etoe/testplugin/sm"
	"google.golang.org/protobuf/encoding/protojson"
)

var queryVersion = flag.Bool("queryVersion", false, "If true, the version of the plugin is printed and the plugin exits.")

var parallelism = runtime.NumCPU()

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	if *queryVersion {
		b, err := protojson.Marshal(version.Semantic)
		if err != nil {
			log.Fatalf("cannot marshal version: %s", err)
		}
		fmt.Fprint(os.Stdout, string(b))
		return
	}

	// Create the StateMachine implementation that will be used in our pipeline.
	xsm, err := sm.NewSM()
	if err != nil {
		log.Fatalf("cannot start state machine: %s", err)
	}

	// Create our parallel and concurrent Pipeline from our StateMachine.
	pipeline, err := stagedpipe.New[sm.Data]("etoe test pipeline", parallelism, xsm)
	if err != nil {
		log.Fatalf("cannot create a pipeline: %s", err)
	}
	defer pipeline.Close()

	plug := plugin.NewPlugin[sm.Data](sm.NewConfig, pipeline)

	if err := plug.Run(); err != nil {
		log.Fatalf("plugin failed: %s", err)
	}
}
