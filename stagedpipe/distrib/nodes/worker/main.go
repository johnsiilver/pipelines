package main

import (
	"flag"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"

	"google.golang.org/grpc"

	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/worker/proto"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/worker/service"
	"github.com/shirou/gopsutil/mem"
)

var (
	addr         = flag.String("addr", "", "The address to listen on")
	overseerAddr = flag.String("oversserAddr", "", "The address of the overseer")
	pluginPath   = flag.String("pluginPath", "", "The path to the plugin")
	memoryLimit  = flag.Int("memoryLimit", 80, "The free memory percentage at which we pause incoming streams. 0 means no limit. 99 is the max limit")
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	if *pluginPath == "" {
		*pluginPath = filepath.Join(os.TempDir(), "plugin")
	}

	if err := os.MkdirAll(*pluginPath, 0770); err != nil {
		log.Fatalf("failed to create plugin path: %s", err)
	}

	// If we have a memory limit, set the go runtime memory limit to the same number so
	// we get a more aggressive GC as we approach the limit.
	if *memoryLimit > 0 && *memoryLimit < 100 {
		mem, err := mem.VirtualMemory()
		if err != nil {
			log.Fatalf("failed to get memory stats: %s", err)
		}

		debug.SetMemoryLimit(int64(float64(mem.Total) * (float64(*memoryLimit) / 100.0)))
	}

	s, err := service.New(*overseerAddr, *pluginPath, *memoryLimit)
	if err != nil {
		log.Fatalf("failed to create service: %s", err)
	}

	var opts = []grpc.ServerOption{
		grpc.MaxRecvMsgSize(100 * 1024 * 1024),
		grpc.MaxSendMsgSize(100 * 1024 * 1024),
	}

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterWorkerServer(grpcServer, s)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Printf("Listening on %s", *addr)
	grpcServer.Serve(lis)
}
