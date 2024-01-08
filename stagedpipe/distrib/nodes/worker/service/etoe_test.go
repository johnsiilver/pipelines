package service

import (
	"context"
	"io"
	"log"
	"net"
	"net/netip"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/worker/client"
	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/worker/proto"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin/etoe/testplugin/sm"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestWorker(t *testing.T) {
	const addr = "127.0.0.1:64434"

	s, err := New("0.0.0.0/24", "../../../plugin/etoe/testplugin", 80)
	if err != nil {
		t.Fatalf("Failed to create server: %s", err)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(100 * 1024 * 1024),
		grpc.MaxSendMsgSize(100 * 1024 * 1024),
	}

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterWorkerServer(grpcServer, s)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		log.Printf("Listening on %s", addr)
		grpcServer.Serve(lis)
	}()

	time.Sleep(1 * time.Second)

	workerClient, err := client.New(netip.MustParseAddrPort(addr))
	if err != nil {
		t.Fatalf("failed to dial: %s", err)
	}

	rg, err := client.GetRequestGroup[sm.Data](context.Background(), workerClient, "testplugin", sm.RGConfig{})
	if err != nil {
		t.Fatalf("failed to get worker client: %s", err)
	}

	got := []int{}
	doneRecv := make(chan error, 1)
	go func() {
		defer close(doneRecv)
		for resp := range rg.Recv() {
			if resp.Err != nil {
				log.Println("got error", resp.Err)
				select {
				case doneRecv <- resp.Err:
				default:
				}
				return
			}
			log.Println("got: ", resp.Msg.Item)
			got = append(got, resp.Msg.Item)
		}
	}()

	// time.Sleep(1 * time.Second)

	for i := 0; i < 2; i++ {
		log.Println("sending", i)
		if err = rg.Send(uint32(i), sm.Data{Item: i}); err != nil {
			t.Fatalf("failed to send: %s", err)
		}
	}

	log.Println("before rg.Close()")
	if err = rg.Close(context.Background()); err != nil {
		t.Fatalf("failed to close: %s", err)
	}
	log.Println("after rg.Close()")

	if err = <-doneRecv; err != nil && err != io.EOF {
		t.Fatalf("failed to receive: %T", err)
	}
}
