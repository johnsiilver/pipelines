package service

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"sync"
	"time"

	"github.com/johnsiilver/broadcaster"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/internal/p2c"
	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/overseer/proto"
)

// handleRegCoordinator handles the registration of a coordinator node with the overseer. It then
// passes the connection off to handleCoordinatorUpdates to handle updates from the coordinator.
func (s *Server) handleRegCoordinator(stream pb.Overseer_SubscribeServer, init *pb.RegisterInit) error {
	workers := s.workers.Backends()
	workerMsg := &pb.Workers{AddWorkers: make([]*pb.Worker, 0, len(workers))}
	for _, w := range workers {
		workerMsg.AddWorkers = append(workerMsg.AddWorkers, &pb.Worker{Address: w.Address().String()})
	}

	sub := &pb.SubscribeOut{
		Message: &pb.SubscribeOut_Register{
			Register: &pb.RegisterResp{
				Message: &pb.RegisterResp_Workers{
					Workers: workerMsg,
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(stream.Context(), 5*time.Second)
	defer cancel()

	// Send the register workers message. If the context is cancelled before we can send it,
	// we return an error. Individual stream messages don't have timeouts, so we have to
	// do this.
	if err := subscribeSend(ctx, stream, sub); err != nil {
		return fmt.Errorf("failed to send register workers message: %w", err)
	}

	fin, err := subscribeRecv(ctx, stream)
	if err != nil {
		return fmt.Errorf("failed to receive fin message: %w", err)
	}
	if fin.GetRegister().GetFin() == nil {
		return fmt.Errorf("expected register fin message, got %v", fin)
	}

	addr := netip.MustParseAddrPort(init.Address)

	b, err := p2c.NewBackend(addr)
	if err != nil {
		return fmt.Errorf("failed to create backend for coordinator(%s): %w", addr, err)
	}

	return s.handleCoordinatorUpdates(stream, b)
}

// handleCoordinatorUpdates handles comms with the coordinator after it has been registered.
// TODO(jdoak): refactor this into smaller pieces, maybe its own object.
func (s *Server) handleCoordinatorUpdates(stream pb.Overseer_SubscribeServer, backend *p2c.Backend) error {
	// We need to keep track of acks we send to the coordinator, so we can notify the waiting
	// go routine when the ack is received.
	acksMu := sync.Mutex{}
	acks := map[uint64]chan struct{}{}

	// Handle messages from the coordinator.
	var recvErr = make(chan error, 1)
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				recvErr <- fmt.Errorf("failed to receive coordinator message: %w", err)
				return
			}

			ack := in.GetAck()
			if ack == nil {
				recvErr <- fmt.Errorf("coordinator message did not contain an ack, was: %+v", in)
				return
			}

			acksMu.Lock()
			defer acksMu.Unlock()
			ch, ok := acks[ack.Id]
			if !ok {
				recvErr <- fmt.Errorf("coordinator message contained an ack that was not expected, was: %+v", in)
				return
			}
			delete(acks, ack.Id)
			close(ch)
		}
	}()

	recv, err := s.updateCoordinator.Receiver(coordUpdateWorkers, 5)
	if err != nil {
		return fmt.Errorf("failed to create receiver for coordinator updates: %w", err)
	}
	defer recv.Close()

	bCh := make(chan broadcaster.Message[*pb.Workers], 1)
	noAck := make(chan struct{}, 1)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		// If we get an error from the receiver, we return it.
		case err := <-recvErr:
			return err
		case <-noAck:
			return fmt.Errorf("coordinator did not ack our update message in the time allotted")
		// If we get a message to update the list of workers for coordinators, we send it to the
		// coordinator.
		case msg := <-func() chan broadcaster.Message[*pb.Workers] {
			go func() {
				msg := recv.Next(stream.Context())
				bCh <- msg
			}()
			return bCh
		}():
			if msg.IsZero() {
				continue
			}
			id := s.msgID.Add(1)
			out := &pb.SubscribeOut{
				Message: &pb.SubscribeOut_UpdateCoordinator{
					UpdateCoordinator: &pb.UpdateCoordinator{
						Id: id,
						Message: &pb.UpdateCoordinator_Workers{
							Workers: msg.Data(),
						},
					},
				},
			}

			ch := make(chan struct{})
			acksMu.Lock()
			acks[id] = ch
			defer acksMu.Unlock()

			go func() {
				after := time.NewTimer(5 * time.Second)
				defer after.Stop()
				select {
				case <-stream.Context().Done():
					return
				case <-ch:
					return
				case <-after.C:
					noAck <- struct{}{}
				}
			}()

			if err := stream.Send(out); err != nil {
				return fmt.Errorf("failed to send coordinator update: %w", err)
			}
		}
	}
}
