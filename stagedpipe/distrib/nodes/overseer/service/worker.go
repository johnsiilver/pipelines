package service

import (
	"context"
	"fmt"
	"io"
	stdfs "io/fs"
	"log"
	"net/netip"
	"path/filepath"
	"sync"
	"time"

	"github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/internal/p2c"
	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/overseer/proto"

	osFS "github.com/gopherfs/fs/io/os"
)

func (s *Server) handleRegWorker(stream pb.Overseer_SubscribeServer, init *pb.RegisterInit) error {
	fsys, err := osFS.New()
	if err != nil {
		return fmt.Errorf("failed to create os fs: %w", err)
	}

	plugCh, err := getPlugins(fsys, s.pluginDir)
	if err != nil {
		return err
	}

	for p := range plugCh {
		if p.Err != nil {
			log.Println("while reading plugins for a new worker: ", p.Err)
			continue
		}
		out := &pb.SubscribeOut{
			Message: &pb.SubscribeOut_Register{
				Register: &pb.RegisterResp{
					Message: &pb.RegisterResp_LoadPlugin{
						LoadPlugin: p.Data,
					},
				},
			},
		}
		ctx, cancel := context.WithTimeout(stream.Context(), 5*time.Second)
		err := subscribeSend(ctx, stream, out)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to send plugin to worker during registration: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(stream.Context(), 5*time.Second)
	err = subscribeSend(
		ctx,
		stream,
		&pb.SubscribeOut{
			Message: &pb.SubscribeOut_Register{
				Register: &pb.RegisterResp{
					Message: &pb.RegisterResp_Fin{
						Fin: &pb.RegisterFin{},
					},
				},
			},
		},
	)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to send fin message to worker during registration: %w", err)
	}

	ctx, cancel = context.WithTimeout(stream.Context(), 5*time.Minute)
	defer cancel()
	in, err := subscribeRecv(ctx, stream)
	if err != nil {
		return fmt.Errorf("failed to receive fin message from worker during registration: %w", err)
	}

	if in.GetRegister().GetFin() == nil {
		return fmt.Errorf("expected fin message from worker during registration, got %v", in)
	}

	addr := netip.MustParseAddrPort(init.Address)
	b, err := p2c.NewBackend(addr)
	if err != nil {
		return fmt.Errorf("failed to create backend for coordinator(%s): %w", addr, err)
	}

	if err := s.workers.Add(stream.Context(), b); err != nil {
		return fmt.Errorf("failed to add worker(%s): %w", addr, err)
	}

	return s.handleWorkerUpdates(stream, b)
}

// handleWorkerUpdates handles updates from the worker. It will block until the worker connection
// is closed or an error occurs.
func (s *Server) handleWorkerUpdates(stream pb.Overseer_SubscribeServer, b *p2c.Backend) error {
	mu := sync.Mutex{}
	acks := map[uint64]chan struct{}{}

	// Handle receiving messages from the worker.
	recvErr := make(chan error, 1)
	go func() {
		defer close(recvErr)
		for {
			in, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				recvErr <- fmt.Errorf("failed to receive worker message: %w", err)
				return
			}
			if in.GetAck() == nil {
				recvErr <- fmt.Errorf("expected ack message, got %v", in)
				return
			}
			mu.Lock()
			promise, ok := acks[in.GetAck().Id]
			if ok {
				delete(acks, in.GetAck().Id)
			}
			mu.Unlock()
			if !ok {
				recvErr <- fmt.Errorf("got ack for unknown message id(%d)", in.GetAck().Id)
				return
			}
			close(promise)
		}
	}()

	plugUpdates, err := s.updateWorkers.Receiver(workerUpdatePlugs, 100)
	if err != nil {
		return fmt.Errorf("failed to create plugin update receiver: %w", err)
	}
	defer plugUpdates.Close()

	// Handle sending messages to the worker.
	var exitErr error
	updatesCh := make(chan *pb.UpdateWorker, 1)
	ackErr := make(chan error, 1)

	for {
		select {
		case <-stream.Context().Done():
			exitErr = stream.Context().Err()
			break
		case exitErr = <-recvErr:
			break
		case exitErr = <-ackErr:
			break
		case update := <-func() chan *pb.UpdateWorker {
			go func() {
				update := plugUpdates.Next(stream.Context())
				if update.IsZero() {
					panic("pluginUpdates.Next() returned a zero value, which should never happen.")
				}
				updatesCh <- update.Data()
			}()
			return updatesCh
		}():
			out := &pb.SubscribeOut{
				Message: &pb.SubscribeOut_UpdateWorker{
					UpdateWorker: update,
				},
			}
			go func() {
				promise := make(chan struct{}, 1)
				mu.Lock()
				acks[update.Id] = promise
				mu.Unlock()
				select {
				case <-stream.Context().Done():
					return
				case <-promise:
					return
				case <-time.After(1 * time.Minute):
					select {
					case ackErr <- fmt.Errorf("failed to get ack for update(%d) in time allotted", update.Id):
					default:
					}
					return
				}
			}()
			if err := stream.Send(out); err != nil {
				return fmt.Errorf("failed to send plugin update to worker: %w", err)
			}
			continue
		}
		break
	}
	return exitErr
}

type plugfs interface {
	stdfs.ReadDirFS
	stdfs.ReadFileFS
	stdfs.StatFS
}

// getPlugins streams the plugins that are located in the directory dir. An error is returned
// if the directory isn't reachable.  The returned channel will be closed when all plugins
// have been sent. If there is an error with a plugin, the error will be sent on the channel
// but the channel won't close.
func getPlugins(fsys plugfs, dir string) (chan streamResp[*pb.LoadPlugin], error) {
	ch := make(chan streamResp[*pb.LoadPlugin], 1)
	entries, err := fsys.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read plugin directory(%s): %w", dir, err)
	}
	go func() {
		defer close(ch)

		for _, dirEntry := range entries {
			switch {
			case dirEntry.IsDir():
				continue
			case filepath.Ext(dirEntry.Name()) != "":
				continue
			case dirEntry.Name() == "README":
				continue
			}

			fqPath := filepath.Join(dir, dirEntry.Name())
			content, err := getPlugin(fsys, fqPath)
			if err != nil {
				ch <- streamResp[*pb.LoadPlugin]{Err: err}
				continue
			}
			ch <- streamResp[*pb.LoadPlugin]{Data: content}
		}
	}()

	return ch, nil
}

// getPlugin returns the plugin at fqPath. An error is returned if the plugin is larger than 100MB.
func getPlugin(fsys plugfs, fqPath string) (*pb.LoadPlugin, error) {
	fi, err := fsys.Stat(fqPath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat plugin(%s): %w", fqPath, err)
	}
	if fi.Size() > 100*1024*1024 {
		return nil, fmt.Errorf("plugin(%s) is larger than 100MB", fqPath)
	}
	content, err := fsys.ReadFile(fqPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read plugin(%s): %w", fqPath, err)
	}
	return &pb.LoadPlugin{Name: filepath.Base(fqPath), Plugin: content}, nil
}
