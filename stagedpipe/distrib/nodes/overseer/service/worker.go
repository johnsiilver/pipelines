package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdfs "io/fs"
	"log"
	"net/netip"
	"path/filepath"
	"sync"
	"time"

	"github.com/johnsiilver/broadcaster"

	"github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/internal/p2c"
	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/overseer/proto"
)

var (
	regSendTimeout     = 5 * time.Second
	pluginsLoadTimeout = 5 * time.Minute
)

type plugfs interface {
	stdfs.ReadDirFS
	stdfs.ReadFileFS
	stdfs.StatFS
}

/*
handleRegWorker handles registering a Worker with the Overseer.

This will get all plugins in our plugin directory and send them out to the plugin.
It then sends a finished message.
We then wait for a finished message from the Worker.
We then add the new Worker to its backend pool.
Finally, we move on to handleWorkerUpdates() to handle any plugin updates that need to
go to the Workers.
*/
func (s *Server) handleRegWorker(stream pb.Overseer_SubscribeServer, init *pb.RegisterInit) error {
	addr, err := netip.ParseAddrPort(init.Address)
	if err != nil {
		return fmt.Errorf("the init message had an address(%v) that was not valid: %v", init.Address, err)
	}

	plugCh, err := getPlugins(s.plugFS, s.pluginDir)
	if err != nil {
		return err
	}

	for p := range plugCh {
		if p.Err != nil {
			return fmt.Errorf("while reading plugins for a new worker: %v", p.Err)
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
		ctx, cancel := context.WithTimeout(stream.Context(), regSendTimeout)
		err = subscribeSend(ctx, stream, out)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to send plugin to worker during registration: %w", err)
		}
	}

	// Tell the Worker that we are done with registration.
	finMsg := &pb.SubscribeOut{
		Message: &pb.SubscribeOut_Register{
			Register: &pb.RegisterResp{
				Message: &pb.RegisterResp_Fin{
					Fin: &pb.RegisterFin{},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(stream.Context(), regSendTimeout)
	err = subscribeSend(ctx, stream, finMsg)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to send fin message to worker during registration: %w", err)
	}

	// Wait for the far side to tell us it is done with starting plugins.
	ctx, cancel = context.WithTimeout(stream.Context(), pluginsLoadTimeout)
	defer cancel()
	in, err := subscribeRecv(ctx, stream)
	if err != nil {
		return fmt.Errorf("failed to receive fin message from worker during registration: %w", err)
	}

	if in.GetRegister().GetFin() == nil {
		return fmt.Errorf("expected fin message from worker during registration, got %v", in)
	}

	// Register our new backend with our p2c pool.
	b, err := p2c.NewBackend(addr)
	if err != nil {
		return fmt.Errorf("failed to create backend for coordinator(%s): %w", addr, err)
	}

	if err := s.workers.Add(stream.Context(), b); err != nil {
		return fmt.Errorf("failed to add worker(%s): %w", addr, err)
	}

	// If we are in a test, don't chain to the next function.
	if s.inTest {
		return nil
	}
	return s.handleWorkerUpdates(stream)
}

var ackTimeout = 1 * time.Minute

// handleWorkerUpdates handles updates from the worker. It will block until the worker connection
// is closed or an error occurs.
func (s *Server) handleWorkerUpdates(stream pb.Overseer_SubscribeServer) error {
	mu := sync.Mutex{}
	acks := map[uint64]chan struct{}{}

	castPlugUpdates, err := s.updateWorkers.Receiver(workerUpdatePlugs, 100)
	if err != nil {
		return fmt.Errorf("failed to create plugin update receiver: %w", err)
	}
	defer castPlugUpdates.Close()

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
			if in.GetAck().ErrMsg != "" {
				recvErr <- fmt.Errorf("worker plugin update: responded with err: %s", in.GetAck().ErrMsg)
				return
			}
		}
	}()

	// Handle sending messages to the worker.
	var exitErr error
	ackErr := make(chan error, 1)

	pluginUpdates := getPluginUpdates(stream.Context(), castPlugUpdates)

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case exitErr = <-recvErr:
			return exitErr
		case exitErr = <-ackErr:
			return exitErr
		case update := <-pluginUpdates:
			if update.Err != nil {
				return update.Err
			}
			out := &pb.SubscribeOut{
				Message: &pb.SubscribeOut_UpdateWorker{
					UpdateWorker: update.Data,
				},
			}

			promise := make(chan struct{}, 1)
			mu.Lock()
			acks[update.Data.Id] = promise
			mu.Unlock()

			go func() {
				select {
				case <-stream.Context().Done():
					return
				case <-promise:
					return
				case <-time.After(ackTimeout):
					select {
					case ackErr <- fmt.Errorf("failed to get ack for update(%d) in time allotted", update.Data.Id):
					default:
					}
					return
				}
			}()
			if err := stream.Send(out); err != nil {
				return fmt.Errorf("failed to send plugin update to worker: %w", err)
			}
		}
	}
}

func getPluginUpdates(ctx context.Context, plugUpdates *broadcaster.Receiver[*pb.UpdateWorker]) chan streamResp[*pb.UpdateWorker] {
	updatesCh := make(chan streamResp[*pb.UpdateWorker], 1)
	go func() {
		log.Println("waiting for updates")
		defer close(updatesCh)
		for {
			update := plugUpdates.Next(ctx)
			log.Println("got a broadcast update")
			if update.IsZero() {
				log.Printf("was zero: %v", update.Data())
				updatesCh <- streamResp[*pb.UpdateWorker]{Err: errors.New("pluginUpdates.Next() returned a zero value, which should never happen")}
				return
			}
			updatesCh <- streamResp[*pb.UpdateWorker]{Data: update.Data()}
			log.Println("broadcast received")
		}
	}()
	return updatesCh
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
