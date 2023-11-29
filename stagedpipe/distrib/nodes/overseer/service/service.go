package service

// do something with the pluginUpdates so that the workers get updated
import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/internal/p2c"
	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/overseer/proto"
	"google.golang.org/grpc/peer"

	"github.com/fsnotify/fsnotify"
	osFS "github.com/gopherfs/fs/io/os"
	"github.com/johnsiilver/broadcaster"
)

var (
	coordUpdateWorkers = []string{"coordinator", "updateWorkers"}
	workerUpdatePlugs  = []string{"worker", "updatePlugs"}
)

type pluginInfo struct {
	modTime time.Time
	fqPath  string
	sha256  string
	size    int64
}

func (p pluginInfo) Name() string {
	return filepath.Base(p.fqPath)
}

type plugins struct {
	m  map[string]pluginInfo
	mu sync.Mutex
}

func (p *plugins) add(pi pluginInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.m[pi.Name()] = pi
}

func (p *plugins) remove(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.m, name)
}

func (p *plugins) get(name string) (pluginInfo, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	v, ok := p.m[name]
	return v, ok
}

type Server struct {
	pluginDir     string
	loadedPlugins *plugins
	coordinators  *p2c.Selector
	workers       *p2c.Selector

	msgID atomic.Uint64

	updateCoordinator *broadcaster.Sender[*pb.Workers]
	updateWorkers     *broadcaster.Sender[*pb.UpdateWorker]

	pb.UnimplementedOverseerServer
}

// New creates a new Server. pluginDir is the directory where plugins are stored.
func New(pluginDir string) (*Server, error) {
	stat, err := os.Stat(pluginDir)
	if err != nil {
		return nil, fmt.Errorf("failed to stat plugin directory(%s): %w", pluginDir, err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("plugin directory(%s) is not a directory", pluginDir)
	}

	coordinators, err := p2c.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator selector: %w", err)
	}

	workers, err := p2c.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create worker selector: %w", err)
	}

	s := &Server{
		pluginDir:         pluginDir,
		loadedPlugins:     &plugins{m: make(map[string]pluginInfo)},
		coordinators:      coordinators,
		workers:           workers,
		updateCoordinator: broadcaster.New[*pb.Workers](),
		updateWorkers:     broadcaster.New[*pb.UpdateWorker](),
	}

	go s.plugins()

	return s, nil
}

type streamResp[T any] struct {
	Data T
	Err  error
}

// Coordinator returns the address of a coordinator node to use. This is based on the
// power of 2 choices algorithm.
func (s *Server) Coordinator(ctx context.Context, in *pb.CoordinatorReq) (*pb.CoordinatorResp, error) {
	b, err := s.coordinators.Next()
	if err != nil {
		return nil, err
	}

	return &pb.CoordinatorResp{
		Address: b.Address().String(),
	}, nil
}

// Subscribe is called by a node to subscribe to the cluster. The node will receive
// updates on the cluster as they occur.
func (s *Server) Subscribe(stream pb.Overseer_SubscribeServer) error {
	return s.handleRegistration(stream)
}

// handleRegistration handles the registration of a node with the overseer. It then passes
// the connection off to another handler that handles any updates that occur.
func (s *Server) handleRegistration(stream pb.Overseer_SubscribeServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), 5*time.Second)
	defer cancel()

	registerMsg, err := subscribeRecv(ctx, stream)
	if err != nil {
		return fmt.Errorf("failed to receive register message in time limit: %w", err)
	}

	reg := registerMsg.GetRegister()
	if reg == nil {
		return fmt.Errorf("expected register message, got %v", registerMsg)
	}
	init := reg.GetInit()
	if init == nil {
		return fmt.Errorf("expected init message, got %v", reg)
	}
	_, err = netip.ParseAddrPort(init.GetAddress())
	if err != nil {
		return fmt.Errorf("failed to parse address(%s) in register init message: %w", init.GetAddress(), err)
	}

	p, _ := peer.FromContext(stream.Context())

	switch init.Type {
	case pb.NodeType_NT_COORDINATOR:
		log.Printf("registering coordinator at(grpc address %s) for endpoint address: %s", p.Addr.String(), init.Address)
		return s.handleRegCoordinator(stream, init)
	case pb.NodeType_NT_WORKER:
		log.Printf("registering worker at(grpc address %s) for endpoint address: %s", p.Addr.String(), init.Address)
		return s.handleRegWorker(stream, init)
	}
	return fmt.Errorf("unsupported node type: %v", init.Type)
}

func subscribeSend(ctx context.Context, stream pb.Overseer_SubscribeServer, msg *pb.SubscribeOut) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled before sending message")
	case err := <-func() chan error {
		ch := make(chan error, 1)
		go func() {
			defer close(ch)
			err := stream.Send(msg)
			if err != nil {
				ch <- err
			}
		}()
		return ch
	}():
		if err != nil {
			return err
		}
	}
	return nil
}

// plugins stores information on the plugins in the plugin directory. If a plugin is updated,
// it will send the new plugin information on the pluginUpdates channel.
func (s *Server) plugins() {
	fsys, err := osFS.New()
	if err != nil {
		panic(fmt.Errorf("failed to create os fs: %w", err))
	}

	// Create new watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	updates := make(chan string, 100)

	// Start listening for events.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Write) {
					log.Println("plugin updated: ", event.Name)
					updates <- event.Name
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					continue
				}
				log.Println("problem with file watcher:", err)
			}
		}
	}()

	if err := watcher.Add(s.pluginDir); err != nil {
		log.Fatalf("failed to add plugin directory to watcher: %s", err)
	}

	// Now get all the initial plugin information and store it.
	dirInfo, err := fsys.ReadDir(s.pluginDir)
	if err != nil {
		log.Fatalf("failed to read plugin directory: %s", err)
	}
	for _, entry := range dirInfo {
		if !entry.IsDir() {
			fqpath := filepath.Join(s.pluginDir, entry.Name())

			fi, err := entry.Info()
			if err != nil {
				log.Fatalf("failed to stat plugin file(%s): %s", fqpath, err)
			}

			hash, err := hashFile(fsys, fqpath)
			if err != nil {
				log.Fatalf("failed to hash plugin file(%s): %s", fqpath, err)
			}

			pi := pluginInfo{
				fqPath:  fqpath,
				sha256:  hash,
				size:    fi.Size(),
				modTime: fi.ModTime(),
			}

			s.loadedPlugins.add(pi)
		}
	}

	// Now deal with updates.
	for fqPath := range updates {
		fi, err := fsys.Stat(fqPath)
		if err != nil {
			log.Printf("failed to stat plugin file(%s): %s", fqPath, err)
			continue
		}
		hash, err := hashFile(fsys, fqPath)
		if err != nil {
			log.Printf("failed to hash plugin file(%s): %s", fqPath, err)
			continue
		}
		name := filepath.Base(fqPath)

		newPI := pluginInfo{fqPath: fqPath, sha256: hash, size: fi.Size(), modTime: fi.ModTime()}

		lp, err := getPlugin(fsys, fqPath)
		if err != nil {
			log.Printf("failed to get plugin(%s): %s", fqPath, err)
			continue
		}

		uw, err := broadcaster.NewMessage[*pb.UpdateWorker](
			workerUpdatePlugs,
			&pb.UpdateWorker{
				Id: s.msgID.Add(1),
				Message: &pb.UpdateWorker_Plugin{
					Plugin: lp,
				},
			},
		)
		if err != nil {
			log.Fatalf("failed to create update worker message for plugin(%s): %s", fqPath, err)
		}

		// If the plugin is not loaded, we need to send an update to all workers.
		oldPI, ok := s.loadedPlugins.get(name)
		if !ok {
			if err := s.updateWorkers.Send(uw); err != nil {
				log.Fatalf("failed to send update worker message for plugin(%s): %s", fqPath, err)
			}
			continue
		}
		if !ok || oldPI.sha256 != newPI.sha256 {
			if err := s.updateWorkers.Send(uw); err != nil {
				log.Fatalf("failed to send update worker message for plugin(%s): %s", fqPath, err)
			}
		}
	}
}

func subscribeRecv(ctx context.Context, stream pb.Overseer_SubscribeServer) (*pb.SubscribeIn, error) {
	var msg *pb.SubscribeIn

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancelled before receiving message")
	case err := <-func() chan error {
		ch := make(chan error, 1)

		go func() {
			defer close(ch)
			var e error
			msg, e = stream.Recv()
			if e != nil {
				ch <- e
			}
		}()
		return ch
	}():
		if err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// hashFile returns the sha256 hash of the file at fqpath.
func hashFile(fsys plugfs, fqpath string) (string, error) {
	file, err := fsys.Open(fqpath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %s", err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("failed to calculate hash: %s", err)
	}

	hashValue := hash.Sum(nil)
	return fmt.Sprintf("%x", hashValue), nil
}
