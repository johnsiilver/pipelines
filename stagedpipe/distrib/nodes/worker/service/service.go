//go:build linux || darwin

// Package service provides the gRPC service for the worker node. The worker node
// runs stagedpipe plugins.
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v3/mem"
	"google.golang.org/protobuf/encoding/protojson"

	messages "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/version"
	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/worker/proto"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin/client"
)

// TODO(jdoak): Monitor plugins for crashes. If a plugin crashes, restart it.

type pluginInfo struct {
	fqPath  string
	plugin  *client.Plugin
	size    int64
	modTime time.Time
	sum     string

	turndown time.Time

	wg sync.WaitGroup
}

// Name returns the name of the plugin without the full path.
func (p *pluginInfo) Name() string {
	return filepath.Base(p.fqPath)
}

// Release decrements the WaitGroup for the plugin. The WaitGroup is incremented
// by getPlugin().
func (p *pluginInfo) Release() {
	p.wg.Done()
}

// Server implements the gRPC service for the worker node.
type Server struct {
	mu sync.Mutex

	// plugins are the plugins that are currently loaded.
	plugins map[string]*pluginInfo
	// pluginTurndown are plugins that have been upgraded, but are still being used
	// by RequestGroups. Once all RequestGroups using the plugin have finished, the
	// plugin will be removed from this map.
	pluginTurndown map[string][]*pluginInfo

	// memoryBlock is the percentage of memory usage at which we block incoming messages.
	memoryBlock int
	// block is a WaitGroup that is used to block incoming messages when memory usage is high.
	block sync.WaitGroup

	pb.UnimplementedWorkerServer
}

// New creates a new Server instance. overseerAddr is the address of the overseer.
// pluginPath is the path to the plugins directory.
func New(overseerAddr string, pluginPath string, memoryBlock int) (*Server, error) {
	ctx := context.Background()

	if memoryBlock > 99 {
		return nil, fmt.Errorf("memoryBlock must be < 99, was %d", memoryBlock)
	}

	s := &Server{
		plugins:        make(map[string]*pluginInfo),
		pluginTurndown: make(map[string][]*pluginInfo),
		memoryBlock:    memoryBlock,
	}

	entries, err := os.ReadDir(pluginPath)
	if err != nil {
		wd, _ := os.Getwd()
		log.Println("working directory: ", wd)
		return nil, fmt.Errorf("failed to read plugin directory: %w", err)
	}

	wg := sync.WaitGroup{}
	for _, dirEntry := range entries {
		switch {
		case dirEntry.IsDir():
			continue
		case filepath.Ext(dirEntry.Name()) != "":
			continue
		case dirEntry.Name() == "README":
			continue
		}

		fi, err := dirEntry.Info()
		if err != nil {
			log.Printf("could not get file info for plugin %s: %s", dirEntry.Name(), err)
			continue
		}

		fqPath := filepath.Join(pluginPath, dirEntry.Name())
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.loadPlugin(ctx, fqPath, fi); err != nil {
				log.Println(err)
			}
		}()
	}
	wg.Wait()

	if memoryBlock > 0 {
		go s.moderateMemory()
	}

	return s, nil
}

// loadPlugin starts a plugin at fqPath and adds it to the plugins map.
func (s *Server) loadPlugin(ctx context.Context, fqPath string, fi fs.FileInfo) error {
	if err := pluginVersionCheck(fqPath); err != nil {
		return fmt.Errorf("plugin(%s) failed version check: %s", fqPath, err)
	}

	sum, err := hashPlugin(fqPath)
	if err != nil {
		return fmt.Errorf("plugin(%s) failed to get sha256 hash: %s", fqPath, err)
	}

	base := filepath.Base(fqPath)
	if s.pluginHasHash(base, sum) {
		log.Printf("plugin(%s) with sha256 sum(%s) is already loaded, so skipping load", base, sum)
		return nil
	}

	p, err := client.New(fqPath)
	if err != nil {
		return fmt.Errorf("plugin(%s) failed to start: %s", fqPath, err)
	}

	pi := &pluginInfo{
		fqPath:  fqPath,
		plugin:  p,
		size:    fi.Size(),
		modTime: fi.ModTime(),
		sum:     sum,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.plugins[pi.Name()]; !ok {
		s.plugins[pi.Name()] = pi
		return nil
	}
	// Handle the case where the plugin has been updated.
	old := s.plugins[pi.Name()]
	old.turndown = time.Now()

	inTurndown := s.pluginTurndown[old.Name()]
	if inTurndown == nil {
		s.pluginTurndown[old.Name()] = []*pluginInfo{old}
	} else {
		s.pluginTurndown[old.Name()] = append(inTurndown, old)
	}

	go func() {
		log.Printf("plugin(%s) has been upgraded", old.Name())

		// This will block until the plugin is done processing all of its requests.
		// This may take a while, as this requires all RequestGroups using that plugin to finish.
		old.wg.Wait()
		if err := old.plugin.Quit(ctx); err != nil {
			log.Printf("plugin(%s) had Quit called(due to upgrade), but had a non-zero error: %s", old.Name(), err)
			return
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		inTurndown = s.pluginTurndown[old.Name()]
		if len(inTurndown) < 2 {
			delete(s.pluginTurndown, old.Name())
		} else {
			n := make([]*pluginInfo, 0, len(inTurndown)-1)
			for _, entry := range inTurndown {
				if entry.sum == old.sum {
					continue
				}
				n = append(n, entry)
			}
			s.pluginTurndown[old.Name()] = n
		}

		log.Printf("upgraded plugin(%s) exited successfully", old.Name())
	}()

	s.plugins[pi.Name()] = pi

	return nil
}

// pluginHasHash returns true if a loaded plugin with name has the sha256 sum.
func (s *Server) pluginHasHash(name, sum string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	pi, ok := s.plugins[name]
	if !ok {
		return false
	}
	return pi.sum == sum
}

// moderateMemory watches the system memory and blocks gRPC reads if the memory usage
// exceeds some limit.
func (s *Server) moderateMemory() {
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	isSet := false

	for range ticker.C {
		v, err := mem.VirtualMemory()
		if err != nil {
			log.Printf("failed to get memory usage: %s", err)
			continue
		}
		// If we are below the limit, unblock if we are blocking.
		if v.UsedPercent < float64(s.memoryBlock) {
			if isSet {
				isSet = false
				s.block.Done()
			}
			continue
		}
		// If we are already blocking, don't block again.
		if isSet {
			runtime.GC()
			continue
		}
		// We need to block.
		isSet = true
		s.block.Add(1)
		runtime.GC()
	}
}

// RequestGroup is the gRPC endpoint for a client to request a new request group,
// stream data to it and receive data from it.
func (s *Server) RequestGroup(stream pb.Worker_RequestGroupServer) error {
	err := runConnServer(stream, s)
	if err != nil {
		// log.Println("runConnServer returned error: ", err)
		return err
	}

	return nil
}

// getPlugin gets the plugin with name. It adds 1 to the WaitGroup of the plugin.
// This must be decremented when the plugin is done.
func (s *Server) getPlugin(name string) (*pluginInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pi, ok := s.plugins[name]
	if !ok {
		return nil, fmt.Errorf("plugin(%s) not found", name)
	}
	pi.wg.Add(1)

	return pi, nil
}

type stage func() (stage, error)

// connServer is the gRPC service for a single connection.
type connServer struct {
	ctx         context.Context
	coordStream pb.Worker_RequestGroupServer
	cancel      context.CancelFunc
	rg          *client.RequestGroup
	server      *Server
	recvClose   atomic.Bool
	pi          *pluginInfo

	err atomic.Value
}

func runConnServer(coordStream pb.Worker_RequestGroupServer, server *Server) error {
	ctx, cancel := context.WithCancel(coordStream.Context())
	c := &connServer{
		ctx:         ctx,
		cancel:      cancel,
		coordStream: coordStream,
		server:      server,
	}

	stage := c.Open
	var err error
	for {
		stage, err = stage()
		if err != nil {
			return err
		}
		if stage == nil {
			if err := c.getErr(); err != nil {
				return err
			}
			return nil
		}
	}
}

func (c *connServer) setErr(err error) error {
	if err == nil {
		return nil
	}
	c.cancel()
	c.err.CompareAndSwap(nil, err)
	return err
}

func (c *connServer) getErr() error {
	e := c.err.Load()
	if e == nil {
		return nil
	}
	return e.(error)
}

// Open represents the first stage of the connection. We wait for the first message
// from the coordinator, which must be an OpenGroup message. We then create a new
// RequestGroup with the plugin and then jump to the StreamData stage.
func (c *connServer) Open() (stage, error) {
	// log.Println("service: Open started")
	c.ctx, c.cancel = context.WithCancel(c.coordStream.Context())

	in, err := recvOnTimeout[*pb.RequestGroupReq](c.coordStream, 1*time.Second)
	if err != nil {
		err = c.setErr(fmt.Errorf("failed to receive request: %w", err))
		// log.Println(err)
		return nil, err
	}

	log.Println("service: received first message")

	og := in.GetOpenGroup()
	if og == nil {
		// log.Println("service: ERROR: first message was not OpenGroup:\n", in.String())
		return nil, c.setErr(fmt.Errorf("first message was not OpenGroup:\n%s", in.String()))
	}
	// log.Println("service: got OpenGroup message")

	c.pi, err = c.server.getPlugin(og.Plugin)
	if err != nil {
		return nil, c.setErr(fmt.Errorf("failed to get plugin(%s): %w", og.Plugin, err))
	}

	// log.Println("service: loaded plugin")

	c.rg, err = c.pi.plugin.RequestGroup(c.ctx, og.Config)
	if err != nil {
		// log.Println("failed to create request group: ", err)
		return nil, c.setErr(fmt.Errorf("failed to create request group: %w", err))
	}

	return c.StreamData, nil
}

// StreamData represents the stage where we stream data to and from the plugin.
func (c *connServer) StreamData() (stage, error) {
	// log.Println("service: StreamData started")
	// defer log.Println("service: StreamData ended")
	defer c.pi.Release()
	defer c.rg.Close()

	errCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		// defer log.Println("service: sendToCoord done")
		defer wg.Done()
		defer close(errCh)

		if err := c.sendToCoord(c.rg); err != nil {
			select {
			case <-c.ctx.Done():
				return
			case errCh <- err:
			default:
			}
			return
		}
	}()

	if err := c.sendToPlugin(c.rg); err != nil {
		return nil, c.setErr(err)
	}
	// log.Println("service sendToPlugin done")

	wg.Wait()
	if err := <-errCh; err != nil {
		return nil, c.setErr(err)
	}
	return nil, nil
}

// sendToPlugin sends data from the coordinator to the plugin.
func (c *connServer) sendToPlugin(plugRG *client.RequestGroup) (err error) {
	// log.Println("service: sentToPlugin started")
	defer func() {
		err = plugRG.Close()
		if err != nil {
			c.setErr(err)
		}
	}()

	for {
		c.server.block.Wait() // Wait for memory to be available.
		// log.Println("service: passed block")
		in, err := c.coordStream.Recv()
		if err != nil {
			if err == io.EOF {
				c.recvClose.Store(true)
				return nil
			}
			// log.Println("received error from coordinator: ", err)
			return fmt.Errorf("failed to receive request: %w", err)
		}

		if err := plugRG.Submit(client.Request{Data: in.GetData().Data}); err != nil {
			// log.Println("failed to submit request to plugin: ", err)
			return fmt.Errorf("failed to submit request to plugin: %w", err)
		}
		// log.Println("service: sent data to plugin")
	}
}

// sendToCoord sends data from the plugin to the coordinator.
func (c *connServer) sendToCoord(plugRG *client.RequestGroup) error {
	// log.Println("service: sendToCoord started")

	var req client.Request
	var ok bool
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case req, ok = <-plugRG.Output():
			if !ok {
				break
			}
			if req.Err != nil {
				// log.Println("plugin returned an error: ", req.Err)
				return c.setErr(fmt.Errorf("plugin returned an error: %w", req.Err))
			}

			resp := &pb.RequestGroupResp{
				Message: &pb.RequestGroupResp_Data{
					Data: &pb.Data{
						Data: req.Data,
						Seq:  req.Seq,
					},
				},
			}

			if err := c.coordStream.Send(resp); err != nil {
				// log.Println("failed to send request back to the Coordinator: ", err)
				return c.setErr(fmt.Errorf("failed to send request back to the Coordinator: %w", err))
			}
			// log.Println("service: sent data to coordinator")
		}
		if !ok {
			break
		}
	}

	if !c.recvClose.Load() {
		return fmt.Errorf("plugin closed stream, but coordinator did not")
	}

	return nil
}

// pluginVersionCheck checks the version of the plugin to ensure it is compatible with the worker node.
func pluginVersionCheck(path string) error {
	cmd := exec.Command(path, "-version")
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to run plugin version check: %w", err)
	}
	v := &messages.Version{}
	if err := protojson.Unmarshal(out, v); err != nil {
		return fmt.Errorf("failed to unmarshal plugin version: %w", err)
	}
	if !version.CanUse(v) {
		return fmt.Errorf("plugin version %v is incompatible with worker node version %v", v, version.Semantic)
	}
	return nil
}

// hashPlugin returns the sha256 hash of the plugin at fqPath.
func hashPlugin(fqPath string) (string, error) {
	f, err := os.Open(fqPath)
	if err != nil {
		return "", fmt.Errorf("failed to open plugin file: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("failed to hash plugin file: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type streamMsg[T any] struct {
	data T
	err  error
}

// streamRecv is an interface that allows us to receive messages from a stream.
type streamRecv[T any] interface {
	Recv() (T, error)
}

// recvOnTimeout receives a message from the stream, but will timeout if it takes too long.
func recvOnTimeout[T any](receiver streamRecv[T], timeout time.Duration) (T, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Spin off a goroutine to do the receive.
	msg := make(chan streamMsg[T], 1)
	go func() {
		in, err := receiver.Recv()
		if err != nil {
			msg <- streamMsg[T]{err: fmt.Errorf("failed to receive request: %w", err)}
			return
		}
		msg <- streamMsg[T]{data: in}
	}()

	// Now wait for either the timeout or the message.
	select {
	case <-timer.C:
		var t T
		return t, fmt.Errorf("timeout waiting for stream open message")
	case m := <-msg:
		if m.err != nil {
			var t T
			return t, m.err
		}
		return m.data, nil
	}
}
