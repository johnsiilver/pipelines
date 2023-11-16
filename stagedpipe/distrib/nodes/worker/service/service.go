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
	"time"

	"github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/version"
	pb "github.com/johnsiilver/pipelines/stagedpipe/distrib/nodes/worker/proto"
	"github.com/johnsiilver/pipelines/stagedpipe/distrib/plugin/client"

	messages "github.com/johnsiilver/pipelines/stagedpipe/distrib/internal/messages/proto"
	"github.com/shirou/gopsutil/v3/mem"
	"google.golang.org/protobuf/encoding/protojson"
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

// Release decrements the WaitGroup for the plugin.
func (p *pluginInfo) Release() {
	p.wg.Done()
}

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
	ticker := time.NewTicker(time.Second * 5)
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
	ctx := stream.Context()

	in, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive request: %w", err)
	}

	og := in.GetOpenGroup()
	if og == nil {
		return fmt.Errorf("first message must be OpenGroup")
	}

	pi, err := s.getPlugin(og.Plugin)
	if err != nil {
		return fmt.Errorf("failed to get plugin(%s): %w", og.Plugin, err)
	}
	defer pi.Release()

	rg, err := pi.plugin.RequestGroup(ctx, og.Config)
	if err != nil {
		return fmt.Errorf("failed to create request group: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		var err error
		for req := range rg.Output() {
			if err != nil {
				continue
			}
			if req.Err != nil {
				rg.Close()
				err = req.Err
			}

			resp := &pb.RequestGroupResp{
				Data: req.Data,
			}

			if sErr := stream.Send(resp); sErr != nil {
				rg.Close()
				err = fmt.Errorf("failed to send request: %w", sErr)
			}
		}
		done <- err
	}()

	for {
		s.block.Wait() // Wait for memory to be available.

		in, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				rg.Close()
				return fmt.Errorf("failed to receive request: %w", err)
			}
		}

		if err := rg.Submit(client.Request{Data: in.GetData()}); err != nil {
			rg.Close()
			return fmt.Errorf("failed to submit request: %w", err)
		}
	}
	if err := rg.Close(); err != nil {
		return fmt.Errorf("failed to close request group: %w", err)
	}
	return <-done
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
