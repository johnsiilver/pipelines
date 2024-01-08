// Package p2c implements a power of two choices load balancer.
// This was made popular in this paper:
// https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf
// That paper is based on the work of:
// https://homes.cs.washington.edu/~karlin/papers/AzarBKU99.pdf
//
// This implementation uses a mutex when adding or removing backends. However,
// it does not use a mutex when reading the backends. Adding and removing backends
// is expected to be a rare operation, which results in a O(n) operation, where n
// is the number of backends. Reading the backends is expected to be a very common
// operation, which results in a O(1) operation.
package p2c

import (
	"context"
	"fmt"
	"math/rand"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Backend is a representation of a Backend with a weight for use in the p2c algorithm.
type Backend struct {
	address netip.AddrPort

	weight atomic.Int32
}

// NewBackend creates a new Backend.
func NewBackend(address netip.AddrPort) *Backend {
	b := &Backend{
		address: address,
	}
	return b
}

// validate validates the Backend is valid.
func (b *Backend) validate() error {
	if !b.address.IsValid() {
		return fmt.Errorf("address is invalid")
	}
	if b.address.Port() == 0 {
		return fmt.Errorf("port cannot be 0")
	}
	return nil
}

func (w *Backend) Address() netip.AddrPort {
	return w.address
}

// Weight returns the weight of the Backend.
func (w *Backend) Weight() int32 {
	return w.weight.Load()
}

// AddWeight adds i to the weight of the Backend. i cannot result in a weight < 0 or it will panic,
// similar to a sync.WaitGroup.
func (w *Backend) AddWeight(i int32) {
	if w.weight.Add(i) < 0 {
		panic("weight cannot be < 0, there is a bug")
	}
}

// Selector implements a power of two choices selector.
type Selector struct {
	// backends is the list of backends. This can be accessed for reading without a lock.
	// However, if you want to modify the backends, you must use the lock.
	backends atomic.Pointer[[]*Backend]
	// rand is the random number generator used to select backends.
	rand *rand.Rand
	// mu protects backends when adding or removing backends.
	mu sync.Mutex
}

// New creates a new Selector instance.
func New() *Selector {
	sp := &Selector{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	backends := []*Backend{}
	sp.backends.Store(&backends)

	return sp
}

// Backends returns the list of backends. The returned slice SHOULD NOT BE MODIFIED.
// Modifying the returned slice will result in undefined behavior.
func (s *Selector) Backends() []*Backend {
	return *(s.backends.Load())
}

// Next returns the next backend to use, but does not adjust the weight.
func (s *Selector) Next() (*Backend, error) {
	backends := *(s.backends.Load())
	if len(backends) == 0 {
		return nil, fmt.Errorf("no backends available")
	}

	x := s.rand.Int31n(int32(len(backends)))
	y := s.rand.Int31n(int32(len(backends)))

	if backends[x].Weight() < backends[y].Weight() {
		return backends[x], nil
	}
	return backends[y], nil
}

// Add adds a backend to the p2c pool.
func (s *Selector) Add(ctx context.Context, b *Backend) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := b.validate(); err != nil {
		return fmt.Errorf("invalid backend: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// We don't have any backends, so just add this and go.
	backends := *(s.backends.Load())
	if len(backends) == 0 {
		back := []*Backend{b}
		s.backends.Store(&back)
		return nil
	}

	// Let's see if the backend already exists, if so, then we don't add it.
	x := s.findBackend(ctx, b, backends)
	if x != -1 { // -1 means not found
		return nil
	}

	newBackends := make([]*Backend, len(backends)+1)
	copy(newBackends, backends)
	newBackends[len(backends)] = b

	// We sort so another function can do a search on the contents.
	sort.Slice(
		newBackends,
		func(i, j int) bool {
			return newBackends[i].address.String() < newBackends[j].address.String()
		},
	)
	s.backends.Store(&newBackends)

	return nil
}

// Remove removes a backend from the p2c pool.
func (s *Selector) Remove(ctx context.Context, b *Backend) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := b.validate(); err != nil {
		return fmt.Errorf("invalid backend: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	backends := *(s.backends.Load())
	i := s.findBackend(ctx, b, backends)

	// Node was not found.
	if i == -1 {
		return nil
	}

	newBackends := make([]*Backend, len(backends))
	copy(newBackends, backends)

	newBackends = append(newBackends[:i], newBackends[i+1:]...)
	s.backends.Store(&newBackends)

	return nil
}

// findBackend finds the index of a backend in a slice of backends. This is for internal use.
func (s *Selector) findBackend(ctx context.Context, b *Backend, backends []*Backend) int {
	n := len(backends)

	i := sort.Search(
		n,
		func(i int) bool {
			entry := (backends)[i]
			if entry.address.Addr().Compare(b.address.Addr()) != 0 {
				return false
			}
			if entry.address.Port() != b.address.Port() {
				return false
			}
			return true
		},
	)
	if i == n {
		return -1
	}
	return i
}
