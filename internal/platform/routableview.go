// Package platform provides Platform types and the sharded routable view.
package platform

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/Resinat/Resin/internal/node"
)

const numShards = 64

// ReadOnlyView exposes only the read operations of RoutableView.
// This is the interface vended to external callers (data plane, API)
// so they cannot bypass FullRebuild/NotifyDirty to mutate the set.
type ReadOnlyView interface {
	Contains(h node.Hash) bool
	Size() int
	HealthySize() int
	RandomPick(rng *rand.Rand) (node.Hash, bool)
	RandomPickPreferHealthy(rng *rand.Rand) (node.Hash, bool)
	Range(fn func(node.Hash) bool)
}

// RoutableView is a 64-shard concurrent set supporting O(1) random pick,
// O(1) add, O(1) remove, and O(1) contains.
//
// It tracks two memberships:
//   - available: all routable nodes (circuit closed + filters pass)
//   - healthy: stable subset preferred for routing
type RoutableView struct {
	shards        [numShards]shard
	healthyShards [numShards]shard
	size          atomic.Int64 // available count
	healthySize   atomic.Int64
}

type shard struct {
	mu    sync.RWMutex
	nodes []node.Hash
	index map[node.Hash]int // hash → position in nodes slice
}

// NewRoutableView creates an empty RoutableView.
func NewRoutableView() *RoutableView {
	rv := &RoutableView{}
	for i := range rv.shards {
		rv.shards[i].index = make(map[node.Hash]int)
		rv.healthyShards[i].index = make(map[node.Hash]int)
	}
	return rv
}

// shardFor returns the shard index for a given hash.
func shardFor(h node.Hash) int {
	// Use first byte for shard selection.
	return int(h[0]) % numShards
}

func shardAdd(s *shard, size *atomic.Int64, h node.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.index[h]; ok {
		return
	}
	s.index[h] = len(s.nodes)
	s.nodes = append(s.nodes, h)
	size.Add(1)
}

func shardRemove(s *shard, size *atomic.Int64, h node.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.index[h]
	if !ok {
		return
	}
	last := len(s.nodes) - 1
	if idx != last {
		s.nodes[idx] = s.nodes[last]
		s.index[s.nodes[idx]] = idx
	}
	s.nodes = s.nodes[:last]
	delete(s.index, h)
	size.Add(-1)
}

func shardContains(s *shard, h node.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.index[h]
	return ok
}

func shardRandomPick(shards *[numShards]shard, total int, rng *rand.Rand) (node.Hash, bool) {
	if total == 0 {
		return node.Zero, false
	}
	target := rng.IntN(total)
	for i := range shards {
		s := &shards[i]
		s.mu.RLock()
		n := len(s.nodes)
		if target < n {
			h := s.nodes[target]
			s.mu.RUnlock()
			return h, true
		}
		target -= n
		s.mu.RUnlock()
	}
	return node.Zero, false
}

// Add inserts a hash into the available set. No-op if already present.
// Does not change healthy membership.
func (rv *RoutableView) Add(h node.Hash) {
	shardAdd(&rv.shards[shardFor(h)], &rv.size, h)
}

// AddHealthy inserts a hash into both available and healthy sets.
func (rv *RoutableView) AddHealthy(h node.Hash) {
	si := shardFor(h)
	shardAdd(&rv.shards[si], &rv.size, h)
	shardAdd(&rv.healthyShards[si], &rv.healthySize, h)
}

// SetMembership ensures the hash is present/absent according to routable/healthy.
// routable=false removes from both sets.
func (rv *RoutableView) SetMembership(h node.Hash, routable, healthy bool) {
	si := shardFor(h)
	if !routable {
		shardRemove(&rv.shards[si], &rv.size, h)
		shardRemove(&rv.healthyShards[si], &rv.healthySize, h)
		return
	}
	shardAdd(&rv.shards[si], &rv.size, h)
	if healthy {
		shardAdd(&rv.healthyShards[si], &rv.healthySize, h)
	} else {
		shardRemove(&rv.healthyShards[si], &rv.healthySize, h)
	}
}

// Remove deletes a hash from both available and healthy sets. No-op if absent.
func (rv *RoutableView) Remove(h node.Hash) {
	si := shardFor(h)
	shardRemove(&rv.shards[si], &rv.size, h)
	shardRemove(&rv.healthyShards[si], &rv.healthySize, h)
}

// Contains returns true if the hash is in the available set.
func (rv *RoutableView) Contains(h node.Hash) bool {
	return shardContains(&rv.shards[shardFor(h)], h)
}

// Size returns the total number of available hashes across all shards.
func (rv *RoutableView) Size() int {
	return int(rv.size.Load())
}

// HealthySize returns the number of healthy/stable hashes.
func (rv *RoutableView) HealthySize() int {
	return int(rv.healthySize.Load())
}

// Clear removes all entries from all shards.
func (rv *RoutableView) Clear() {
	for i := range rv.shards {
		s := &rv.shards[i]
		s.mu.Lock()
		s.nodes = s.nodes[:0]
		s.index = make(map[node.Hash]int)
		s.mu.Unlock()

		hs := &rv.healthyShards[i]
		hs.mu.Lock()
		hs.nodes = hs.nodes[:0]
		hs.index = make(map[node.Hash]int)
		hs.mu.Unlock()
	}
	rv.size.Store(0)
	rv.healthySize.Store(0)
}

// RandomPick selects a random hash from the available set.
// Returns ok=false if the view is empty.
func (rv *RoutableView) RandomPick(rng *rand.Rand) (node.Hash, bool) {
	return shardRandomPick(&rv.shards, rv.Size(), rng)
}

// RandomPickPreferHealthy selects from the healthy set when non-empty,
// otherwise falls back to the available set.
func (rv *RoutableView) RandomPickPreferHealthy(rng *rand.Rand) (node.Hash, bool) {
	if hs := rv.HealthySize(); hs > 0 {
		return shardRandomPick(&rv.healthyShards, hs, rng)
	}
	return rv.RandomPick(rng)
}

// Range calls fn for each available hash in the view. If fn returns false, iteration stops.
func (rv *RoutableView) Range(fn func(node.Hash) bool) {
	for i := range rv.shards {
		s := &rv.shards[i]
		s.mu.RLock()
		for _, h := range s.nodes {
			if !fn(h) {
				s.mu.RUnlock()
				return
			}
		}
		s.mu.RUnlock()
	}
}
