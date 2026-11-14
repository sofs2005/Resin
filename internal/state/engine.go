package state

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/Resinat/Resin/internal/model"
)

// NodeLatencyDirtyKey is the composite key for the node_latency dirty set.
type NodeLatencyDirtyKey = model.NodeLatencyKey

// LeaseDirtyKey is the composite key for the leases dirty set.
type LeaseDirtyKey = model.LeaseKey

// SubscriptionNodeDirtyKey is the composite key for the subscription_nodes dirty set.
type SubscriptionNodeDirtyKey = model.SubscriptionNodeKey

// CacheReaders provides callbacks for reading current in-memory values at flush time.
// If a reader returns nil for a key marked OpUpsert, the key is
// treated as a delete (the object was removed between mark and flush).
type CacheReaders struct {
	ReadNodeStatic       func(hash string) *model.NodeStatic
	ReadNodeDynamic      func(hash string) *model.NodeDynamic
	ReadNodeLatency      func(key NodeLatencyDirtyKey) *model.NodeLatency
	ReadLease            func(key LeaseDirtyKey) *model.Lease
	ReadSubscriptionNode func(key SubscriptionNodeDirtyKey) *model.SubscriptionNode
}

// StateEngine is the single write entry point for all persistence operations.
// Strong-persist data (config, platforms, subscriptions, rules) goes through
// transactional writes to state.db. Weak-persist data (nodes, leases) is
// marked dirty and batch-flushed to cache.db.
//
// When persistEnabled is false (RESIN_CACHE_PERSIST=off, the default) the weak
// persist layer is disabled entirely: every Mark* call becomes a no-op and the
// dirty sets never grow, so cache.db is never written and the process idles at
// zero steady-state disk writes for topology data. Strong-persist writes
// (state.db) are unaffected because they are not routed through the dirty sets.
type StateEngine struct {
	*StateRepo
	*CacheRepo

	// persistEnabled gates the weak-persist (cache.db) write path. It defaults
	// to enabled so existing embedders keep the documented behaviour; the
	// runtime disables it explicitly when cache persistence is off.
	persistEnabled atomic.Bool

	dirtyNodesStatic       *DirtySet[string]
	dirtyNodesDynamic      *DirtySet[string]
	dirtyNodeLatency       *DirtySet[NodeLatencyDirtyKey]
	dirtyLeases            *DirtySet[LeaseDirtyKey]
	dirtySubscriptionNodes *DirtySet[SubscriptionNodeDirtyKey]
}

// newStateEngine creates a StateEngine with the given repos.
func newStateEngine(stateRepo *StateRepo, cacheRepo *CacheRepo) *StateEngine {
	e := &StateEngine{
		StateRepo:              stateRepo,
		CacheRepo:              cacheRepo,
		dirtyNodesStatic:       NewDirtySet[string](),
		dirtyNodesDynamic:      NewDirtySet[string](),
		dirtyNodeLatency:       NewDirtySet[NodeLatencyDirtyKey](),
		dirtyLeases:            NewDirtySet[LeaseDirtyKey](),
		dirtySubscriptionNodes: NewDirtySet[SubscriptionNodeDirtyKey](),
	}
	e.persistEnabled.Store(true)
	return e
}

// SetPersistEnabled enables or disables the weak-persist (cache.db) write path.
// Disabling it is one-way safe: already-queued dirty keys stay in the sets until
// the next flush but no new key is ever added. Callers should disable it before
// any Mark* call happens (i.e. right after bootstrap) and must not start the
// CacheFlushWorker afterwards.
func (e *StateEngine) SetPersistEnabled(enabled bool) {
	e.persistEnabled.Store(enabled)
}

// PersistEnabled reports whether weak-persist writes to cache.db are enabled.
func (e *StateEngine) PersistEnabled() bool {
	return e.persistEnabled.Load()
}

// --- Weak-persist methods (dirty-mark only) ---

func (e *StateEngine) MarkNodeStatic(hash string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtyNodesStatic.MarkUpsert(hash)
}

func (e *StateEngine) MarkNodeStaticDelete(hash string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtyNodesStatic.MarkDelete(hash)
}

func (e *StateEngine) MarkNodeDynamic(hash string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtyNodesDynamic.MarkUpsert(hash)
}

func (e *StateEngine) MarkNodeDynamicDelete(hash string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtyNodesDynamic.MarkDelete(hash)
}

func (e *StateEngine) MarkNodeLatency(nodeHash, domain string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtyNodeLatency.MarkUpsert(NodeLatencyDirtyKey{NodeHash: nodeHash, Domain: domain})
}

func (e *StateEngine) MarkNodeLatencyDelete(nodeHash, domain string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtyNodeLatency.MarkDelete(NodeLatencyDirtyKey{NodeHash: nodeHash, Domain: domain})
}

func (e *StateEngine) MarkLease(platformID, account string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtyLeases.MarkUpsert(LeaseDirtyKey{PlatformID: platformID, Account: account})
}

func (e *StateEngine) MarkLeaseDelete(platformID, account string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtyLeases.MarkDelete(LeaseDirtyKey{PlatformID: platformID, Account: account})
}

func (e *StateEngine) MarkSubscriptionNode(subID, nodeHash string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtySubscriptionNodes.MarkUpsert(SubscriptionNodeDirtyKey{SubscriptionID: subID, NodeHash: nodeHash})
}

func (e *StateEngine) MarkSubscriptionNodeDelete(subID, nodeHash string) {
	if !e.persistEnabled.Load() {
		return
	}
	e.dirtySubscriptionNodes.MarkDelete(SubscriptionNodeDirtyKey{SubscriptionID: subID, NodeHash: nodeHash})
}

// DirtyCount returns the total number of dirty entries across all sets.
func (e *StateEngine) DirtyCount() int {
	return e.dirtyNodesStatic.Len() +
		e.dirtyNodesDynamic.Len() +
		e.dirtyNodeLatency.Len() +
		e.dirtyLeases.Len() +
		e.dirtySubscriptionNodes.Len()
}

// classifyDirtySet splits a drained dirty-set snapshot into upsert values and
// delete keys. For OpUpsert entries, the reader is called to fetch the current
// in-memory value; a nil return is treated as a delete.
func classifyDirtySet[K comparable, V any](
	drained map[K]DirtyOp,
	reader func(K) *V,
) (upserts []V, deletes []K) {
	for key, op := range drained {
		if op == OpDelete {
			deletes = append(deletes, key)
			continue
		}
		v := reader(key)
		if v == nil {
			deletes = append(deletes, key)
		} else {
			upserts = append(upserts, *v)
		}
	}
	return
}

// FlushDirtySets drains all dirty sets, reads current values via readers,
// and batch-writes to cache.db in a single transaction.
// On failure, undrained entries are merged back.
func (e *StateEngine) FlushDirtySets(readers CacheReaders) error {
	// Drain all sets atomically (each set is individually atomic).
	drainedStatic := e.dirtyNodesStatic.Drain()
	drainedSubNodes := e.dirtySubscriptionNodes.Drain()
	drainedDynamic := e.dirtyNodesDynamic.Drain()
	drainedLatency := e.dirtyNodeLatency.Drain()
	drainedLeases := e.dirtyLeases.Drain()

	// Re-merge helper on failure.
	remerge := func() {
		e.dirtyNodesStatic.Merge(drainedStatic)
		e.dirtySubscriptionNodes.Merge(drainedSubNodes)
		e.dirtyNodesDynamic.Merge(drainedDynamic)
		e.dirtyNodeLatency.Merge(drainedLatency)
		e.dirtyLeases.Merge(drainedLeases)
	}

	// Classify each dirty set into upsert values and delete keys.
	upsertStatic, deleteStatic := classifyDirtySet(drainedStatic, readers.ReadNodeStatic)
	upsertSubNodes, deleteSubNodes := classifyDirtySet(drainedSubNodes, readers.ReadSubscriptionNode)
	upsertDynamic, deleteDynamic := classifyDirtySet(drainedDynamic, readers.ReadNodeDynamic)
	upsertLatency, deleteLatency := classifyDirtySet(drainedLatency, readers.ReadNodeLatency)
	upsertLeases, deleteLeases := classifyDirtySet(drainedLeases, readers.ReadLease)

	// Execute all writes in a single transaction.
	if err := e.CacheRepo.FlushTx(FlushOps{
		UpsertNodesStatic:       upsertStatic,
		DeleteNodesStatic:       deleteStatic,
		UpsertSubscriptionNodes: upsertSubNodes,
		DeleteSubscriptionNodes: deleteSubNodes,
		UpsertNodesDynamic:      upsertDynamic,
		DeleteNodesDynamic:      deleteDynamic,
		UpsertNodeLatency:       upsertLatency,
		DeleteNodeLatency:       deleteLatency,
		UpsertLeases:            upsertLeases,
		DeleteLeases:            deleteLeases,
	}); err != nil {
		remerge()
		return fmt.Errorf("flush: %w", err)
	}

	log.Printf("[state] flushed dirty sets: static=%d, sub_nodes=%d, dynamic=%d, latency=%d, leases=%d",
		len(drainedStatic), len(drainedSubNodes), len(drainedDynamic), len(drainedLatency), len(drainedLeases))
	return nil
}
