package state

import (
	"testing"

	"github.com/Resinat/Resin/internal/model"
)

// nilCacheReaders returns readers that resolve every key to nil, which is enough
// for exercising the empty-dirty-set flush path.
func nilCacheReaders() CacheReaders {
	return CacheReaders{
		ReadNodeStatic:       func(string) *model.NodeStatic { return nil },
		ReadNodeDynamic:      func(string) *model.NodeDynamic { return nil },
		ReadNodeLatency:      func(NodeLatencyDirtyKey) *model.NodeLatency { return nil },
		ReadLease:            func(LeaseDirtyKey) *model.Lease { return nil },
		ReadSubscriptionNode: func(SubscriptionNodeDirtyKey) *model.SubscriptionNode { return nil },
	}
}

// TestStateEngine_PersistDisabled_MarksAreNoOps verifies that disabling cache
// persistence turns every weak-persist Mark* call into a no-op so the dirty sets
// can never grow (and therefore nothing is ever written to cache.db).
func TestStateEngine_PersistDisabled_MarksAreNoOps(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	if !engine.PersistEnabled() {
		t.Fatal("persist should be enabled by default")
	}

	engine.SetPersistEnabled(false)
	if engine.PersistEnabled() {
		t.Fatal("PersistEnabled() = true after SetPersistEnabled(false)")
	}

	engine.MarkNodeStatic("hash-1")
	engine.MarkNodeStaticDelete("hash-2")
	engine.MarkNodeDynamic("hash-1")
	engine.MarkNodeDynamicDelete("hash-2")
	engine.MarkNodeLatency("hash-1", "example.com")
	engine.MarkNodeLatencyDelete("hash-1", "example.com")
	engine.MarkLease("plat-1", "acct-1")
	engine.MarkLeaseDelete("plat-1", "acct-1")
	engine.MarkSubscriptionNode("sub-1", "hash-1")
	engine.MarkSubscriptionNodeDelete("sub-1", "hash-1")

	if got := engine.DirtyCount(); got != 0 {
		t.Fatalf("DirtyCount() = %d after marks with persistence disabled, want 0", got)
	}

	// Read paths must stay functional: cache.db is still loaded at bootstrap.
	if _, err := engine.LoadAllNodesStatic(); err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if _, err := engine.LoadAllLeases(); err != nil {
		t.Fatalf("LoadAllLeases: %v", err)
	}

	// Flushing an empty dirty set must be a no-op instead of an error.
	if err := engine.FlushDirtySets(nilCacheReaders()); err != nil {
		t.Fatalf("FlushDirtySets: %v", err)
	}
	if got := engine.DirtyCount(); got != 0 {
		t.Fatalf("DirtyCount() = %d after flush, want 0", got)
	}
}

// TestStateEngine_PersistEnabled_MarksGrowDirtySet is the counterpart check: with
// persistence on, marks are collected exactly as before.
func TestStateEngine_PersistEnabled_MarksGrowDirtySet(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	engine.SetPersistEnabled(true)
	engine.MarkNodeStatic("hash-1")
	engine.MarkNodeDynamic("hash-1")
	engine.MarkNodeLatency("hash-1", "example.com")
	engine.MarkLease("plat-1", "acct-1")
	engine.MarkSubscriptionNode("sub-1", "hash-1")

	if got := engine.DirtyCount(); got != 5 {
		t.Fatalf("DirtyCount() = %d, want 5", got)
	}
}

// TestStateEngine_PersistToggledOffStopsNewMarks verifies that disabling after
// some marks keeps the already-queued keys but stops the sets from growing.
func TestStateEngine_PersistToggledOffStopsNewMarks(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	engine.MarkNodeStatic("hash-1")
	if got := engine.DirtyCount(); got != 1 {
		t.Fatalf("DirtyCount() = %d, want 1", got)
	}

	engine.SetPersistEnabled(false)
	engine.MarkNodeStatic("hash-2")
	engine.MarkNodeDynamic("hash-2")

	if got := engine.DirtyCount(); got != 1 {
		t.Fatalf("DirtyCount() = %d after disabling, want 1 (no new keys)", got)
	}
}

// TestPersistenceBootstrap_EngineStartsWithPersistEnabled documents the default:
// embedders that never call SetPersistEnabled keep writing cache.db.
func TestPersistenceBootstrap_EngineStartsWithPersistEnabled(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	if !engine.PersistEnabled() {
		t.Fatal("fresh engine must start with persist enabled")
	}
	engine.MarkLease("plat-1", "acct-1")
	if got := engine.DirtyCount(); got != 1 {
		t.Fatalf("DirtyCount() = %d, want 1", got)
	}
}
