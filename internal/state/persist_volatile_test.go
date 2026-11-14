package state

import "testing"

// TestSetPersistVolatile_GatesVolatileTables locks in the second-phase contract:
// with cache persistence on, only the static topology set (nodes_static,
// subscription_nodes, leases) reaches the dirty sets. Probe results and latency
// samples (nodes_dynamic, node_latency) stay in memory because they are
// recomputed within one probe interval and otherwise dirty tens of thousands of
// rows every flush on a large pool.
func TestSetPersistVolatile_GatesVolatileTables(t *testing.T) {
	engine, _, _ := newTestEngine(t)

	// Library default: every table is persisted (historical behaviour).
	if !engine.PersistVolatile() {
		t.Fatal("PersistVolatile() = false, want true by default (newStateEngine)")
	}
	if !engine.PersistEnabled() {
		t.Fatal("PersistEnabled() = false, want true by default (newStateEngine)")
	}

	// The runtime turns the volatile tables off unless asked otherwise.
	engine.SetPersistVolatile(false)
	engine.MarkNodeDynamic("hash-a")
	engine.MarkNodeLatency("hash-a", "gstatic.com")
	if got := engine.DirtyCount(); got != 0 {
		t.Fatalf("DirtyCount() = %d, want 0 (volatile marks must be dropped)", got)
	}

	// The static set is unaffected.
	engine.MarkNodeStatic("hash-a")
	engine.MarkSubscriptionNode("sub-1", "hash-a")
	engine.MarkLease("plat-1", "acct")
	if got := engine.DirtyCount(); got != 3 {
		t.Fatalf("DirtyCount() = %d, want 3 (static nodes + subscription bindings + leases)", got)
	}

	// Opting in restores the pre-existing behaviour.
	engine.SetPersistVolatile(true)
	engine.MarkNodeDynamic("hash-a")
	engine.MarkNodeLatency("hash-a", "gstatic.com")
	if got := engine.DirtyCount(); got != 5 {
		t.Fatalf("DirtyCount() = %d, want 5 after enabling volatile persistence", got)
	}

	// Disabling persistence entirely still drops every new mark, volatile or not.
	engine.SetPersistEnabled(false)
	engine.MarkNodeDynamic("hash-b")
	engine.MarkNodeLatency("hash-b", "github.com")
	engine.MarkNodeStatic("hash-b")
	if got := engine.DirtyCount(); got != 5 {
		t.Fatalf("DirtyCount() = %d, want 5 with persistence disabled", got)
	}
}
