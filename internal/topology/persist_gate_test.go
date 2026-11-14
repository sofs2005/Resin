package topology

import (
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
)

// persistGateCounters counts persistence callbacks fired by the pool.
type persistGateCounters struct {
	dynamic atomic.Int32
	latency atomic.Int32
}

// newPersistGatePool builds a pool whose persistence callbacks are counted.
func newPersistGatePool(
	minSuccesses int,
	maxFailures int,
	counters *persistGateCounters,
) (*GlobalNodePool, *SubscriptionManager) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "PersistGate", "url", true, false)
	subMgr.Register(sub)

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:               subMgr.Lookup,
		GeoLookup:               func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries:  16,
		MaxConsecutiveFailures:  func() int { return maxFailures },
		MinConsecutiveSuccesses: func() int { return minSuccesses },
		OnNodeDynamicChanged:    func(node.Hash) { counters.dynamic.Add(1) },
		OnNodeLatencyChanged:    func(node.Hash, string) { counters.latency.Add(1) },
	})
	return pool, subMgr
}

func addPersistGateNode(t *testing.T, pool *GlobalNodePool, subMgr *SubscriptionManager, raw string) node.Hash {
	t.Helper()
	sub := subMgr.Lookup("s1")
	if sub == nil {
		t.Fatal("subscription s1 is not registered")
	}
	mn := subscription.NewManagedNodes()
	h := node.HashFromRawOptions([]byte(raw))
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	sub.SwapManagedNodes(mn)
	pool.AddNodeFromSub(h, []byte(raw), "s1")
	return h
}

// TestRecordResult_PersistsOnlyOnStateCrossings locks in the write-reduction
// contract: nodes_dynamic is only marked dirty when a decision-relevant field
// crosses a boundary. Counters that merely increment stay in memory.
func TestRecordResult_PersistsOnlyOnStateCrossings(t *testing.T) {
	counters := &persistGateCounters{}
	const minSuccesses, maxFailures = 3, 3
	pool, subMgr := newPersistGatePool(minSuccesses, maxFailures, counters)
	h := addPersistGateNode(t, pool, subMgr, `{"type":"ss","n":"crossings"}`)

	// New nodes start circuit-open, so the first success is a double crossing:
	// circuit re-closed and the success streak restarted at 1.
	pool.RecordResult(h, true)
	if got := counters.dynamic.Load(); got != 1 {
		t.Fatalf("after first success: dynamic callbacks = %d, want 1", got)
	}

	// Streak 1 -> 2 is not a crossing.
	pool.RecordResult(h, true)
	if got := counters.dynamic.Load(); got != 1 {
		t.Fatalf("after second success: dynamic callbacks = %d, want 1 (no crossing)", got)
	}

	// Streak 2 -> 3 reaches min_consecutive_successes: crossing into healthy.
	pool.RecordResult(h, true)
	if got := counters.dynamic.Load(); got != 2 {
		t.Fatalf("after healthy threshold: dynamic callbacks = %d, want 2", got)
	}

	// Beyond the threshold nothing changes.
	pool.RecordResult(h, true)
	if got := counters.dynamic.Load(); got != 2 {
		t.Fatalf("after healthy streak growth: dynamic callbacks = %d, want 2 (no crossing)", got)
	}

	// failure_count 0 -> 1 is a crossing.
	pool.RecordResult(h, false)
	if got := counters.dynamic.Load(); got != 3 {
		t.Fatalf("after first failure: dynamic callbacks = %d, want 3", got)
	}

	// failure_count 1 -> 2 is not.
	pool.RecordResult(h, false)
	if got := counters.dynamic.Load(); got != 3 {
		t.Fatalf("after second failure: dynamic callbacks = %d, want 3 (no crossing)", got)
	}

	// Reaching max_consecutive_failures opens the circuit: crossing.
	pool.RecordResult(h, false)
	if got := counters.dynamic.Load(); got != 4 {
		t.Fatalf("after circuit open: dynamic callbacks = %d, want 4", got)
	}

	// Further failures while open are not crossings.
	pool.RecordResult(h, false)
	if got := counters.dynamic.Load(); got != 4 {
		t.Fatalf("after repeated failure: dynamic callbacks = %d, want 4 (no crossing)", got)
	}

	// Recovery resets failure_count >=1 -> 0 and re-closes the circuit: crossing.
	pool.RecordResult(h, true)
	if got := counters.dynamic.Load(); got != 5 {
		t.Fatalf("after recovery: dynamic callbacks = %d, want 5", got)
	}

	// failure_count is still tracked accurately in memory.
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	if got := entry.FailureCount.Load(); got != 0 {
		t.Fatalf("FailureCount = %d, want 0 after recovery", got)
	}
	if entry.IsCircuitOpen() {
		t.Fatal("circuit should be closed after recovery")
	}
}

// TestRecordResult_PersistsImmediatelyWhenThresholdIsOne covers the degenerate
// configuration max_consecutive_failures=1 / min_consecutive_successes=1, where
// every result is a crossing and persistence must therefore stay per-result.
func TestRecordResult_PersistsImmediatelyWhenThresholdIsOne(t *testing.T) {
	counters := &persistGateCounters{}
	pool, subMgr := newPersistGatePool(1, 1, counters)
	h := addPersistGateNode(t, pool, subMgr, `{"type":"ss","n":"threshold-one"}`)

	pool.RecordResult(h, true) // circuit closed + streak 1 == minSuccesses
	first := counters.dynamic.Load()
	if first == 0 {
		t.Fatal("expected a persistence callback for the first success")
	}

	pool.RecordResult(h, false) // failure 0->1 == maxFailures, circuit opens
	if got := counters.dynamic.Load(); got != first+1 {
		t.Fatalf("open-circuit failure should persist: callbacks = %d, want %d", got, first+1)
	}

	entry, _ := pool.GetEntry(h)
	if !entry.IsCircuitOpen() {
		t.Fatal("circuit should be open after hitting max_consecutive_failures")
	}
}

// TestRecordLatency_PersistsOnlyMaterialChanges verifies the 25% EWMA change
// threshold used to gate node_latency writes.
func TestRecordLatency_PersistsOnlyMaterialChanges(t *testing.T) {
	const domain = "example.com"
	counters := &persistGateCounters{}
	pool, subMgr := newPersistGatePool(3, 3, counters)
	h := addPersistGateNode(t, pool, subMgr, `{"type":"ss","n":"latency-gate"}`)
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}

	// First sample for a domain always persists (nothing stored yet).
	first := 100 * time.Millisecond
	pool.RecordLatency(h, domain, &first)
	if got := counters.latency.Load(); got != 1 {
		t.Fatalf("first sample: latency callbacks = %d, want 1", got)
	}

	// Seed a stale sample so the next TD-EWMA weight collapses to zero and the
	// new sample fully replaces the old EWMA. This makes the drift exact.
	seed := func(ewma time.Duration) {
		entry.LatencyTable.LoadEntry(domain, node.DomainLatencyStats{
			Ewma:        ewma,
			LastUpdated: time.Now().Add(-time.Hour),
		})
	}

	// +20% drift stays in memory.
	seed(100 * time.Millisecond)
	small := 120 * time.Millisecond
	pool.RecordLatency(h, domain, &small)
	if got := counters.latency.Load(); got != 1 {
		t.Fatalf("+20%% drift: latency callbacks = %d, want 1 (below threshold)", got)
	}

	// A sample that doubles the EWMA is material.
	seed(100 * time.Millisecond)
	large := 200 * time.Millisecond
	pool.RecordLatency(h, domain, &large)
	if got := counters.latency.Load(); got != 2 {
		t.Fatalf("+100%% drift: latency callbacks = %d, want 2", got)
	}

	// A nil sample (probe failure) leaves the table untouched and persists nothing.
	pool.RecordLatency(h, domain, nil)
	if got := counters.latency.Load(); got != 2 {
		t.Fatalf("nil sample: latency callbacks = %d, want 2 (nothing changed)", got)
	}

	// The in-memory EWMA did follow the material sample.
	stats, ok := entry.LatencyTable.PeekDomainStats(domain)
	if !ok {
		t.Fatal("latency sample missing")
	}
	if stats.Ewma < 190*time.Millisecond {
		t.Fatalf("in-memory EWMA = %v, want ~200ms", stats.Ewma)
	}
}

// TestRecordLatency_ThresholdBoundary documents the inclusive/exclusive edges of
// latencyChangeExceeded.
func TestRecordLatency_ThresholdBoundary(t *testing.T) {
	cases := []struct {
		name    string
		prev    time.Duration
		current time.Duration
		want    bool
	}{
		{"no change", 100 * time.Millisecond, 100 * time.Millisecond, false},
		{"exactly 25 percent", 100 * time.Millisecond, 125 * time.Millisecond, false},
		{"just above 25 percent", 100 * time.Millisecond, 126 * time.Millisecond, true},
		{"drop of 25 percent", 100 * time.Millisecond, 75 * time.Millisecond, false},
		{"drop beyond 25 percent", 100 * time.Millisecond, 70 * time.Millisecond, true},
		{"zero previous", 0, time.Second, true},
		{"zero previous zero current", 0, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := latencyChangeExceeded(tc.prev, tc.current); got != tc.want {
				t.Fatalf("latencyChangeExceeded(%v, %v) = %v, want %v", tc.prev, tc.current, got, tc.want)
			}
		})
	}
}

// TestUpdateNodeEgressIP_AttemptsDoNotPersist verifies that repeated egress
// probes for an unchanged node no longer mark nodes_dynamic dirty.
func TestUpdateNodeEgressIP_AttemptsDoNotPersist(t *testing.T) {
	counters := &persistGateCounters{}
	pool, subMgr := newPersistGatePool(3, 3, counters)
	h := addPersistGateNode(t, pool, subMgr, `{"type":"ss","n":"egress-gate"}`)

	ip := netip.MustParseAddr("1.2.3.4")
	pool.UpdateNodeEgressIP(h, &ip, nil) // IP appeared: crossing
	if got := counters.dynamic.Load(); got != 1 {
		t.Fatalf("first egress sample: dynamic callbacks = %d, want 1", got)
	}

	for i := 0; i < 5; i++ {
		pool.UpdateNodeEgressIP(h, &ip, nil) // same IP, still ready
	}
	if got := counters.dynamic.Load(); got != 1 {
		t.Fatalf("repeat same-IP probes: dynamic callbacks = %d, want 1", got)
	}

	// Losing readiness is a crossing.
	pool.UpdateNodeEgressIP(h, nil, nil)
	if got := counters.dynamic.Load(); got != 2 {
		t.Fatalf("readiness loss: dynamic callbacks = %d, want 2", got)
	}

	// Staying not-ready is not.
	pool.UpdateNodeEgressIP(h, nil, nil)
	if got := counters.dynamic.Load(); got != 2 {
		t.Fatalf("repeat failure probes: dynamic callbacks = %d, want 2", got)
	}
}
