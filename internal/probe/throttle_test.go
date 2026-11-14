package probe

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

// newBackoffEntry builds a node entry with the given breaker state.
func newBackoffEntry(t *testing.T, failures int32, circuitOpen bool) *node.NodeEntry {
	t.Helper()
	raw := []byte(fmt.Sprintf(`{"type":"backoff","n":%d-%v}`, failures, circuitOpen))
	entry := node.NewNodeEntry(node.HashFromRawOptions(raw), raw, time.Now(), 4)
	entry.FailureCount.Store(failures)
	if circuitOpen {
		entry.CircuitOpenSince.Store(time.Now().UnixNano())
	}
	return entry
}

func TestProbeBackoffMultiplier(t *testing.T) {
	cases := []struct {
		name        string
		failures    int32
		circuitOpen bool
		maxFailures int
		wantMult    int
	}{
		{name: "no threshold configured", failures: 100, circuitOpen: true, maxFailures: 0, wantMult: 1},
		{name: "healthy node", failures: 0, circuitOpen: false, maxFailures: 3, wantMult: 1},
		{name: "new node starts open with no failures", failures: 0, circuitOpen: true, maxFailures: 3, wantMult: 1},
		{name: "just opened", failures: 3, circuitOpen: true, maxFailures: 3, wantMult: 2},
		{name: "one extra failure band", failures: 6, circuitOpen: true, maxFailures: 3, wantMult: 4},
		{name: "two extra failure bands", failures: 9, circuitOpen: true, maxFailures: 3, wantMult: 8},
		{name: "capped at 8x", failures: 1000, circuitOpen: true, maxFailures: 3, wantMult: 8},
		{name: "below threshold but open", failures: 1, circuitOpen: true, maxFailures: 3, wantMult: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := newBackoffEntry(t, tc.failures, tc.circuitOpen)
			if got := probeBackoffMultiplier(entry, tc.maxFailures); got != tc.wantMult {
				t.Fatalf("probeBackoffMultiplier = %d, want %d", got, tc.wantMult)
			}
		})
	}
}

func TestEffectiveProbeInterval_CappedAt24h(t *testing.T) {
	if got := effectiveProbeInterval(time.Hour, 8); got != 8*time.Hour {
		t.Fatalf("1h x8 = %v, want 8h", got)
	}
	// 6h egress interval x8 would be 48h; the cap keeps zombie nodes re-checked.
	if got := effectiveProbeInterval(6*time.Hour, 8); got != probeBackoffMaxInterval {
		t.Fatalf("6h x8 = %v, want %v", got, probeBackoffMaxInterval)
	}
	if got := effectiveProbeInterval(time.Hour, 0); got != time.Hour {
		t.Fatalf("multiplier < 1 should be treated as 1, got %v", got)
	}
	if got := effectiveProbeInterval(0, 8); got != 0 {
		t.Fatalf("zero interval should stay zero, got %v", got)
	}
}

// TestIsLatencyProbeDue_Backoff verifies that a circuit-open node is probed less
// often than a healthy node with the same attempt timestamp.
func TestIsLatencyProbeDue_Backoff(t *testing.T) {
	mgr := NewProbeManager(ProbeConfig{})
	now := time.Now()
	const baseInterval = time.Hour
	const authoritiesInterval = 3 * time.Hour
	authorities := []string{"gstatic.com"}

	elapsed := 90 * time.Minute

	healthy := newBackoffEntry(t, 0, false)
	healthy.LastLatencyProbeAttempt.Store(now.Add(-elapsed).UnixNano())
	healthy.LastAuthorityLatencyProbeAttempt.Store(now.Add(-elapsed).UnixNano())
	if !mgr.isLatencyProbeDue(healthy, now, baseInterval, authoritiesInterval, authorities, 15*time.Second, 3) {
		t.Fatal("healthy node must be due after one full interval")
	}

	// Same elapsed time, but the breaker keeps opening: 2x backoff means not due.
	open := newBackoffEntry(t, 3, true)
	open.LastLatencyProbeAttempt.Store(now.Add(-elapsed).UnixNano())
	open.LastAuthorityLatencyProbeAttempt.Store(now.Add(-elapsed).UnixNano())
	if mgr.isLatencyProbeDue(open, now, baseInterval, authoritiesInterval, authorities, 15*time.Second, 3) {
		t.Fatal("circuit-open node must not be due before interval x backoff")
	}

	// After the backed-off interval (1h x 2) it is due again.
	if !mgr.isLatencyProbeDue(open, now.Add(45*time.Minute), baseInterval, authoritiesInterval, authorities, 15*time.Second, 3) {
		t.Fatal("circuit-open node must be due after interval x backoff")
	}

	// With backoff disabled (threshold 0) the same node keeps the base interval.
	if !mgr.isLatencyProbeDue(open, now, baseInterval, authoritiesInterval, authorities, 15*time.Second, 0) {
		t.Fatal("backoff must be disabled when max_consecutive_failures is 0")
	}
}

func TestEgressProbeDue_Backoff(t *testing.T) {
	now := time.Now()
	const interval = 6 * time.Hour
	const lookahead = 15 * time.Second

	healthy := newBackoffEntry(t, 0, false)
	healthy.LastEgressUpdateAttempt.Store(now.Add(-7 * time.Hour).UnixNano())
	if !egressProbeDue(healthy, now, interval, lookahead, 3) {
		t.Fatal("healthy node must be due after the base interval")
	}

	// 8x backoff on a 6h interval is capped at 24h.
	zombie := newBackoffEntry(t, 9, true)
	zombie.LastEgressUpdateAttempt.Store(now.Add(-7 * time.Hour).UnixNano())
	if egressProbeDue(zombie, now, interval, lookahead, 3) {
		t.Fatal("zombie node must not be due 7h after the last attempt")
	}
	zombie.LastEgressUpdateAttempt.Store(now.Add(-25 * time.Hour).UnixNano())
	if !egressProbeDue(zombie, now, interval, lookahead, 3) {
		t.Fatal("zombie node must be due after the 24h cap")
	}

	// Never-probed nodes are always due.
	fresh := newBackoffEntry(t, 0, true)
	if !egressProbeDue(fresh, now, interval, lookahead, 3) {
		t.Fatal("never-probed node must be due immediately")
	}
}

func TestScanCursor_TakeWraps(t *testing.T) {
	var cursor scanCursor

	start, n := cursor.take(10, 4)
	if start != 0 || n != 4 {
		t.Fatalf("first window = (%d,%d), want (0,4)", start, n)
	}
	start, n = cursor.take(10, 4)
	if start != 4 || n != 4 {
		t.Fatalf("second window = (%d,%d), want (4,4)", start, n)
	}
	start, n = cursor.take(10, 4)
	if start != 8 || n != 4 {
		t.Fatalf("third window = (%d,%d), want (8,4)", start, n)
	}
	// The third window starts at 8 and wraps inside itself (8,9,0,1), so the
	// cursor advances to 2 — consecutive windows never overlap.
	start, n = cursor.take(10, 4)
	if start != 2 || n != 4 {
		t.Fatalf("wrapped window = (%d,%d), want (2,4)", start, n)
	}

	// Limit larger than the candidate space clamps to the whole space.
	start, n = cursor.take(3, 10)
	if start != 0 || n != 3 {
		t.Fatalf("clamped window = (%d,%d), want (0,3)", start, n)
	}

	if _, n := cursor.take(0, 4); n != 0 {
		t.Fatalf("empty candidate space must produce empty window, got %d", n)
	}
}

// TestScanLatency_BatchLimitedPerTick verifies that a scan tick enqueues at most
// the batch limit but still covers every due node across successive ticks.
func TestScanLatency_BatchLimitedPerTick(t *testing.T) {
	const nodeCount = 10
	const batch = 2

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("sub1", "sub1", "url", true, false)
	subMgr.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 4,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	managed := subscription.NewManagedNodes()
	for i := 0; i < nodeCount; i++ {
		raw := []byte(fmt.Sprintf(`{"type":"ss","n":"batch-%d"}`, i))
		h := node.HashFromRawOptions(raw)
		managed.StoreNode(h, subscription.ManagedNode{Tags: []string{"tag"}})
		pool.AddNodeFromSub(h, raw, "sub1")
		entry, ok := pool.GetEntry(h)
		if !ok {
			t.Fatalf("entry %d not found", i)
		}
		storeOutbound(entry)
	}
	// All nodes must stay in the subscription's managed set, otherwise
	// IsDisabledBySubscriptions skips them during the scan.
	sub.SwapManagedNodes(managed)

	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		ScanBatch:   batch,
	})
	defer mgr.Stop()

	// Every node is due (no attempt timestamps yet), so each tick enqueues one
	// cursor window of `batch` tasks.
	seen := map[node.Hash]int{}
	for tick := 0; tick < nodeCount/batch; tick++ {
		mgr.scanLatency()
		mgr.taskStates.Range(func(key probeTaskKey, _ *probeTaskState) bool {
			if key.kind != probeTaskKindLatency {
				return true
			}
			seen[key.hash]++
			return true
		})
		// Clear task state so the next tick can queue the same node again if the
		// cursor wraps; we only care about window coverage here.
		mgr.taskStates.Range(func(key probeTaskKey, _ *probeTaskState) bool {
			mgr.taskStates.Delete(key)
			return true
		})
	}

	if len(seen) != nodeCount {
		t.Fatalf("batch-limited scans covered %d distinct nodes, want %d", len(seen), nodeCount)
	}
	for h, times := range seen {
		if times != 1 {
			t.Fatalf("node %s scanned %d times in one full rotation, want 1", h.Hex(), times)
		}
	}
}

func TestScanBatchLimit(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		MaxLatencyTableEntries: 4,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	mgr := NewProbeManager(ProbeConfig{Pool: pool, Concurrency: 1000})
	// Empty pool: limit clamps down to the pool size.
	if got := mgr.scanBatchLimit(); got != 0 {
		t.Fatalf("empty pool scanBatchLimit = %d, want 0", got)
	}

	for i := 0; i < 5; i++ {
		raw := []byte(fmt.Sprintf(`{"type":"ss","n":"limit-%d"}`, i))
		pool.AddNodeFromSub(node.HashFromRawOptions(raw), raw, "sub1")
	}
	// max(64, concurrency) clamped to pool size.
	if got := mgr.scanBatchLimit(); got != 5 {
		t.Fatalf("scanBatchLimit = %d, want 5", got)
	}

	overridden := NewProbeManager(ProbeConfig{Pool: pool, Concurrency: 1000, ScanBatch: 2})
	if got := overridden.scanBatchLimit(); got != 2 {
		t.Fatalf("overridden scanBatchLimit = %d, want 2", got)
	}
}

// newTestLimiter builds a limiter with an injected clock and sink.
func newTestLimiter(mode string, clock *time.Time) (*failureLogLimiter, *[]string) {
	lines := make([]string, 0, 16)
	limiter := newFailureLogLimiter(mode)
	limiter.now = func() time.Time { return *clock }
	limiter.windowEnd = clock.Add(limiter.window)
	limiter.emit = func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}
	return limiter, &lines
}

func TestFailureLogLimiter_LimitedMode(t *testing.T) {
	now := time.Now()
	limiter, lines := newTestLimiter(FailureLogModeLimited, &now)

	// First window: exactly maxPerWindow lines get through.
	for i := 0; i < failureLogMaxLines+5; i++ {
		limiter.printf("[probe] latency probe failed for node-%d: %v", i, "timeout")
	}
	if got := len(*lines); got != failureLogMaxLines {
		t.Fatalf("emitted %d lines in the first window, want %d", got, failureLogMaxLines)
	}
	if got := limiter.SuppressedCount(); got != 5 {
		t.Fatalf("suppressed = %d, want 5", got)
	}

	// Next window resumes and reports the suppressed count on the first line.
	now = now.Add(2 * failureLogWindow)
	limiter.printf("[probe] latency probe failed for node-x: %v", "timeout")
	if got := len(*lines); got != failureLogMaxLines+1 {
		t.Fatalf("emitted %d lines after window rollover, want %d", got, failureLogMaxLines+1)
	}
	last := (*lines)[len(*lines)-1]
	if !strings.Contains(last, "(suppressed 5)") {
		t.Fatalf("rollover line %q should report suppressed count", last)
	}
	if got := limiter.SuppressedCount(); got != 0 {
		t.Fatalf("suppressed counter should reset after reporting, got %d", got)
	}
}

func TestFailureLogLimiter_OffMode(t *testing.T) {
	now := time.Now()
	limiter, lines := newTestLimiter(FailureLogModeOff, &now)
	for i := 0; i < 20; i++ {
		limiter.printf("[probe] egress probe failed for node-%d: %v", i, "refused")
	}
	if got := len(*lines); got != 0 {
		t.Fatalf("off mode emitted %d lines, want 0", got)
	}
}

func TestFailureLogLimiter_AllMode(t *testing.T) {
	now := time.Now()
	limiter, lines := newTestLimiter(FailureLogModeAll, &now)
	const total = 50
	for i := 0; i < total; i++ {
		limiter.printf("[probe] egress probe failed for node-%d: %v", i, "refused")
	}
	if got := len(*lines); got != total {
		t.Fatalf("all mode emitted %d lines, want %d", got, total)
	}
}

func TestParseFailureLogMode(t *testing.T) {
	cases := map[string]failureLogMode{
		"off":      failureLogModeOff,
		"OFF":      failureLogModeOff,
		" all ":    failureLogModeAll,
		"limited":  failureLogModeLimited,
		"":         failureLogModeLimited,
		"nonsense": failureLogModeLimited,
	}
	for raw, want := range cases {
		if got := parseFailureLogMode(raw); got != want {
			t.Fatalf("parseFailureLogMode(%q) = %v, want %v", raw, got, want)
		}
	}
}
