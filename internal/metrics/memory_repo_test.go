package metrics

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestMemoryMetricsRepo_WriteAndQuery mirrors TestMetricsRepo_WriteAndQuery and
// locks in that the memory backend returns exactly the same shapes/values.
func TestMemoryMetricsRepo_WriteAndQuery(t *testing.T) {
	repo := NewMemoryMetricsRepo(24 * time.Hour)
	t.Cleanup(func() { _ = repo.Close() })

	if !repo.IsMemory() {
		t.Fatal("repo should report memory mode")
	}
	if repo.Retention() != 24*time.Hour {
		t.Fatalf("Retention() = %v, want 24h", repo.Retention())
	}

	bucketStart := time.Now().Add(-time.Minute).Unix()
	if err := repo.WriteBucket(&BucketFlushData{
		BucketStartUnix: bucketStart,
		Traffic:         trafficAccum{IngressBytes: 100, EgressBytes: 200},
		Requests: map[string]requestAccum{
			"":       {Total: 5, Success: 4},
			"plat-1": {Total: 3, Success: 2},
		},
		Probes: probeAccum{Total: 7},
		LeaseLifetimes: map[string]*leaseLifeAccum{
			"plat-1": {Samples: []int64{int64(time.Second), int64(2 * time.Second), int64(3 * time.Second)}},
		},
	}); err != nil {
		t.Fatalf("WriteBucket: %v", err)
	}
	if err := repo.WriteNodePoolSnapshot(bucketStart, 20, 12, 6); err != nil {
		t.Fatalf("WriteNodePoolSnapshot: %v", err)
	}
	if err := repo.WriteLatencyBucket(bucketStart, "", []int64{1, 2, 3}); err != nil {
		t.Fatalf("WriteLatencyBucket global: %v", err)
	}
	if err := repo.WriteLatencyBucket(bucketStart, "plat-1", []int64{4, 5, 6}); err != nil {
		t.Fatalf("WriteLatencyBucket platform: %v", err)
	}

	from, to := bucketStart-10, bucketStart+10

	traffic, err := repo.QueryTraffic(from, to)
	if err != nil {
		t.Fatalf("QueryTraffic: %v", err)
	}
	if len(traffic) != 1 || traffic[0].IngressBytes != 100 || traffic[0].EgressBytes != 200 {
		t.Fatalf("unexpected traffic rows: %+v", traffic)
	}

	// Empty platformID means global scope only (platform_id IS NULL in SQL).
	globalRequests, err := repo.QueryRequests(from, to, "")
	if err != nil {
		t.Fatalf("QueryRequests(global): %v", err)
	}
	if len(globalRequests) != 1 || globalRequests[0].TotalRequests != 5 || globalRequests[0].SuccessRequests != 4 {
		t.Fatalf("unexpected global request rows: %+v", globalRequests)
	}
	if globalRequests[0].PlatformID != "" {
		t.Fatalf("global row platform id = %q, want empty", globalRequests[0].PlatformID)
	}

	platformRequests, err := repo.QueryRequests(from, to, "plat-1")
	if err != nil {
		t.Fatalf("QueryRequests(plat-1): %v", err)
	}
	if len(platformRequests) != 1 || platformRequests[0].TotalRequests != 3 || platformRequests[0].SuccessRequests != 2 {
		t.Fatalf("unexpected request rows: %+v", platformRequests)
	}

	probes, err := repo.QueryProbes(from, to)
	if err != nil {
		t.Fatalf("QueryProbes: %v", err)
	}
	if len(probes) != 1 || probes[0].TotalCount != 7 {
		t.Fatalf("unexpected probe rows: %+v", probes)
	}

	nodePool, err := repo.QueryNodePool(from, to)
	if err != nil {
		t.Fatalf("QueryNodePool: %v", err)
	}
	if len(nodePool) != 1 || nodePool[0].TotalNodes != 20 || nodePool[0].HealthyNodes != 12 ||
		nodePool[0].EgressIPCount != 6 {
		t.Fatalf("unexpected node-pool rows: %+v", nodePool)
	}

	globalLatency, err := repo.QueryAccessLatency(from, to, "")
	if err != nil {
		t.Fatalf("QueryAccessLatency(global): %v", err)
	}
	if len(globalLatency) != 1 || globalLatency[0].BucketsJSON != "[1,2,3]" {
		t.Fatalf("unexpected global latency rows: %+v", globalLatency)
	}

	platformLatency, err := repo.QueryAccessLatency(from, to, "plat-1")
	if err != nil {
		t.Fatalf("QueryAccessLatency(plat-1): %v", err)
	}
	if len(platformLatency) != 1 || platformLatency[0].BucketsJSON != "[4,5,6]" {
		t.Fatalf("unexpected platform latency rows: %+v", platformLatency)
	}

	leaseLife, err := repo.QueryLeaseLifetime(from, to, "plat-1")
	if err != nil {
		t.Fatalf("QueryLeaseLifetime: %v", err)
	}
	if len(leaseLife) != 1 {
		t.Fatalf("unexpected lease lifetime rows: %+v", leaseLife)
	}
	if leaseLife[0].SampleCount != 3 || leaseLife[0].P1Ms != 1000 || leaseLife[0].P5Ms != 1000 || leaseLife[0].P50Ms != 2000 {
		t.Fatalf("unexpected lease lifetime values: %+v", leaseLife[0])
	}

	// An empty platformID returns every platform for lease lifetimes.
	allLeaseLife, err := repo.QueryLeaseLifetime(from, to, "")
	if err != nil {
		t.Fatalf("QueryLeaseLifetime(all): %v", err)
	}
	if len(allLeaseLife) != 1 || allLeaseLife[0].PlatformID != "plat-1" {
		t.Fatalf("unexpected all-platform lease lifetime rows: %+v", allLeaseLife)
	}

	// Out-of-range query returns no rows.
	if rows, err := repo.QueryTraffic(to+1, to+10); err != nil || len(rows) != 0 {
		t.Fatalf("QueryTraffic(out of range) = (%+v, %v), want empty", rows, err)
	}
}

// TestMemoryMetricsRepo_UpsertOverwrites verifies repeated writes for the same
// bucket replace the previous values, like the SQLite ON CONFLICT DO UPDATE.
func TestMemoryMetricsRepo_UpsertOverwrites(t *testing.T) {
	repo := NewMemoryMetricsRepo(time.Hour)
	defer repo.Close()

	bucket := time.Now().Unix()
	if err := repo.WriteBucket(&BucketFlushData{
		BucketStartUnix: bucket,
		Traffic:         trafficAccum{IngressBytes: 1, EgressBytes: 2},
		Probes:          probeAccum{Total: 1},
	}); err != nil {
		t.Fatalf("WriteBucket(1): %v", err)
	}
	if err := repo.WriteBucket(&BucketFlushData{
		BucketStartUnix: bucket,
		Traffic:         trafficAccum{IngressBytes: 10, EgressBytes: 20},
		Probes:          probeAccum{Total: 9},
	}); err != nil {
		t.Fatalf("WriteBucket(2): %v", err)
	}
	if err := repo.WriteNodePoolSnapshot(bucket, 1, 1, 1); err != nil {
		t.Fatalf("WriteNodePoolSnapshot(1): %v", err)
	}
	if err := repo.WriteNodePoolSnapshot(bucket, 5, 4, 3); err != nil {
		t.Fatalf("WriteNodePoolSnapshot(2): %v", err)
	}

	traffic, err := repo.QueryTraffic(bucket, bucket)
	if err != nil {
		t.Fatalf("QueryTraffic: %v", err)
	}
	if len(traffic) != 1 || traffic[0].IngressBytes != 10 || traffic[0].EgressBytes != 20 {
		t.Fatalf("traffic not overwritten: %+v", traffic)
	}

	probes, err := repo.QueryProbes(bucket, bucket)
	if err != nil {
		t.Fatalf("QueryProbes: %v", err)
	}
	if len(probes) != 1 || probes[0].TotalCount != 9 {
		t.Fatalf("probes not overwritten: %+v", probes)
	}

	nodePool, err := repo.QueryNodePool(bucket, bucket)
	if err != nil {
		t.Fatalf("QueryNodePool: %v", err)
	}
	if len(nodePool) != 1 || nodePool[0].TotalNodes != 5 || nodePool[0].HealthyNodes != 4 ||
		nodePool[0].EgressIPCount != 3 {
		t.Fatalf("node pool snapshot not overwritten: %+v", nodePool)
	}
}

// TestMemoryMetricsRepo_EvictsExpiredBuckets verifies that buckets older than
// the retention window are dropped on write while fresh buckets survive.
func TestMemoryMetricsRepo_EvictsExpiredBuckets(t *testing.T) {
	repo := NewMemoryMetricsRepo(time.Hour)
	defer repo.Close()

	now := time.Now().Unix()
	expired := now - int64(3*time.Hour/time.Second)
	fresh := now - 60

	if err := repo.WriteBucket(&BucketFlushData{
		BucketStartUnix: expired,
		Traffic:         trafficAccum{IngressBytes: 1, EgressBytes: 1},
	}); err != nil {
		t.Fatalf("WriteBucket(expired): %v", err)
	}
	if err := repo.WriteNodePoolSnapshot(expired, 1, 1, 1); err != nil {
		t.Fatalf("WriteNodePoolSnapshot(expired): %v", err)
	}
	if err := repo.WriteLatencyBucket(expired, "plat-1", []int64{1}); err != nil {
		t.Fatalf("WriteLatencyBucket(expired): %v", err)
	}
	if err := repo.WriteBucket(&BucketFlushData{
		BucketStartUnix: fresh,
		Traffic:         trafficAccum{IngressBytes: 2, EgressBytes: 2},
		LeaseLifetimes: map[string]*leaseLifeAccum{
			"plat-1": {Samples: []int64{int64(time.Second)}},
		},
	}); err != nil {
		t.Fatalf("WriteBucket(fresh): %v", err)
	}

	from, to := expired-10, fresh+10

	traffic, err := repo.QueryTraffic(from, to)
	if err != nil {
		t.Fatalf("QueryTraffic: %v", err)
	}
	if len(traffic) != 1 || traffic[0].BucketStartUnix != fresh {
		t.Fatalf("expired traffic bucket not evicted: %+v", traffic)
	}

	if rows, err := repo.QueryNodePool(from, to); err != nil || len(rows) != 0 {
		t.Fatalf("expired node-pool bucket not evicted: %+v (%v)", rows, err)
	}
	if rows, err := repo.QueryAccessLatency(from, to, "plat-1"); err != nil || len(rows) != 0 {
		t.Fatalf("expired latency bucket not evicted: %+v (%v)", rows, err)
	}

	leaseLife, err := repo.QueryLeaseLifetime(from, to, "plat-1")
	if err != nil {
		t.Fatalf("QueryLeaseLifetime: %v", err)
	}
	if len(leaseLife) != 1 || leaseLife[0].BucketStartUnix != fresh {
		t.Fatalf("unexpected lease lifetime rows: %+v", leaseLife)
	}

	// Expired global request/probe rows written earlier must be gone too.
	if rows, err := repo.QueryRequests(from, to, ""); err != nil || len(rows) != 1 || rows[0].BucketStartUnix != fresh {
		t.Fatalf("expired request bucket not evicted: %+v (%v)", rows, err)
	}
}

// TestMemoryMetricsRepo_QueriesOrderedByBucket verifies ascending bucket order
// for out-of-order writes.
func TestMemoryMetricsRepo_QueriesOrderedByBucket(t *testing.T) {
	repo := NewMemoryMetricsRepo(time.Hour)
	defer repo.Close()

	now := time.Now().Unix()
	for _, bucket := range []int64{now, now - 120, now - 60} {
		if err := repo.WriteNodePoolSnapshot(bucket, 1, 1, 1); err != nil {
			t.Fatalf("WriteNodePoolSnapshot: %v", err)
		}
		if err := repo.WriteBucket(&BucketFlushData{
			BucketStartUnix: bucket,
			Traffic:         trafficAccum{IngressBytes: 1, EgressBytes: 1},
		}); err != nil {
			t.Fatalf("WriteBucket: %v", err)
		}
	}

	rows, err := repo.QueryNodePool(now-120, now)
	if err != nil {
		t.Fatalf("QueryNodePool: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i-1].BucketStartUnix >= rows[i].BucketStartUnix {
			t.Fatalf("rows not ordered ascending: %+v", rows)
		}
	}

	traffic, err := repo.QueryTraffic(now-120, now)
	if err != nil {
		t.Fatalf("QueryTraffic: %v", err)
	}
	if len(traffic) != 3 {
		t.Fatalf("traffic rows = %d, want 3", len(traffic))
	}
	for i := 1; i < len(traffic); i++ {
		if traffic[i-1].BucketStartUnix >= traffic[i].BucketStartUnix {
			t.Fatalf("traffic rows not ordered ascending: %+v", traffic)
		}
	}
}

// TestMemoryMetricsRepo_CreatesNoFiles verifies the memory backend never touches
// the filesystem it is not given a path for, and that maintenance is a no-op.
func TestMemoryMetricsRepo_CreatesNoFiles(t *testing.T) {
	root := t.TempDir()
	repo := NewMemoryMetricsRepo(time.Hour)

	if err := repo.WriteLatencyBucket(time.Now().Unix(), "", []int64{1, 2}); err != nil {
		t.Fatalf("WriteLatencyBucket: %v", err)
	}
	if err := repo.TruncateWAL(); err != nil {
		t.Fatalf("TruncateWAL: %v", err)
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close is idempotent for the memory backend.
	if err := repo.Close(); err != nil {
		t.Fatalf("Close(2): %v", err)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("memory repo must not create files, found %d under %s", len(entries), filepath.Clean(root))
	}
}

// TestMemoryMetricsRepo_DefaultRetentionWhenNonPositive pins the 24h fallback.
func TestMemoryMetricsRepo_DefaultRetentionWhenNonPositive(t *testing.T) {
	repo := NewMemoryMetricsRepo(0)
	defer repo.Close()
	if got := repo.Retention(); got != defaultMetricsRetentionHours*time.Hour {
		t.Fatalf("Retention() = %v, want %v", got, defaultMetricsRetentionHours*time.Hour)
	}
}
