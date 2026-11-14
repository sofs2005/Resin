package metrics

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// defaultMetricsRetentionHours mirrors RESIN_METRICS_RETENTION_HOURS.
const defaultMetricsRetentionHours = 24

// globalSeriesKey is the map key used for rows whose platform_id is NULL in the
// SQLite schema (global scope). Platform ids are never empty strings.
const globalSeriesKey = ""

// memoryMetricsRepo keeps metric buckets in memory instead of metrics.db.
//
// Every write upserts into a per-table map keyed by bucket start (plus platform
// id for the dimensioned tables). Writes also evict buckets that fell out of the
// retention window, so memory usage is bounded by
// retention / bucketSeconds × dimension count.
//
// Query semantics mirror the SQLite implementation exactly: rows are filtered by
// the inclusive [from, to] bucket range and returned ordered by bucket start.
type memoryMetricsRepo struct {
	mu        sync.RWMutex
	retention time.Duration

	traffic  map[int64]TrafficBucketRow
	requests map[int64]map[string]RequestBucketRow
	probes   map[int64]ProbeBucketRow
	nodePool map[int64]NodePoolBucketRow
	latency  map[int64]map[string]string
	lease    map[int64]map[string]LeaseLifetimeBucketRow
}

// newMemoryMetricsRepo creates an empty in-memory bucket store.
// A non-positive retention falls back to 24h.
func newMemoryMetricsRepo(retention time.Duration) *memoryMetricsRepo {
	if retention <= 0 {
		retention = defaultMetricsRetentionHours * time.Hour
	}
	return &memoryMetricsRepo{
		retention: retention,
		traffic:   make(map[int64]TrafficBucketRow),
		requests:  make(map[int64]map[string]RequestBucketRow),
		probes:    make(map[int64]ProbeBucketRow),
		nodePool:  make(map[int64]NodePoolBucketRow),
		latency:   make(map[int64]map[string]string),
		lease:     make(map[int64]map[string]LeaseLifetimeBucketRow),
	}
}

// retentionWindow exposes the configured retention window.
func (m *memoryMetricsRepo) retentionWindow() time.Duration {
	if m == nil {
		return 0
	}
	return m.retention
}

// evictExpiredLocked drops buckets older than now-retention.
// The caller must hold the write lock.
func (m *memoryMetricsRepo) evictExpiredLocked(now time.Time) {
	cutoff := now.Add(-m.retention).Unix()
	for k := range m.traffic {
		if k < cutoff {
			delete(m.traffic, k)
		}
	}
	for k := range m.probes {
		if k < cutoff {
			delete(m.probes, k)
		}
	}
	for k := range m.nodePool {
		if k < cutoff {
			delete(m.nodePool, k)
		}
	}
	for k := range m.requests {
		if k < cutoff {
			delete(m.requests, k)
		}
	}
	for k := range m.latency {
		if k < cutoff {
			delete(m.latency, k)
		}
	}
	for k := range m.lease {
		if k < cutoff {
			delete(m.lease, k)
		}
	}
}

// writeBucket upserts one bucket flush data set.
func (m *memoryMetricsRepo) writeBucket(data *BucketFlushData) error {
	if data == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	bucket := data.BucketStartUnix

	m.traffic[bucket] = TrafficBucketRow{
		BucketStartUnix: bucket,
		IngressBytes:    data.Traffic.IngressBytes,
		EgressBytes:     data.Traffic.EgressBytes,
	}

	globalRequests, ok := data.Requests[globalSeriesKey]
	if !ok {
		globalRequests = requestAccum{}
	}

	perBucket, ok := m.requests[bucket]
	if !ok {
		perBucket = make(map[string]RequestBucketRow, len(data.Requests)+1)
		m.requests[bucket] = perBucket
	}
	perBucket[globalSeriesKey] = RequestBucketRow{
		BucketStartUnix: bucket,
		PlatformID:      "",
		TotalRequests:   globalRequests.Total,
		SuccessRequests: globalRequests.Success,
	}
	for pid, rq := range data.Requests {
		if pid == globalSeriesKey {
			continue
		}
		perBucket[pid] = RequestBucketRow{
			BucketStartUnix: bucket,
			PlatformID:      pid,
			TotalRequests:   rq.Total,
			SuccessRequests: rq.Success,
		}
	}

	m.probes[bucket] = ProbeBucketRow{
		BucketStartUnix: bucket,
		TotalCount:      data.Probes.Total,
	}

	for pid, acc := range data.LeaseLifetimes {
		if len(acc.Samples) == 0 {
			continue
		}
		p1, p5, p50 := computePercentiles(acc.Samples)
		perPlatform, ok := m.lease[bucket]
		if !ok {
			perPlatform = make(map[string]LeaseLifetimeBucketRow, len(data.LeaseLifetimes))
			m.lease[bucket] = perPlatform
		}
		perPlatform[pid] = LeaseLifetimeBucketRow{
			BucketStartUnix: bucket,
			PlatformID:      pid,
			SampleCount:     len(acc.Samples),
			P1Ms:            p1,
			P5Ms:            p5,
			P50Ms:           p50,
		}
	}

	m.evictExpiredLocked(time.Now())
	return nil
}

// writeNodePoolSnapshot upserts the node-pool snapshot of a bucket.
func (m *memoryMetricsRepo) writeNodePoolSnapshot(bucketStartUnix int64, totalNodes, healthyNodes, egressIPCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodePool[bucketStartUnix] = NodePoolBucketRow{
		BucketStartUnix: bucketStartUnix,
		TotalNodes:      totalNodes,
		HealthyNodes:    healthyNodes,
		EgressIPCount:   egressIPCount,
	}
	m.evictExpiredLocked(time.Now())
	return nil
}

// writeLatencyBucket upserts the access latency histogram of a bucket.
func (m *memoryMetricsRepo) writeLatencyBucket(bucketStartUnix int64, platformID string, buckets []int64) error {
	encoded, err := json.Marshal(buckets)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	perPlatform, ok := m.latency[bucketStartUnix]
	if !ok {
		perPlatform = make(map[string]string, 2)
		m.latency[bucketStartUnix] = perPlatform
	}
	perPlatform[platformID] = string(encoded)
	m.evictExpiredLocked(time.Now())
	return nil
}

// queryTraffic returns traffic buckets inside [from, to] ordered by bucket.
func (m *memoryMetricsRepo) queryTraffic(from, to int64) ([]TrafficBucketRow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]TrafficBucketRow, 0, len(m.traffic))
	for bucket, row := range m.traffic {
		if bucket < from || bucket > to {
			continue
		}
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].BucketStartUnix < result[j].BucketStartUnix })
	return result, nil
}

// queryRequests returns request buckets inside [from, to].
// An empty platformID selects the global (platform_id IS NULL) rows only,
// matching the SQLite query.
func (m *memoryMetricsRepo) queryRequests(from, to int64, platformID string) ([]RequestBucketRow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]RequestBucketRow, 0, len(m.requests))
	for bucket, perPlatform := range m.requests {
		if bucket < from || bucket > to {
			continue
		}
		row, ok := perPlatform[platformID]
		if !ok {
			continue
		}
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].BucketStartUnix < result[j].BucketStartUnix })
	return result, nil
}

// queryProbes returns probe buckets inside [from, to] ordered by bucket.
func (m *memoryMetricsRepo) queryProbes(from, to int64) ([]ProbeBucketRow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]ProbeBucketRow, 0, len(m.probes))
	for bucket, row := range m.probes {
		if bucket < from || bucket > to {
			continue
		}
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].BucketStartUnix < result[j].BucketStartUnix })
	return result, nil
}

// queryNodePool returns node-pool buckets inside [from, to] ordered by bucket.
func (m *memoryMetricsRepo) queryNodePool(from, to int64) ([]NodePoolBucketRow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]NodePoolBucketRow, 0, len(m.nodePool))
	for bucket, row := range m.nodePool {
		if bucket < from || bucket > to {
			continue
		}
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].BucketStartUnix < result[j].BucketStartUnix })
	return result, nil
}

// queryAccessLatency returns access latency histogram buckets inside [from, to].
// An empty platformID selects the global rows only, matching the SQLite query.
func (m *memoryMetricsRepo) queryAccessLatency(from, to int64, platformID string) ([]AccessLatencyBucketRow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]AccessLatencyBucketRow, 0, len(m.latency))
	for bucket, perPlatform := range m.latency {
		if bucket < from || bucket > to {
			continue
		}
		encoded, ok := perPlatform[platformID]
		if !ok {
			continue
		}
		result = append(result, AccessLatencyBucketRow{
			BucketStartUnix: bucket,
			PlatformID:      platformID,
			BucketsJSON:     encoded,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].BucketStartUnix < result[j].BucketStartUnix })
	return result, nil
}

// queryLeaseLifetime returns lease lifetime buckets inside [from, to].
// An empty platformID returns every platform, matching the SQLite query which
// only applies the platform filter when it is non-empty.
func (m *memoryMetricsRepo) queryLeaseLifetime(from, to int64, platformID string) ([]LeaseLifetimeBucketRow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]LeaseLifetimeBucketRow, 0, len(m.lease))
	for bucket, perPlatform := range m.lease {
		if bucket < from || bucket > to {
			continue
		}
		for pid, row := range perPlatform {
			if platformID != "" && pid != platformID {
				continue
			}
			result = append(result, row)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].BucketStartUnix != result[j].BucketStartUnix {
			return result[i].BucketStartUnix < result[j].BucketStartUnix
		}
		return result[i].PlatformID < result[j].PlatformID
	})
	return result, nil
}
