package probe

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/scanloop"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/puzpuzpuz/xsync/v4"
)

// Fetcher executes an HTTP request through the given node, returning
// response body and TLS handshake latency. This is injectable for testing.
type Fetcher func(hash node.Hash, url string) (body []byte, latency time.Duration, err error)

// ContextFetcher is the context-aware form used by production probes so one
// logical probe can share a total deadline across fallback URLs.
type ContextFetcher func(ctx context.Context, hash node.Hash, url string) (body []byte, latency time.Duration, err error)

// ProbeConfig configures the ProbeManager.
// Field names align 1:1 with RuntimeConfig to prevent mis-wiring.
type ProbeConfig struct {
	Pool        *topology.GlobalNodePool
	Concurrency int // number of async probe workers
	// QueueCapacity is the per-priority async queue capacity.
	// If <= 0, defaults to max(1024, Concurrency*4).
	QueueCapacity int

	// Fetcher executes HTTP via node hash. Injectable for testing.
	Fetcher Fetcher
	// FetcherWithContext is preferred in production; it shares one deadline
	// across all URLs in a logical egress probe.
	FetcherWithContext ContextFetcher

	// EgressTraceURLs returns the ordered egress probe URL list. The first
	// successful Cloudflare-trace response wins.
	EgressTraceURLs func() []string
	// ProbeTimeout supplies the total timeout for one logical probe.
	ProbeTimeout func() time.Duration

	// Interval thresholds — closures for hot-reload from RuntimeConfig.
	MaxEgressTestInterval           func() time.Duration
	MaxLatencyTestInterval          func() time.Duration
	MaxAuthorityLatencyTestInterval func() time.Duration

	LatencyTestURL     func() string
	LatencyAuthorities func() []string

	// MaxConsecutiveFailures supplies the live circuit-breaker threshold.
	// Used to derive the probe backoff multiplier for circuit-open nodes.
	// Nil (or a non-positive result) disables backoff.
	MaxConsecutiveFailures func() int

	// FailureLogMode selects how probe failure logs are emitted:
	// "off", "limited" (default), or "all". See failureLogLimiter.
	FailureLogMode string

	// ScanBatch caps how many tasks one scan tick may enqueue.
	// <= 0 derives the limit from the pool size and probe concurrency.
	ScanBatch int

	// OnProbeEvent is called after each probe attempt completes (egress or latency).
	// The kind parameter is "egress" or "latency".
	OnProbeEvent func(kind string)

	// ChooseNormalWhenBoth chooses whether to pop normal-priority queue when
	// both high and normal queues are non-empty.
	// Nil defaults to 10% chance.
	ChooseNormalWhenBoth func() bool
}

// ProbeManager schedules and executes active probes against nodes in the pool.
// It holds a direct reference to *topology.GlobalNodePool (no interface).
type ProbeManager struct {
	pool               *topology.GlobalNodePool
	stopCh             chan struct{}
	stopOnce           sync.Once
	wg                 sync.WaitGroup
	fetcher            Fetcher
	fetcherWithContext ContextFetcher
	workerCount        int
	taskQueue          *probeTaskQueue
	taskStates         *xsync.Map[probeTaskKey, *probeTaskState]

	maxEgressTestInterval           func() time.Duration
	maxLatencyTestInterval          func() time.Duration
	maxAuthorityLatencyTestInterval func() time.Duration
	latencyTestURL                  func() string
	latencyAuthorities              func() []string
	egressTraceURLs                 func() []string
	probeTimeout                    func() time.Duration
	onProbeEvent                    func(kind string)
	maxConsecutiveFailures          func() int
	scanBatch                       int
	failureLog                      *failureLogLimiter

	// egressCursor and latencyCursor rotate the per-tick enqueue window so a
	// batch-limited scan still covers the whole pool over successive ticks.
	egressCursor  scanCursor
	latencyCursor scanCursor
}

// probeBackoffMaxShift caps the speculative probe backoff at 2^3 == 8x.
const probeBackoffMaxShift = 3

// probeBackoffMaxInterval caps the effective probe interval after backoff.
const probeBackoffMaxInterval = 24 * time.Hour

// scanCursor implements the round-robin window used by batch-limited scans.
type scanCursor struct {
	mu   sync.Mutex
	next int
}

// take returns the window [start, start+n) over a space of total candidates and
// advances the cursor past that window (wrapping). n is clamped to total.
func (c *scanCursor) take(total, limit int) (start, n int) {
	if total <= 0 || limit <= 0 {
		return 0, 0
	}
	n = limit
	if n > total {
		n = total
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.next < 0 || c.next >= total {
		c.next = 0
	}
	start = c.next
	c.next = (start + n) % total
	return start, n
}

const (
	egressTraceURL        = "https://cloudflare.com/cdn-cgi/trace"
	egressBackupTraceURL  = "https://www.cloudflare.com/cdn-cgi/trace"
	egressTraceDomain     = "cloudflare.com"
	defaultLatencyTestURL = "https://www.gstatic.com/generate_204"
	defaultProbeTimeout   = 15 * time.Second
	defaultQueueCap       = 1024
	// minScanBatch is the floor for the per-tick scan batch, so small pools are
	// still drained in a single tick.
	minScanBatch = 64
)

var defaultEgressTraceURLs = []string{egressTraceURL, egressBackupTraceURL}

type probePriority uint8

const (
	probePriorityNormal probePriority = iota
	probePriorityHigh
)

type probeTaskKind uint8

const (
	probeTaskKindEgress probeTaskKind = iota
	probeTaskKindLatency
)

type probeTaskKey struct {
	hash node.Hash
	kind probeTaskKind
}

type probeTask struct {
	key probeTaskKey
}

type probeTaskState struct {
	flags atomic.Uint32
}

const (
	taskFlagQueued uint32 = 1 << iota
	taskFlagRunning
	taskFlagDirty
	taskFlagDirtyHigh
	taskFlagQueuedHigh
)

type probeTaskBuffer struct {
	items []probeTask
	head  int
}

func (b *probeTaskBuffer) len() int {
	return len(b.items) - b.head
}

func (b *probeTaskBuffer) push(task probeTask) {
	b.items = append(b.items, task)
}

func (b *probeTaskBuffer) pop() probeTask {
	task := b.items[b.head]
	b.head++
	if b.head >= len(b.items) {
		b.items = nil
		b.head = 0
		return task
	}
	if b.head > 64 && b.head*2 >= len(b.items) {
		b.items = append([]probeTask(nil), b.items[b.head:]...)
		b.head = 0
	}
	return task
}

func (b *probeTaskBuffer) clear() {
	b.items = nil
	b.head = 0
}

type probeTaskQueue struct {
	mu                   sync.Mutex
	notEmpty             *sync.Cond
	high                 probeTaskBuffer
	normal               probeTaskBuffer
	highCap              int
	normalCap            int
	stopped              bool
	chooseNormalWhenBoth func() bool
}

func newProbeTaskQueue(highCap, normalCap int, chooseNormalWhenBoth func() bool) *probeTaskQueue {
	if highCap <= 0 {
		highCap = defaultQueueCap
	}
	if normalCap <= 0 {
		normalCap = defaultQueueCap
	}
	if chooseNormalWhenBoth == nil {
		chooseNormalWhenBoth = func() bool { return rand.IntN(10) == 0 }
	}
	q := &probeTaskQueue{
		highCap:              highCap,
		normalCap:            normalCap,
		chooseNormalWhenBoth: chooseNormalWhenBoth,
	}
	q.notEmpty = sync.NewCond(&q.mu)
	return q
}

func (q *probeTaskQueue) Enqueue(task probeTask, priority probePriority) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return false
	}

	switch priority {
	case probePriorityHigh:
		if q.high.len() >= q.highCap {
			return false
		}
		q.high.push(task)
	default:
		if q.normal.len() >= q.normalCap {
			return false
		}
		q.normal.push(task)
	}

	q.notEmpty.Signal()
	return true
}

func (q *probeTaskQueue) Dequeue() (probeTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if q.stopped {
			return probeTask{}, false
		}

		highLen := q.high.len()
		normalLen := q.normal.len()
		switch {
		case highLen > 0 && normalLen > 0:
			if q.chooseNormalWhenBoth() {
				return q.normal.pop(), true
			}
			return q.high.pop(), true
		case highLen > 0:
			return q.high.pop(), true
		case normalLen > 0:
			return q.normal.pop(), true
		default:
			q.notEmpty.Wait()
		}
	}
}

func (q *probeTaskQueue) StopDropPending() {
	q.mu.Lock()
	q.stopped = true
	q.high.clear()
	q.normal.clear()
	q.mu.Unlock()
	q.notEmpty.Broadcast()
}

type egressProbeErrorStage int

const (
	egressProbeNoError egressProbeErrorStage = iota
	egressProbeFetchError
	egressProbeParseError
)

// NewProbeManager creates a new ProbeManager.
func NewProbeManager(cfg ProbeConfig) *ProbeManager {
	conc := cfg.Concurrency
	if conc <= 0 {
		conc = 8
	}
	queueCap := cfg.QueueCapacity
	if queueCap <= 0 {
		queueCap = conc * 4
		if queueCap < defaultQueueCap {
			queueCap = defaultQueueCap
		}
	}

	return &ProbeManager{
		pool:                            cfg.Pool,
		stopCh:                          make(chan struct{}),
		fetcher:                         cfg.Fetcher,
		fetcherWithContext:              cfg.FetcherWithContext,
		workerCount:                     conc,
		taskQueue:                       newProbeTaskQueue(queueCap, queueCap, cfg.ChooseNormalWhenBoth),
		taskStates:                      xsync.NewMap[probeTaskKey, *probeTaskState](),
		maxEgressTestInterval:           cfg.MaxEgressTestInterval,
		maxLatencyTestInterval:          cfg.MaxLatencyTestInterval,
		maxAuthorityLatencyTestInterval: cfg.MaxAuthorityLatencyTestInterval,
		latencyTestURL:                  cfg.LatencyTestURL,
		latencyAuthorities:              cfg.LatencyAuthorities,
		egressTraceURLs:                 cfg.EgressTraceURLs,
		probeTimeout:                    cfg.ProbeTimeout,
		onProbeEvent:                    cfg.OnProbeEvent,
		maxConsecutiveFailures:          cfg.MaxConsecutiveFailures,
		scanBatch:                       cfg.ScanBatch,
		failureLog:                      newFailureLogLimiter(cfg.FailureLogMode),
	}
}

// SetOnProbeEvent sets the probe event callback. Must be called before Start.
func (m *ProbeManager) SetOnProbeEvent(fn func(kind string)) {
	m.onProbeEvent = fn
}

// Start launches the background probe workers.
func (m *ProbeManager) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		scanloop.Run(m.stopCh, scanloop.DefaultMinInterval, scanloop.DefaultJitterRange, m.scanEgress)
	}()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		scanloop.Run(m.stopCh, scanloop.DefaultMinInterval, scanloop.DefaultJitterRange, m.scanLatency)
	}()

	for i := 0; i < m.workerCount; i++ {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.runProbeWorker()
		}()
	}
}

// Stop signals all probe workers to stop and waits for completion.
//
// Design note:
//   - In-flight worker tasks are drained before Stop returns.
//   - Pending queued tasks are dropped on stop.
//   - We intentionally do not reject post-stop triggers via extra manager-global
//     state; expected ownership is that callers stop upstream event sources first.
func (m *ProbeManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		m.taskQueue.StopDropPending()
	})
	m.wg.Wait()
}

// TriggerImmediateEgressProbe enqueues an async egress probe for a node.
// Caller returns immediately.
func (m *ProbeManager) TriggerImmediateEgressProbe(hash node.Hash) {
	m.enqueueProbe(hash, probeTaskKindEgress, probePriorityNormal)
}

// TriggerImmediateLatencyProbe enqueues an async latency probe for a node.
// Caller returns immediately.
func (m *ProbeManager) TriggerImmediateLatencyProbe(hash node.Hash) {
	m.enqueueProbe(hash, probeTaskKindLatency, probePriorityNormal)
}

func (m *ProbeManager) egressURLs() []string {
	configured := defaultEgressTraceURLs
	if m.egressTraceURLs != nil {
		configured = m.egressTraceURLs()
		if len(configured) == 0 {
			configured = defaultEgressTraceURLs
		}
	}

	urls := make([]string, 0, len(configured))
	seen := make(map[string]struct{}, len(configured))
	for _, rawURL := range configured {
		url := strings.TrimSpace(rawURL)
		if url == "" {
			continue
		}
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		urls = append(urls, url)
	}
	if len(urls) == 0 {
		return append([]string(nil), defaultEgressTraceURLs...)
	}
	return urls
}

func (m *ProbeManager) newProbeContext() (context.Context, context.CancelFunc) {
	timeout := defaultProbeTimeout
	if m.probeTimeout != nil {
		if configured := m.probeTimeout(); configured > 0 {
			timeout = configured
		}
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (m *ProbeManager) fetch(ctx context.Context, hash node.Hash, url string) ([]byte, time.Duration, error) {
	if m.fetcherWithContext != nil {
		return m.fetcherWithContext(ctx, hash, url)
	}
	if m.fetcher != nil {
		return m.fetcher(hash, url)
	}
	return nil, 0, fmt.Errorf("no probe fetcher configured")
}

// EgressProbeResult holds the results of a synchronous egress probe.
type EgressProbeResult struct {
	EgressIP      string  `json:"egress_ip"`
	Region        string  `json:"region,omitempty"`
	LatencyEwmaMs float64 `json:"latency_ewma_ms"`
}

// ProbeEgressSync performs a blocking egress probe and returns the results.
// Used by API action endpoints that must return probe data synchronously.
func (m *ProbeManager) ProbeEgressSync(hash node.Hash) (*EgressProbeResult, error) {
	if m.fetcher == nil && m.fetcherWithContext == nil {
		return nil, fmt.Errorf("no probe fetcher configured")
	}
	select {
	case <-m.stopCh:
		return nil, fmt.Errorf("probe manager stopped")
	default:
	}

	entry, ok := m.pool.GetEntry(hash)
	if !ok {
		return nil, fmt.Errorf("node not found")
	}
	if entry.Outbound.Load() == nil {
		return nil, fmt.Errorf("node outbound not ready")
	}

	// Record synchronous probe attempts for metrics parity with async paths.
	if m.onProbeEvent != nil {
		m.onProbeEvent("egress")
	}

	ctx, cancel := m.newProbeContext()
	defer cancel()
	ip, stage, err := m.performEgressProbe(ctx, hash)
	if err != nil {
		if stage == egressProbeParseError {
			return nil, fmt.Errorf("parse egress IP: %w", err)
		}
		return nil, fmt.Errorf("egress probe failed: %w", err)
	}

	// Read back EWMA for cloudflare.com from the latency table.
	var ewmaMs float64
	if entry.LatencyTable != nil {
		if stats, ok := entry.LatencyTable.GetDomainStats(egressTraceDomain); ok {
			ewmaMs = float64(stats.Ewma) / float64(time.Millisecond)
		}
	}

	return &EgressProbeResult{
		EgressIP:      ip.String(),
		LatencyEwmaMs: ewmaMs,
	}, nil
}

// LatencyProbeResult holds the results of a synchronous latency probe.
type LatencyProbeResult struct {
	LatencyEwmaMs float64 `json:"latency_ewma_ms"`
}

// ProbeLatencySync performs a blocking latency probe and returns the results.
func (m *ProbeManager) ProbeLatencySync(hash node.Hash) (*LatencyProbeResult, error) {
	if m.fetcher == nil && m.fetcherWithContext == nil {
		return nil, fmt.Errorf("no probe fetcher configured")
	}
	select {
	case <-m.stopCh:
		return nil, fmt.Errorf("probe manager stopped")
	default:
	}

	entry, ok := m.pool.GetEntry(hash)
	if !ok {
		return nil, fmt.Errorf("node not found")
	}
	if entry.Outbound.Load() == nil {
		return nil, fmt.Errorf("node outbound not ready")
	}

	testURL := m.currentLatencyTestURL()
	domain := netutil.ExtractDomain(testURL)

	// Record synchronous probe attempts for metrics parity with async paths.
	if m.onProbeEvent != nil {
		m.onProbeEvent("latency")
	}

	ctx, cancel := m.newProbeContext()
	defer cancel()
	if err := m.performLatencyProbe(ctx, hash, testURL); err != nil {
		return nil, fmt.Errorf("latency probe failed: %w", err)
	}

	// Read back EWMA.
	var ewmaMs float64
	if entry.LatencyTable != nil {
		if stats, ok := entry.LatencyTable.GetDomainStats(domain); ok {
			ewmaMs = float64(stats.Ewma) / float64(time.Millisecond)
		}
	}

	return &LatencyProbeResult{
		LatencyEwmaMs: ewmaMs,
	}, nil
}

// scanEgress iterates all pool nodes and probes those due for egress check.
//
// At most batchLimit tasks are enqueued per tick and the window rotates, so
// successive ticks cover the whole pool. Without the cap one tick enqueued one
// task per node (tens of thousands on a large pool), flooding the task queue and
// the probe workers in a burst.
func (m *ProbeManager) scanEgress() {
	now := time.Now()
	interval := 24 * time.Hour // default MaxEgressTestInterval
	if m.maxEgressTestInterval != nil {
		interval = m.maxEgressTestInterval()
	}
	lookahead := 15 * time.Second
	subLookup := m.pool.MakeSubLookup()
	maxFailures := m.currentMaxConsecutiveFailures()

	candidates := make([]node.Hash, 0, m.scanCandidateHint())
	m.pool.Range(func(h node.Hash, entry *node.NodeEntry) bool {
		// Check stop signal.
		select {
		case <-m.stopCh:
			return false
		default:
		}

		if entry.IsDisabledBySubscriptions(subLookup) {
			return true // disabled node -> skip periodic probe
		}

		if entry.Outbound.Load() == nil {
			return true // skip nil outbound
		}

		if !egressProbeDue(entry, now, interval, lookahead, maxFailures) {
			return true // not yet due
		}

		candidates = append(candidates, h)
		return true
	})

	m.enqueueScanWindow(candidates, probeTaskKindEgress, &m.egressCursor)
}

// scanLatency iterates all pool nodes and probes those due for latency check.
// Batch limiting and the rotating window work exactly as in scanEgress.
func (m *ProbeManager) scanLatency() {
	now := time.Now()
	maxLatencyInterval := 1 * time.Hour // default
	if m.maxLatencyTestInterval != nil {
		maxLatencyInterval = m.maxLatencyTestInterval()
	}
	maxAuthorityInterval := 3 * time.Hour // default
	if m.maxAuthorityLatencyTestInterval != nil {
		maxAuthorityInterval = m.maxAuthorityLatencyTestInterval()
	}
	lookahead := 15 * time.Second
	subLookup := m.pool.MakeSubLookup()
	maxFailures := m.currentMaxConsecutiveFailures()
	var authorities []string
	if m.latencyAuthorities != nil {
		authorities = m.latencyAuthorities()
	}

	candidates := make([]node.Hash, 0, m.scanCandidateHint())
	m.pool.Range(func(h node.Hash, entry *node.NodeEntry) bool {
		select {
		case <-m.stopCh:
			return false
		default:
		}

		if entry.IsDisabledBySubscriptions(subLookup) {
			return true // disabled node -> skip periodic probe
		}

		if entry.Outbound.Load() == nil {
			return true // skip nil outbound
		}

		if !m.isLatencyProbeDue(entry, now, maxLatencyInterval, maxAuthorityInterval, authorities, lookahead, maxFailures) {
			return true
		}

		candidates = append(candidates, h)
		return true
	})

	m.enqueueScanWindow(candidates, probeTaskKindLatency, &m.latencyCursor)
}

// enqueueScanWindow enqueues one cursor window of the candidate list.
func (m *ProbeManager) enqueueScanWindow(candidates []node.Hash, kind probeTaskKind, cursor *scanCursor) {
	if len(candidates) == 0 {
		return
	}
	start, n := cursor.take(len(candidates), m.scanBatchLimit())
	for i := 0; i < n; i++ {
		select {
		case <-m.stopCh:
			return
		default:
		}
		m.enqueueProbe(candidates[(start+i)%len(candidates)], kind, probePriorityNormal)
	}
}

// scanBatchLimit returns how many tasks one scan tick may enqueue:
// min(poolSize, max(64, ProbeConcurrency)), overridable via ProbeConfig.ScanBatch.
func (m *ProbeManager) scanBatchLimit() int {
	limit := m.workerCount
	if limit < minScanBatch {
		limit = minScanBatch
	}
	if m.scanBatch > 0 {
		limit = m.scanBatch
	}
	if poolSize := m.pool.Size(); limit > poolSize {
		limit = poolSize
	}
	if limit < 0 {
		limit = 0
	}
	return limit
}

// scanCandidateHint pre-sizes the per-tick candidate slice without allocating
// one slot per node on very large pools.
func (m *ProbeManager) scanCandidateHint() int {
	hint := m.scanBatchLimit() * 2
	if hint < 64 {
		hint = 64
	}
	return hint
}

// currentMaxConsecutiveFailures returns the live circuit-breaker threshold, or 0
// when backoff is not configured.
func (m *ProbeManager) currentMaxConsecutiveFailures() int {
	if m.maxConsecutiveFailures == nil {
		return 0
	}
	if v := m.maxConsecutiveFailures(); v > 0 {
		return v
	}
	return 0
}

// probeBackoffMultiplier returns the interval multiplier for a node whose
// circuit breaker is currently open:
//
//	1 << min(failureCount/maxConsecutiveFailures, 3)  →  2x / 4x / 8x
//
// Healthy nodes (circuit closed) and pools without a failure threshold always
// return 1. A newly added node also starts circuit-open with failureCount 0, so
// it returns 1 and is probed at the normal rate — exactly what it needs.
func probeBackoffMultiplier(entry *node.NodeEntry, maxConsecutiveFailures int) int {
	if entry == nil || maxConsecutiveFailures <= 0 {
		return 1
	}
	if entry.CircuitOpenSince.Load() == 0 {
		return 1
	}
	failures := int(entry.FailureCount.Load())
	if failures < 0 {
		failures = 0
	}
	shift := failures / maxConsecutiveFailures
	if shift > probeBackoffMaxShift {
		shift = probeBackoffMaxShift
	}
	return 1 << shift
}

// effectiveProbeInterval applies the backoff multiplier, capped at 24h so a
// zombie node is still re-probed regularly enough to notice recovery.
func effectiveProbeInterval(interval time.Duration, multiplier int) time.Duration {
	if interval <= 0 {
		return interval
	}
	if multiplier < 1 {
		multiplier = 1
	}
	effective := interval * time.Duration(multiplier)
	if effective > probeBackoffMaxInterval {
		effective = probeBackoffMaxInterval
	}
	return effective
}

// egressProbeDue reports whether an egress probe is due, applying the
// circuit-open backoff so dead nodes decay to at most one probe per 24h.
func egressProbeDue(
	entry *node.NodeEntry,
	now time.Time,
	interval time.Duration,
	lookahead time.Duration,
	maxConsecutiveFailures int,
) bool {
	lastAttempt := entry.LastEgressUpdateAttempt.Load()
	if lastAttempt == 0 {
		return true
	}
	effective := effectiveProbeInterval(interval, probeBackoffMultiplier(entry, maxConsecutiveFailures))
	nextDue := time.Unix(0, lastAttempt).Add(effective).Add(-lookahead)
	return !now.Before(nextDue)
}

func (m *ProbeManager) runProbeWorker() {
	for {
		task, ok := m.taskQueue.Dequeue()
		if !ok {
			return
		}

		state, ok := m.markTaskRunning(task.key)
		if !ok {
			continue
		}

		m.executeTask(task)
		m.finishTask(task.key, state)
	}
}

func (m *ProbeManager) executeTask(task probeTask) {
	entry, ok := m.pool.GetEntry(task.key.hash)
	if !ok || entry.Outbound.Load() == nil {
		return
	}
	if entry.IsDisabledBySubscriptions(m.pool.MakeSubLookup()) {
		return
	}

	switch task.key.kind {
	case probeTaskKindEgress:
		m.probeEgress(task.key.hash, entry)
	case probeTaskKindLatency:
		m.probeLatency(task.key.hash, entry, m.currentLatencyTestURL())
	}
}

func (m *ProbeManager) enqueueProbe(hash node.Hash, kind probeTaskKind, priority probePriority) bool {
	key := probeTaskKey{hash: hash, kind: kind}
	state, _ := m.taskStates.LoadOrCompute(key, func() (*probeTaskState, bool) {
		return &probeTaskState{}, false
	})
	allowQueuedHighUpgrade := priority == probePriorityHigh

	for {
		flags := state.flags.Load()
		if flags&taskFlagRunning != 0 {
			next := flags | taskFlagDirty
			if priority == probePriorityHigh {
				next |= taskFlagDirtyHigh
			}
			if state.flags.CompareAndSwap(flags, next) {
				return false
			}
			continue
		}

		if flags&taskFlagQueued != 0 {
			// If a normal-priority task is already queued, add a high-priority token
			// so the next dequeue can observe the upgraded urgency. The stale normal
			// token will later no-op when it reaches a worker.
			if allowQueuedHighUpgrade && flags&taskFlagQueuedHigh == 0 {
				next := flags | taskFlagQueuedHigh
				if !state.flags.CompareAndSwap(flags, next) {
					continue
				}
				if m.taskQueue.Enqueue(probeTask{key: key}, probePriorityHigh) {
					return true
				}
				for {
					current := state.flags.Load()
					revert := current &^ taskFlagQueuedHigh
					if state.flags.CompareAndSwap(current, revert) {
						break
					}
				}
				allowQueuedHighUpgrade = false
				continue
			}

			next := flags | taskFlagDirty
			if priority == probePriorityHigh {
				next |= taskFlagDirtyHigh
			}
			if state.flags.CompareAndSwap(flags, next) {
				return false
			}
			continue
		}

		next := flags | taskFlagQueued
		if priority == probePriorityHigh {
			next |= taskFlagQueuedHigh
		} else {
			next &^= taskFlagQueuedHigh
		}
		if !state.flags.CompareAndSwap(flags, next) {
			continue
		}

		if m.taskQueue.Enqueue(probeTask{key: key}, priority) {
			return true
		}

		m.clearDroppedState(state)
		m.tryDeleteTaskState(key, state)
		return false
	}
}

func (m *ProbeManager) markTaskRunning(key probeTaskKey) (*probeTaskState, bool) {
	state, ok := m.taskStates.Load(key)
	if !ok {
		return nil, false
	}

	for {
		flags := state.flags.Load()
		if flags&taskFlagQueued == 0 {
			m.tryDeleteTaskState(key, state)
			return nil, false
		}
		next := (flags | taskFlagRunning) &^ (taskFlagQueued | taskFlagQueuedHigh)
		if state.flags.CompareAndSwap(flags, next) {
			return state, true
		}
	}
}

func (m *ProbeManager) finishTask(key probeTaskKey, state *probeTaskState) {
	requeue := false
	requeuePriority := probePriorityNormal

	for {
		flags := state.flags.Load()
		next := flags &^ taskFlagRunning
		requeue = false

		if flags&taskFlagDirty != 0 {
			requeue = true
			next |= taskFlagQueued
			next &^= taskFlagDirty
			if flags&taskFlagDirtyHigh != 0 {
				next |= taskFlagQueuedHigh
				next &^= taskFlagDirtyHigh
				requeuePriority = probePriorityHigh
			} else {
				next &^= taskFlagQueuedHigh
				requeuePriority = probePriorityNormal
			}
		} else {
			next &^= (taskFlagDirty | taskFlagDirtyHigh | taskFlagQueuedHigh)
		}

		if state.flags.CompareAndSwap(flags, next) {
			break
		}
	}

	if requeue {
		if !m.taskQueue.Enqueue(probeTask{key: key}, requeuePriority) {
			m.clearDroppedState(state)
		}
	}

	m.tryDeleteTaskState(key, state)
}

func (m *ProbeManager) clearDroppedState(state *probeTaskState) {
	for {
		flags := state.flags.Load()
		next := flags &^ (taskFlagQueued | taskFlagQueuedHigh | taskFlagDirty | taskFlagDirtyHigh)
		if state.flags.CompareAndSwap(flags, next) {
			return
		}
	}
}

func (m *ProbeManager) tryDeleteTaskState(key probeTaskKey, state *probeTaskState) {
	m.taskStates.Compute(key, func(current *probeTaskState, loaded bool) (*probeTaskState, xsync.ComputeOp) {
		if !loaded || current != state {
			return current, xsync.CancelOp
		}
		if current.flags.Load() != 0 {
			return current, xsync.CancelOp
		}
		return nil, xsync.DeleteOp
	})
}

// isLatencyProbeDue checks whether a node needs a latency probe, based on
// last probe-attempt timestamps (not latency-table timestamps).
//
// maxConsecutiveFailures enables the circuit-open backoff: a node that keeps
// failing is probed 2x/4x/8x less often (capped at 24h), which cuts the probe
// rate of permanently dead nodes without delaying the recovery of live ones.
// Pass 0 (or a non-positive value) to disable backoff.
func (m *ProbeManager) isLatencyProbeDue(
	entry *node.NodeEntry,
	now time.Time,
	maxLatencyInterval, maxAuthorityInterval time.Duration,
	authorities []string,
	lookahead time.Duration,
	maxConsecutiveFailures int,
) bool {
	multiplier := probeBackoffMultiplier(entry, maxConsecutiveFailures)

	lastAny := entry.LastLatencyProbeAttempt.Load()
	if lastAny == 0 {
		return true
	}
	anyDeadline := time.Unix(0, lastAny).Add(effectiveProbeInterval(maxLatencyInterval, multiplier)).Add(-lookahead)
	if !now.Before(anyDeadline) {
		return true
	}

	if len(authorities) == 0 {
		return false
	}

	lastAuthority := entry.LastAuthorityLatencyProbeAttempt.Load()
	if lastAuthority == 0 {
		return true
	}
	authorityDeadline := time.Unix(0, lastAuthority).
		Add(effectiveProbeInterval(maxAuthorityInterval, multiplier)).
		Add(-lookahead)
	return !now.Before(authorityDeadline)
}

// probeEgress performs a single egress probe against a node via Cloudflare trace.
// Writes back: RecordResult, RecordLatency (cloudflare.com), UpdateNodeEgressIP.
func (m *ProbeManager) probeEgress(hash node.Hash, entry *node.NodeEntry) {
	if m.fetcher == nil && m.fetcherWithContext == nil {
		return
	}

	if entry.Outbound.Load() == nil {
		return
	}

	// Always record the probe attempt (success or failure).
	if m.onProbeEvent != nil {
		m.onProbeEvent("egress")
	}

	ctx, cancel := m.newProbeContext()
	defer cancel()
	_, stage, err := m.performEgressProbe(ctx, hash)
	if err != nil {
		if stage == egressProbeParseError {
			m.logEgressFailure("parse egress IP for %s: %v", hash.Hex(), err)
			return
		}
		m.logEgressFailure("egress probe failed for %s: %v", hash.Hex(), err)
		return
	}
}

// probeLatency performs a latency probe against a node using the configured test URL.
// Writes back: RecordResult, RecordLatency.
func (m *ProbeManager) probeLatency(hash node.Hash, entry *node.NodeEntry, testURL string) {
	if m.fetcher == nil && m.fetcherWithContext == nil {
		return
	}

	if entry.Outbound.Load() == nil {
		return
	}

	// Always record the probe attempt (success or failure).
	if m.onProbeEvent != nil {
		m.onProbeEvent("latency")
	}

	ctx, cancel := m.newProbeContext()
	defer cancel()
	if err := m.performLatencyProbe(ctx, hash, testURL); err != nil {
		m.logLatencyFailure("latency probe failed for %s: %v", hash.Hex(), err)
		return
	}
}

// logLatencyFailure emits a latency-probe failure line subject to the
// configured rate limit. Errors are still returned to callers unchanged; only
// the log line is throttled.
func (m *ProbeManager) logLatencyFailure(format string, args ...any) {
	if m.failureLog == nil {
		log.Printf("[probe] "+format, args...)
		return
	}
	m.failureLog.printf("[probe] "+format, args...)
}

// logEgressFailure emits an egress-probe failure line subject to the configured
// rate limit.
func (m *ProbeManager) logEgressFailure(format string, args ...any) {
	if m.failureLog == nil {
		log.Printf("[probe] "+format, args...)
		return
	}
	m.failureLog.printf("[probe] "+format, args...)
}

func (m *ProbeManager) performEgressProbe(ctx context.Context, hash node.Hash) (netip.Addr, egressProbeErrorStage, error) {
	var lastStage egressProbeErrorStage = egressProbeFetchError
	var lastErr error

	for _, url := range m.egressURLs() {
		if err := ctx.Err(); err != nil {
			lastErr = err
			break
		}

		body, latency, err := m.fetch(ctx, hash, url)
		if err != nil {
			lastStage = egressProbeFetchError
			lastErr = fmt.Errorf("%s: %w", url, err)
			continue
		}

		// Fetch success alone is not enough — a non-trace body (captive portal,
		// error page, truncated response) must count as probe failure.
		ip, loc, err := ParseCloudflareTrace(body)
		if err != nil {
			lastStage = egressProbeParseError
			lastErr = fmt.Errorf("%s: %w", url, err)
			continue
		}

		m.pool.UpdateNodeEgressIP(hash, &ip, loc)
		if latency > 0 {
			m.pool.RecordLatency(hash, egressTraceDomain, &latency)
		}
		m.pool.RecordResult(hash, true)
		return ip, egressProbeNoError, nil
	}

	m.pool.UpdateNodeEgressIP(hash, nil, nil)
	m.pool.RecordResult(hash, false)
	if lastErr == nil {
		lastErr = fmt.Errorf("all egress trace URLs failed")
	}
	return netip.Addr{}, lastStage, lastErr
}

func (m *ProbeManager) performLatencyProbe(ctx context.Context, hash node.Hash, testURL string) error {
	domain := netutil.ExtractDomain(testURL)
	_, latency, err := m.fetch(ctx, hash, testURL)
	if err != nil {
		m.pool.RecordResult(hash, false)
		m.pool.RecordLatency(hash, domain, nil)
		return err
	}

	m.pool.RecordResult(hash, true)
	m.pool.RecordLatency(hash, domain, &latency)
	return nil
}

func (m *ProbeManager) currentLatencyTestURL() string {
	testURL := defaultLatencyTestURL
	if m.latencyTestURL != nil {
		testURL = m.latencyTestURL()
	}
	return testURL
}
