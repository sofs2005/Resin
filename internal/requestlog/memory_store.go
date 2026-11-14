package requestlog

import (
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/Resinat/Resin/internal/proxy"
)

// StorageMode selects where request logs are kept.
type StorageMode string

const (
	// StorageModeSQLite keeps the historical rolling-SQLite behaviour.
	StorageModeSQLite StorageMode = "sqlite"
	// StorageModeMemory keeps request logs in a bounded in-memory ring buffer.
	// Nothing is written to disk and the history is lost on restart.
	StorageModeMemory StorageMode = "memory"
)

// defaultMemoryEntries is the ring-buffer capacity used when the caller does not
// specify one (mirrors RESIN_REQUEST_LOG_MEMORY_ENTRIES).
const defaultMemoryEntries = 10000

// memoryLogEntry is one retained log record: its summary plus the optional
// detail payload (nil when the entry carried no payload at all).
type memoryLogEntry struct {
	summary LogSummary
	payload *PayloadRow
}

// memoryLogStore is a fixed-capacity FIFO ring of request-log entries.
//
// Ordering of the ring itself is insertion order; query ordering is applied on
// every read (ts_ns DESC, id ASC) so the observable semantics match the SQLite
// implementation. Write is O(1) amortised, reads are O(capacity) at worst which
// is bounded by defaultMemoryEntries.
//
// Every retained byte slice is copied on insert so callers may reuse or mutate
// their entry buffers after InsertBatch returns.
type memoryLogStore struct {
	mu       sync.RWMutex
	capacity int

	// order is a ring of ids. The oldest live id sits at head and the ring holds
	// exactly count live ids.
	order []string
	head  int
	count int

	byID map[string]*memoryLogEntry
}

// newMemoryLogStore creates a ring buffer with the given capacity.
// A non-positive capacity falls back to defaultMemoryEntries.
func newMemoryLogStore(capacity int) *memoryLogStore {
	if capacity <= 0 {
		capacity = defaultMemoryEntries
	}
	return &memoryLogStore{
		capacity: capacity,
		order:    make([]string, capacity),
		byID:     make(map[string]*memoryLogEntry, capacity),
	}
}

// capacityEntries reports the configured ring capacity.
func (s *memoryLogStore) capacityEntries() int {
	if s == nil {
		return 0
	}
	return s.capacity
}

// len reports how many entries are currently retained.
func (s *memoryLogStore) len() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// insert appends a batch of entries, evicting the oldest entries when the ring
// is full. Entries whose (derived) id already exists are ignored, matching the
// INSERT OR IGNORE behaviour of the SQLite implementation.
// It returns the number of entries actually retained.
func (s *memoryLogStore) insert(entries []proxy.RequestLogEntry) int {
	if s == nil || len(entries) == 0 {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	inserted := 0
	for i := range entries {
		e := &entries[i]
		id := e.ID
		if id == "" {
			id = uuid.NewString()
		}
		if _, exists := s.byID[id]; exists {
			continue // INSERT OR IGNORE
		}

		if s.count == s.capacity {
			// Evict the oldest entry, then reuse its slot for the new id.
			delete(s.byID, s.order[s.head])
			s.order[s.head] = id
			s.head = (s.head + 1) % s.capacity
		} else {
			s.order[(s.head+s.count)%s.capacity] = id
			s.count++
		}
		s.byID[id] = newMemoryLogEntry(id, e)
		inserted++
	}
	return inserted
}

// list returns a page of matching summaries ordered by ts_ns DESC, id ASC.
// hasMore and the next cursor follow exactly the same rules as the SQLite path.
func (s *memoryLogStore) list(f ListFilter) ([]LogSummary, bool, *ListCursor) {
	if s == nil {
		return []LogSummary{}, false, nil
	}

	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}

	s.mu.RLock()
	matches := make([]LogSummary, 0, s.count)
	for _, entry := range s.byID {
		if matchLogFilter(&entry.summary, f) {
			matches = append(matches, entry.summary)
		}
	}
	s.mu.RUnlock()

	if len(matches) == 0 {
		return []LogSummary{}, false, nil
	}

	// DESIGN.md: ts_ns DESC, same ts_ns by id ASC.
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].TsNs != matches[j].TsNs {
			return matches[i].TsNs > matches[j].TsNs
		}
		return matches[i].ID < matches[j].ID
	})

	hasMore := len(matches) > limit
	if hasMore {
		matches = matches[:limit]
	}

	var nextCursor *ListCursor
	if hasMore && len(matches) > 0 {
		last := matches[len(matches)-1]
		nextCursor = &ListCursor{TsNs: last.TsNs, ID: last.ID}
	}
	return matches, hasMore, nextCursor
}

// getByID returns a copy of the summary for id, or nil when it is not retained.
func (s *memoryLogStore) getByID(id string) *LogSummary {
	if s == nil || id == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.byID[id]
	if !ok {
		return nil
	}
	out := entry.summary
	return &out
}

// getPayloads returns a copy of the payload for id, or nil when the entry is not
// retained or was stored without a payload row.
func (s *memoryLogStore) getPayloads(id string) *PayloadRow {
	if s == nil || id == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.byID[id]
	if !ok || entry.payload == nil {
		return nil
	}
	return &PayloadRow{
		LogID:       entry.payload.LogID,
		ReqHeaders:  cloneBytes(entry.payload.ReqHeaders),
		ReqBody:     cloneBytes(entry.payload.ReqBody),
		RespHeaders: cloneBytes(entry.payload.RespHeaders),
		RespBody:    cloneBytes(entry.payload.RespBody),
	}
}

// newMemoryLogEntry converts a proxy entry into the retained representation.
func newMemoryLogEntry(id string, e *proxy.RequestLogEntry) *memoryLogEntry {
	hasPayload := e.ReqHeaders != nil || e.ReqBody != nil || e.RespHeaders != nil || e.RespBody != nil

	entry := &memoryLogEntry{
		summary: LogSummary{
			ID:                   id,
			TsNs:                 e.StartedAtNs,
			ProxyType:            int(e.ProxyType),
			ClientIP:             e.ClientIP,
			PlatformID:           e.PlatformID,
			PlatformName:         e.PlatformName,
			Account:              e.Account,
			TargetHost:           e.TargetHost,
			TargetURL:            e.TargetURL,
			NodeHash:             e.NodeHash,
			NodeTag:              e.NodeTag,
			EgressIP:             e.EgressIP,
			DurationNs:           e.DurationNs,
			FirstByteDurationNs:  e.FirstByteDurationNs,
			NetOK:                e.NetOK,
			HTTPMethod:           e.HTTPMethod,
			HTTPStatus:           e.HTTPStatus,
			ResinError:           e.ResinError,
			UpstreamStage:        e.UpstreamStage,
			UpstreamErrKind:      e.UpstreamErrKind,
			UpstreamErrno:        e.UpstreamErrno,
			UpstreamErrMsg:       e.UpstreamErrMsg,
			IngressBytes:         e.IngressBytes,
			EgressBytes:          e.EgressBytes,
			PayloadPresent:       hasPayload,
			ReqHeadersLen:        e.ReqHeadersLen,
			ReqBodyLen:           e.ReqBodyLen,
			RespHeadersLen:       e.RespHeadersLen,
			RespBodyLen:          e.RespBodyLen,
			ReqHeadersTruncated:  e.ReqHeadersTruncated,
			ReqBodyTruncated:     e.ReqBodyTruncated,
			RespHeadersTruncated: e.RespHeadersTruncated,
			RespBodyTruncated:    e.RespBodyTruncated,
		},
	}
	if hasPayload {
		entry.payload = &PayloadRow{
			LogID:       id,
			ReqHeaders:  cloneBytes(e.ReqHeaders),
			ReqBody:     cloneBytes(e.ReqBody),
			RespHeaders: cloneBytes(e.RespHeaders),
			RespBody:    cloneBytes(e.RespBody),
		}
	}
	return entry
}

// matchLogFilter mirrors the WHERE clause built by Repo.queryLogs.
// Fuzzy matching is ASCII-insensitive in SQLite (lower()); Go's ToLower is a
// superset, which only widens matches for non-ASCII platform names.
func matchLogFilter(row *LogSummary, f ListFilter) bool {
	if f.ProxyType != nil && row.ProxyType != *f.ProxyType {
		return false
	}
	if f.PlatformID != "" {
		if f.Fuzzy {
			if !containsFold(row.PlatformID, f.PlatformID) {
				return false
			}
		} else if row.PlatformID != f.PlatformID {
			return false
		}
	}
	if f.PlatformName != "" {
		if f.Fuzzy {
			if !containsFold(row.PlatformName, f.PlatformName) {
				return false
			}
		} else if row.PlatformName != f.PlatformName {
			return false
		}
	}
	if f.Account != "" {
		if f.Fuzzy {
			if !containsFold(row.Account, f.Account) {
				return false
			}
		} else if row.Account != f.Account || row.Account == "" {
			return false
		}
	}
	if f.TargetHost != "" {
		if f.Fuzzy {
			if !containsFold(row.TargetHost, f.TargetHost) {
				return false
			}
		} else if row.TargetHost != f.TargetHost {
			return false
		}
	}
	if f.EgressIP != "" && row.EgressIP != f.EgressIP {
		return false
	}
	if f.NetOK != nil && row.NetOK != *f.NetOK {
		return false
	}
	if f.HTTPStatus != nil && row.HTTPStatus != *f.HTTPStatus {
		return false
	}
	if f.Before > 0 && row.TsNs >= f.Before {
		return false
	}
	if f.After > 0 && row.TsNs <= f.After {
		return false
	}
	if f.Cursor != nil {
		// ORDER BY ts_ns DESC, id ASC: "after the cursor" means an older ts, or
		// the same ts with a larger id.
		if !(row.TsNs < f.Cursor.TsNs || (row.TsNs == f.Cursor.TsNs && row.ID > f.Cursor.ID)) {
			return false
		}
	}
	return true
}

// containsFold reports whether needle is a case-insensitive substring of hay.
func containsFold(hay, needle string) bool {
	return strings.Contains(strings.ToLower(hay), strings.ToLower(needle))
}

// cloneBytes returns a copy of b, preserving nil.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
