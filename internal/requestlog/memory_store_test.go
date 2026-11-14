package requestlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/proxy"
)

// newMemoryRepo creates a memory-mode repo rooted at a directory that must never
// be created.
func newMemoryRepo(t *testing.T, capacity int) *Repo {
	t.Helper()
	logDir := filepath.Join(t.TempDir(), "logs")
	repo := NewRepo(logDir, 1<<20, 2)
	repo.SetStorageMode(StorageModeMemory, capacity)
	if err := repo.Open(); err != nil {
		t.Fatalf("Open(memory): %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	return repo
}

func memoryTestEntry(id string, tsNs int64, opts ...func(*proxy.RequestLogEntry)) proxy.RequestLogEntry {
	e := proxy.RequestLogEntry{
		ID:          id,
		StartedAtNs: tsNs,
		ProxyType:   proxy.ProxyTypeForward,
		ClientIP:    "10.0.0.1",
		PlatformID:  "plat-1",
		Account:     "acct-a",
		TargetHost:  "example.com",
		NetOK:       true,
		HTTPStatus:  200,
	}
	for _, opt := range opts {
		opt(&e)
	}
	return e
}

func TestRepo_MemoryMode_CreatesNoFiles(t *testing.T) {
	logDir := filepath.Join(t.TempDir(), "logs")
	repo := NewRepo(logDir, 1<<20, 2)
	repo.SetStorageMode(StorageModeMemory, 10)
	if !repo.IsMemory() {
		t.Fatal("repo should report memory mode")
	}
	if err := repo.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer repo.Close()

	if _, err := os.Stat(logDir); !os.IsNotExist(err) {
		t.Fatalf("log dir %s must not be created in memory mode (err=%v)", logDir, err)
	}

	if _, err := repo.InsertBatch([]proxy.RequestLogEntry{
		memoryTestEntry("a", time.Now().UnixNano()),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, err := os.Stat(logDir); !os.IsNotExist(err) {
		t.Fatalf("log dir %s must stay absent after insert (err=%v)", logDir, err)
	}

	// Maintenance paths are no-ops in memory mode.
	if err := repo.CleanupExpired(); err != nil {
		t.Fatalf("CleanupExpired: %v", err)
	}
	if err := repo.TruncateWAL(); err != nil {
		t.Fatalf("TruncateWAL: %v", err)
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRepo_MemoryMode_CapacityEviction(t *testing.T) {
	repo := newMemoryRepo(t, 3)

	entries := make([]proxy.RequestLogEntry, 0, 5)
	for _, id := range []string{"e1", "e2", "e3", "e4", "e5"} {
		entries = append(entries, memoryTestEntry(id, time.Now().UnixNano()))
	}
	inserted, err := repo.InsertBatch(entries)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if inserted != 5 {
		t.Fatalf("inserted = %d, want 5", inserted)
	}

	if got := repo.memory.len(); got != 3 {
		t.Fatalf("retained = %d, want 3 (capacity)", got)
	}

	for _, id := range []string{"e1", "e2"} {
		if row, err := repo.GetByID(id); err != nil || row != nil {
			t.Fatalf("GetByID(%s) = (%v, %v), want (nil, nil) after eviction", id, row, err)
		}
	}
	for _, id := range []string{"e3", "e4", "e5"} {
		row, err := repo.GetByID(id)
		if err != nil {
			t.Fatalf("GetByID(%s): %v", id, err)
		}
		if row == nil || row.ID != id {
			t.Fatalf("GetByID(%s) = %+v, want retained", id, row)
		}
	}
}

func TestRepo_MemoryMode_DuplicateIDIgnored(t *testing.T) {
	repo := newMemoryRepo(t, 10)

	entry := memoryTestEntry("dup", time.Now().UnixNano())
	if n, err := repo.InsertBatch([]proxy.RequestLogEntry{entry}); err != nil || n != 1 {
		t.Fatalf("first insert = (%d, %v), want (1, nil)", n, err)
	}
	// Same id again with a different ts: INSERT OR IGNORE semantics keep the first.
	entry.StartedAtNs = entry.StartedAtNs + int64(time.Second)
	if n, err := repo.InsertBatch([]proxy.RequestLogEntry{entry}); err != nil || n != 0 {
		t.Fatalf("duplicate insert = (%d, %v), want (0, nil)", n, err)
	}
	if got := repo.memory.len(); got != 1 {
		t.Fatalf("retained = %d, want 1", got)
	}
	row, err := repo.GetByID("dup")
	if err != nil || row == nil {
		t.Fatalf("GetByID(dup) = (%v, %v)", row, err)
	}
	if row.TsNs == entry.StartedAtNs {
		t.Fatal("duplicate insert must not overwrite the retained row")
	}
}

func TestRepo_MemoryMode_ListOrderingAndPaging(t *testing.T) {
	repo := newMemoryRepo(t, 100)

	base := time.Now().UnixNano()
	// Same ts for a/b to exercise the id ASC tiebreak.
	entries := []proxy.RequestLogEntry{
		memoryTestEntry("a", base),
		memoryTestEntry("b", base),
		memoryTestEntry("c", base+2),
		memoryTestEntry("d", base+1),
	}
	if _, err := repo.InsertBatch(entries); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	rows, hasMore, cursor, err := repo.List(ListFilter{Limit: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 2 || rows[0].ID != "c" || rows[1].ID != "d" {
		t.Fatalf("unexpected first page: %+v", rows)
	}
	if !hasMore || cursor == nil {
		t.Fatalf("hasMore=%v cursor=%v, want true/non-nil", hasMore, cursor)
	}
	if cursor.TsNs != base+1 || cursor.ID != "d" {
		t.Fatalf("unexpected cursor: %+v", cursor)
	}

	rows2, hasMore2, cursor2, err := repo.List(ListFilter{Limit: 2, Cursor: cursor})
	if err != nil {
		t.Fatalf("List(page2): %v", err)
	}
	if len(rows2) != 2 || rows2[0].ID != "a" || rows2[1].ID != "b" {
		t.Fatalf("unexpected second page: %+v", rows2)
	}
	if hasMore2 || cursor2 != nil {
		t.Fatalf("hasMore=%v cursor=%v, want false/nil", hasMore2, cursor2)
	}

	// Cursor in the middle of a ts tie must resume strictly after the id.
	tied, hasMoreTied, _, err := repo.List(ListFilter{Limit: 10, Cursor: &ListCursor{TsNs: base, ID: "a"}})
	if err != nil {
		t.Fatalf("List(tied): %v", err)
	}
	if hasMoreTied || len(tied) != 1 || tied[0].ID != "b" {
		t.Fatalf("unexpected tied page: %+v (hasMore=%v)", tied, hasMoreTied)
	}
}

func TestRepo_MemoryMode_ListFilters(t *testing.T) {
	repo := newMemoryRepo(t, 100)

	base := time.Now().UnixNano()
	entries := []proxy.RequestLogEntry{
		memoryTestEntry("fwd-ok", base-4, func(e *proxy.RequestLogEntry) {
			e.PlatformID = "plat-1"
			e.PlatformName = "Alpha Platform"
			e.Account = "acct-a"
			e.TargetHost = "a.example.com"
			e.EgressIP = "1.1.1.1"
			e.NetOK = true
			e.HTTPStatus = 200
			e.ProxyType = proxy.ProxyTypeForward
		}),
		memoryTestEntry("rev-fail", base-3, func(e *proxy.RequestLogEntry) {
			e.PlatformID = "plat-2"
			e.PlatformName = "Beta Platform"
			e.Account = ""
			e.TargetHost = "b.example.org"
			e.EgressIP = "2.2.2.2"
			e.NetOK = false
			e.HTTPStatus = 502
			e.ProxyType = proxy.ProxyTypeReverse
		}),
		memoryTestEntry("socks-ok", base-2, func(e *proxy.RequestLogEntry) {
			e.PlatformID = "plat-1"
			e.PlatformName = "Alpha Platform"
			e.Account = "acct-b"
			e.TargetHost = "c.example.net"
			e.EgressIP = "1.1.1.1"
			e.NetOK = true
			e.HTTPStatus = 204
			e.ProxyType = proxy.ProxyTypeSocks5Forward
		}),
	}

	if _, err := repo.InsertBatch(entries); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	proxyType := int(proxy.ProxyTypeForward)
	netOK := true
	status := 502

	cases := []struct {
		name   string
		filter ListFilter
		want   []string
	}{
		{
			name:   "proxy_type",
			filter: ListFilter{ProxyType: &proxyType},
			want:   []string{"fwd-ok"},
		},
		{
			name:   "platform_id_exact",
			filter: ListFilter{PlatformID: "plat-1"},
			want:   []string{"socks-ok", "fwd-ok"},
		},
		{
			name:   "platform_name_fuzzy_case_insensitive",
			filter: ListFilter{PlatformName: "alpha", Fuzzy: true},
			want:   []string{"socks-ok", "fwd-ok"},
		},
		{
			name:   "account_exact",
			filter: ListFilter{Account: "acct-a"},
			want:   []string{"fwd-ok"},
		},
		{
			name:   "account_empty_means_no_filter",
			filter: ListFilter{Account: ""},
			want:   []string{"socks-ok", "rev-fail", "fwd-ok"},
		},
		{
			name:   "target_host_fuzzy",
			filter: ListFilter{TargetHost: "example.o", Fuzzy: true},
			want:   []string{"rev-fail"},
		},
		{
			name:   "egress_ip",
			filter: ListFilter{EgressIP: "1.1.1.1"},
			want:   []string{"socks-ok", "fwd-ok"},
		},
		{
			name:   "net_ok",
			filter: ListFilter{NetOK: &netOK},
			want:   []string{"socks-ok", "fwd-ok"},
		},
		{
			name:   "http_status",
			filter: ListFilter{HTTPStatus: &status},
			want:   []string{"rev-fail"},
		},
		{
			name:   "before_exclusive",
			filter: ListFilter{Before: base - 3},
			want:   []string{"fwd-ok"},
		},
		{
			name:   "after_exclusive",
			filter: ListFilter{After: base - 3},
			want:   []string{"socks-ok"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.filter.Limit = 50
			rows, hasMore, cursor, err := repo.List(tc.filter)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if hasMore || cursor != nil {
				t.Fatalf("hasMore=%v cursor=%v, want false/nil", hasMore, cursor)
			}
			got := make([]string, 0, len(rows))
			for _, row := range rows {
				got = append(got, row.ID)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ids = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("ids = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestRepo_MemoryMode_GetPayloads(t *testing.T) {
	repo := newMemoryRepo(t, 10)

	payloadEntry := memoryTestEntry("with-payload", time.Now().UnixNano(), func(e *proxy.RequestLogEntry) {
		e.ReqHeaders = []byte("req-h")
		e.ReqBody = []byte("req-b")
		e.ReqHeadersLen = 5
		e.ReqBodyLen = 5
		e.ReqBodyTruncated = true
	})
	if _, err := repo.InsertBatch([]proxy.RequestLogEntry{
		payloadEntry,
		memoryTestEntry("no-payload", time.Now().UnixNano()),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	// Mutating the caller's buffer after insert must not affect retained data.
	payloadEntry.ReqHeaders[0] = 'X'

	row, err := repo.GetByID("with-payload")
	if err != nil || row == nil {
		t.Fatalf("GetByID: (%v, %v)", row, err)
	}
	if !row.PayloadPresent || !row.ReqBodyTruncated {
		t.Fatalf("unexpected summary flags: %+v", row)
	}

	payload, err := repo.GetPayloads("with-payload")
	if err != nil {
		t.Fatalf("GetPayloads: %v", err)
	}
	if payload == nil {
		t.Fatal("GetPayloads returned nil for an entry with payload")
	}
	if string(payload.ReqHeaders) != "req-h" || string(payload.ReqBody) != "req-b" {
		t.Fatalf("unexpected payload bytes: %+v", payload)
	}
	if payload.LogID != "with-payload" {
		t.Fatalf("payload.LogID = %q", payload.LogID)
	}
	if payload.RespHeaders != nil || payload.RespBody != nil {
		t.Fatalf("expected nil response payload, got %+v", payload)
	}

	noPayload, err := repo.GetPayloads("no-payload")
	if err != nil {
		t.Fatalf("GetPayloads(no-payload): %v", err)
	}
	if noPayload != nil {
		t.Fatalf("GetPayloads(no-payload) = %+v, want nil", noPayload)
	}

	if missing, err := repo.GetPayloads("does-not-exist"); err != nil || missing != nil {
		t.Fatalf("GetPayloads(missing) = (%v, %v), want (nil, nil)", missing, err)
	}
	if row, err := repo.GetByID("does-not-exist"); err != nil || row != nil {
		t.Fatalf("GetByID(missing) = (%v, %v), want (nil, nil)", row, err)
	}
}

func TestRepo_MemoryMode_GeneratedID(t *testing.T) {
	repo := newMemoryRepo(t, 10)

	entry := memoryTestEntry("", time.Now().UnixNano())
	entry.ID = ""
	if n, err := repo.InsertBatch([]proxy.RequestLogEntry{entry}); err != nil || n != 1 {
		t.Fatalf("InsertBatch = (%d, %v), want (1, nil)", n, err)
	}

	rows, _, _, err := repo.List(ListFilter{Limit: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 1 || rows[0].ID == "" {
		t.Fatalf("expected a generated id, got %+v", rows)
	}
	if row, err := repo.GetByID(rows[0].ID); err != nil || row == nil {
		t.Fatalf("GetByID(generated) = (%v, %v)", row, err)
	}
}

func TestRepo_MemoryMode_ServiceRoundTrip(t *testing.T) {
	repo := newMemoryRepo(t, 10)
	svc := NewService(ServiceConfig{
		Repo:          repo,
		QueueSize:     16,
		FlushBatch:    4,
		FlushInterval: time.Hour,
	})
	svc.Start()
	defer svc.Stop()

	base := time.Now().UnixNano()
	svc.EmitRequestLog(memoryTestEntry("svc-1", base))
	svc.EmitRequestLog(memoryTestEntry("svc-2", base+1))
	svc.FlushNow()

	rows, hasMore, _, err := repo.List(ListFilter{Limit: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if hasMore || len(rows) != 2 {
		t.Fatalf("rows = %+v (hasMore=%v), want 2 entries", rows, hasMore)
	}
}
