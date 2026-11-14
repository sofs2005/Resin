package requestlog

import (
	"reflect"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/proxy"
)

// parityBaseOffset keeps the parity dataset away from "now" so Before/After
// filters cannot accidentally match rows written by a concurrent test.
const parityBaseOffset = int64(time.Hour)

// parityEntries builds a dataset that exercises every observable branch of the
// query semantics: equal ts_ns (id ASC tiebreak), empty account, entries with and
// without payloads, all three proxy types, and repeated egress IPs.
func parityEntries(base int64) []proxy.RequestLogEntry {
	return []proxy.RequestLogEntry{
		{
			ID:            "e0",
			StartedAtNs:   base + 5,
			ProxyType:     proxy.ProxyTypeForward,
			ClientIP:      "10.0.0.1",
			PlatformID:    "plat-1",
			PlatformName:  "Alpha Platform",
			Account:       "acct-a",
			TargetHost:    "a.example.com",
			TargetURL:     "https://a.example.com/x",
			NodeHash:      "node-1",
			NodeTag:       "tag-1",
			EgressIP:      "1.1.1.1",
			DurationNs:    1000,
			NetOK:         true,
			HTTPMethod:    "GET",
			HTTPStatus:    200,
			IngressBytes:  10,
			EgressBytes:   20,
			ReqHeaders:    []byte("req-h-e0"),
			ReqBody:       []byte("req-b-e0"),
			RespHeaders:   []byte("resp-h-e0"),
			RespBody:      []byte("resp-b-e0"),
			ReqHeadersLen: 8,
			ReqBodyLen:    8,
		},
		{
			// Same ts as e0: ordering must fall back to id ASC (e0 before e1).
			ID:              "e1",
			StartedAtNs:     base + 5,
			ProxyType:       proxy.ProxyTypeReverse,
			ClientIP:        "10.0.0.2",
			PlatformID:      "plat-2",
			PlatformName:    "Beta Platform",
			Account:         "acct-b",
			TargetHost:      "b.example.org",
			TargetURL:       "https://b.example.org/y",
			NodeHash:        "node-2",
			EgressIP:        "2.2.2.2",
			DurationNs:      2000,
			NetOK:           false,
			HTTPMethod:      "POST",
			HTTPStatus:      502,
			ResinError:      "UPSTREAM_REQUEST_FAILED",
			UpstreamStage:   "reverse_roundtrip",
			UpstreamErrKind: "connection_refused",
			UpstreamErrno:   "ECONNREFUSED",
			UpstreamErrMsg:  "dial tcp 203.0.113.1:443: connect: connection refused",
			IngressBytes:    30,
			EgressBytes:     40,
		},
		{
			// Empty account must be excluded by an exact Account filter.
			ID:           "e2",
			StartedAtNs:  base + 4,
			ProxyType:    proxy.ProxyTypeSocks5Forward,
			ClientIP:     "10.0.0.3",
			PlatformID:   "plat-1",
			PlatformName: "Alpha Platform",
			Account:      "",
			TargetHost:   "c.example.net",
			EgressIP:     "1.1.1.1",
			DurationNs:   3000,
			NetOK:        true,
			HTTPMethod:   "CONNECT",
			HTTPStatus:   204,
		},
		{
			ID:           "e3",
			StartedAtNs:  base + 3,
			ProxyType:    proxy.ProxyTypeForward,
			ClientIP:     "10.0.0.4",
			PlatformID:   "plat-2",
			PlatformName: "Beta Platform",
			Account:      "acct-a",
			TargetHost:   "d.example.io",
			EgressIP:     "3.3.3.3",
			DurationNs:   4000,
			NetOK:        true,
			HTTPMethod:   "GET",
			HTTPStatus:   200,
			ReqBody:      []byte("only-body"),
			ReqBodyLen:   9,
		},
		{
			ID:           "e4",
			StartedAtNs:  base + 2,
			ProxyType:    proxy.ProxyTypeReverse,
			ClientIP:     "10.0.0.5",
			PlatformID:   "plat-3",
			PlatformName: "Gamma Platform",
			Account:      "acct-c",
			TargetHost:   "example.com",
			EgressIP:     "4.4.4.4",
			DurationNs:   5000,
			NetOK:        false,
			HTTPMethod:   "GET",
			HTTPStatus:   500,
		},
		{
			ID:           "e5",
			StartedAtNs:  base + 1,
			ProxyType:    proxy.ProxyTypeSocks5Forward,
			ClientIP:     "10.0.0.6",
			PlatformID:   "plat-1",
			PlatformName: "Alpha Platform",
			Account:      "acct-b",
			TargetHost:   "e.example.com",
			EgressIP:     "1.1.1.1",
			DurationNs:   6000,
			NetOK:        true,
			HTTPMethod:   "GET",
			HTTPStatus:   302,
		},
	}
}

// newParitySQLiteRepo opens a file-backed repo (the historical backend).
func newParitySQLiteRepo(t *testing.T) *Repo {
	t.Helper()
	repo := NewRepo(t.TempDir(), 1<<20, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("Open(sqlite): %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	return repo
}

// TestRepo_StorageModeParity_List asserts that the memory backend reproduces the
// SQLite backend byte for byte: same rows, same order, same hasMore, same cursor.
// This is the contract that lets the API layer stay backend-agnostic.
func TestRepo_StorageModeParity_List(t *testing.T) {
	base := time.Now().UnixNano() - parityBaseOffset
	entries := parityEntries(base)

	sqliteRepo := newParitySQLiteRepo(t)
	memoryRepo := newMemoryRepo(t, 100)

	for _, repo := range []*Repo{sqliteRepo, memoryRepo} {
		if n, err := repo.InsertBatch(entries); err != nil || n != len(entries) {
			t.Fatalf("InsertBatch(%s) = (%d, %v), want (%d, nil)", repo.StorageMode(), n, err, len(entries))
		}
	}

	proxyTypeForward := int(proxy.ProxyTypeForward)
	proxyTypeReverse := int(proxy.ProxyTypeReverse)
	netOKTrue := true
	netOKFalse := false
	status502 := 502
	status200 := 200

	filters := []struct {
		name   string
		filter ListFilter
	}{
		{name: "no_filter_default_limit", filter: ListFilter{}},
		{name: "explicit_limit", filter: ListFilter{Limit: 10}},
		{name: "limit_one", filter: ListFilter{Limit: 1}},
		{name: "limit_equals_total", filter: ListFilter{Limit: len(entries)}},
		{name: "limit_less_than_total", filter: ListFilter{Limit: 3}},
		{name: "proxy_type_forward", filter: ListFilter{ProxyType: &proxyTypeForward, Limit: 10}},
		{name: "proxy_type_reverse", filter: ListFilter{ProxyType: &proxyTypeReverse, Limit: 10}},
		{name: "platform_id_exact", filter: ListFilter{PlatformID: "plat-1", Limit: 10}},
		{name: "platform_id_exact_unknown", filter: ListFilter{PlatformID: "plat-none", Limit: 10}},
		{name: "platform_name_fuzzy", filter: ListFilter{PlatformName: "alpha", Fuzzy: true, Limit: 10}},
		{name: "platform_name_exact", filter: ListFilter{PlatformName: "Alpha Platform", Limit: 10}},
		{name: "account_exact", filter: ListFilter{Account: "acct-a", Limit: 10}},
		{name: "account_fuzzy", filter: ListFilter{Account: "acct", Fuzzy: true, Limit: 10}},
		{name: "target_host_exact", filter: ListFilter{TargetHost: "example.com", Limit: 10}},
		{name: "target_host_fuzzy", filter: ListFilter{TargetHost: "example.", Fuzzy: true, Limit: 10}},
		{name: "egress_ip", filter: ListFilter{EgressIP: "1.1.1.1", Limit: 10}},
		{name: "net_ok_true", filter: ListFilter{NetOK: &netOKTrue, Limit: 10}},
		{name: "net_ok_false", filter: ListFilter{NetOK: &netOKFalse, Limit: 10}},
		{name: "http_status_502", filter: ListFilter{HTTPStatus: &status502, Limit: 10}},
		{name: "http_status_200", filter: ListFilter{HTTPStatus: &status200, Limit: 10}},
		{name: "before_exclusive", filter: ListFilter{Before: base + 3, Limit: 10}},
		{name: "after_exclusive", filter: ListFilter{After: base + 3, Limit: 10}},
		{name: "before_and_after", filter: ListFilter{Before: base + 5, After: base + 1, Limit: 10}},
		{
			name:   "cursor_tie_middle",
			filter: ListFilter{Limit: 10, Cursor: &ListCursor{TsNs: base + 5, ID: "e0"}},
		},
		{
			name:   "cursor_after_newest_ts",
			filter: ListFilter{Limit: 10, Cursor: &ListCursor{TsNs: base + 5, ID: "e1"}},
		},
		{
			name:   "cursor_oldest_ts",
			filter: ListFilter{Limit: 10, Cursor: &ListCursor{TsNs: base + 1, ID: "e5"}},
		},
		{
			name:   "cursor_with_filter",
			filter: ListFilter{Limit: 10, PlatformID: "plat-1", Cursor: &ListCursor{TsNs: base + 5, ID: "e0"}},
		},
	}

	for _, tc := range filters {
		t.Run(tc.name, func(t *testing.T) {
			wantRows, wantMore, wantCursor, err := sqliteRepo.List(tc.filter)
			if err != nil {
				t.Fatalf("sqlite List: %v", err)
			}
			gotRows, gotMore, gotCursor, err := memoryRepo.List(tc.filter)
			if err != nil {
				t.Fatalf("memory List: %v", err)
			}

			if !reflect.DeepEqual(gotRows, wantRows) {
				t.Fatalf("rows mismatch\nsqlite: %+v\nmemory: %+v", wantRows, gotRows)
			}
			if gotMore != wantMore {
				t.Fatalf("hasMore = %v, want %v", gotMore, wantMore)
			}
			if !reflect.DeepEqual(gotCursor, wantCursor) {
				t.Fatalf("cursor = %+v, want %+v", gotCursor, wantCursor)
			}
		})
	}
}

// TestRepo_StorageModeParity_Pagination walks the full result set page by page on
// both backends and compares the visited id sequence, proving that the cursor
// handoff cannot skip or repeat rows in memory mode.
func TestRepo_StorageModeParity_Pagination(t *testing.T) {
	base := time.Now().UnixNano() - parityBaseOffset
	entries := parityEntries(base)

	sqliteRepo := newParitySQLiteRepo(t)
	memoryRepo := newMemoryRepo(t, 100)
	for _, repo := range []*Repo{sqliteRepo, memoryRepo} {
		if _, err := repo.InsertBatch(entries); err != nil {
			t.Fatalf("InsertBatch(%s): %v", repo.StorageMode(), err)
		}
	}

	walk := func(repo *Repo) []string {
		var ids []string
		var cursor *ListCursor
		for page := 0; page < len(entries)+2; page++ {
			rows, hasMore, next, err := repo.List(ListFilter{Limit: 2, Cursor: cursor})
			if err != nil {
				t.Fatalf("List(%s, page %d): %v", repo.StorageMode(), page, err)
			}
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			if !hasMore {
				return ids
			}
			if next == nil {
				t.Fatalf("List(%s, page %d): hasMore without cursor", repo.StorageMode(), page)
			}
			cursor = next
		}
		t.Fatalf("List(%s): pagination did not terminate", repo.StorageMode())
		return nil
	}

	want := walk(sqliteRepo)
	got := walk(memoryRepo)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paginated ids = %v, want %v", got, want)
	}
	if len(want) != len(entries) {
		t.Fatalf("paginated %d ids, want %d", len(want), len(entries))
	}
}

// TestRepo_StorageModeParity_GetByIDAndPayloads compares point lookups, including
// the "no payload row" and "missing id" cases.
func TestRepo_StorageModeParity_GetByIDAndPayloads(t *testing.T) {
	base := time.Now().UnixNano() - parityBaseOffset
	entries := parityEntries(base)

	sqliteRepo := newParitySQLiteRepo(t)
	memoryRepo := newMemoryRepo(t, 100)
	for _, repo := range []*Repo{sqliteRepo, memoryRepo} {
		if _, err := repo.InsertBatch(entries); err != nil {
			t.Fatalf("InsertBatch(%s): %v", repo.StorageMode(), err)
		}
	}

	ids := []string{"e0", "e1", "e3", "e4", "does-not-exist"}
	for _, id := range ids {
		t.Run("summary_"+id, func(t *testing.T) {
			want, err := sqliteRepo.GetByID(id)
			if err != nil {
				t.Fatalf("sqlite GetByID: %v", err)
			}
			got, err := memoryRepo.GetByID(id)
			if err != nil {
				t.Fatalf("memory GetByID: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("summary mismatch for %q\nsqlite: %+v\nmemory: %+v", id, want, got)
			}
		})

		t.Run("payload_"+id, func(t *testing.T) {
			want, err := sqliteRepo.GetPayloads(id)
			if err != nil {
				t.Fatalf("sqlite GetPayloads: %v", err)
			}
			got, err := memoryRepo.GetPayloads(id)
			if err != nil {
				t.Fatalf("memory GetPayloads: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("payload mismatch for %q\nsqlite: %+v\nmemory: %+v", id, want, got)
			}
		})
	}
}
