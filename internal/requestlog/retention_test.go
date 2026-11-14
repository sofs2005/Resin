package requestlog

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/proxy"
)

// makeFakeShard writes a placeholder shard file (plus WAL/SHM siblings) with the
// given creation timestamp embedded in its name. Retention cleanup only stats
// and removes shard files, so the content does not matter.
func makeFakeShard(t *testing.T, dir string, createdMs int64) string {
	t.Helper()
	path := filepath.Join(dir, "request_logs-"+strconv.FormatInt(createdMs, 10)+".db")
	if err := os.WriteFile(path, []byte("stub"), 0o600); err != nil {
		t.Fatalf("write shard %s: %v", path, err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.WriteFile(path+suffix, []byte("stub"), 0o600); err != nil {
			t.Fatalf("write shard sibling %s%s: %v", path, suffix, err)
		}
	}
	return path
}

// TestRepo_CleanupExpired_RemovesOldShards verifies that a shard older than the
// retention window is deleted together with its WAL/SHM siblings on startup,
// while recent shards and the active shard survive.
func TestRepo_CleanupExpired_RemovesOldShards(t *testing.T) {
	dir := t.TempDir()

	// Create the active shard first; it must survive every cleanup pass.
	repo := NewRepo(dir, 1024*1024, 10)
	repo.SetRetention(24 * time.Hour)
	if err := repo.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	activePath := repo.activePath
	if err := repo.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	old := makeFakeShard(t, dir, time.Now().Add(-72*time.Hour).UnixMilli())

	repo2 := NewRepo(dir, 1024*1024, 10)
	repo2.SetRetention(24 * time.Hour)
	if err := repo2.Open(); err != nil {
		t.Fatalf("Open(2): %v", err)
	}
	defer repo2.Close()

	for _, path := range []string{old, old + "-wal", old + "-shm"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s should have been removed by retention cleanup", path)
		}
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("active shard must survive retention cleanup: %v", err)
	}
}

// TestRemoveExpiredFiles_KeepsRecentShards exercises the file-level rule without
// opening any of the shards.
func TestRemoveExpiredFiles_KeepsRecentShards(t *testing.T) {
	dir := t.TempDir()
	repo := NewRepo(dir, 1024*1024, 10)
	repo.SetRetention(24 * time.Hour)

	old := makeFakeShard(t, dir, time.Now().Add(-72*time.Hour).UnixMilli())
	recent := makeFakeShard(t, dir, time.Now().Add(-1*time.Hour).UnixMilli())

	if err := repo.removeExpiredFiles(time.Now()); err != nil {
		t.Fatalf("removeExpiredFiles: %v", err)
	}

	for _, path := range []string{old, old + "-wal", old + "-shm"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s should have been removed", path)
		}
	}
	for _, path := range []string{recent, recent + "-wal", recent + "-shm"} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s should survive: %v", path, err)
		}
	}
}

// TestRepo_CleanupExpired_DeletesRowsInActiveShard verifies the row-level half of
// the retention window, including the payload cascade.
func TestRepo_CleanupExpired_DeletesRowsInActiveShard(t *testing.T) {
	dir := t.TempDir()
	repo := NewRepo(dir, 64*1024*1024, 5)
	repo.SetRetention(24 * time.Hour)
	if err := repo.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer repo.Close()

	now := time.Now()
	entries := []proxy.RequestLogEntry{
		{
			ID:          "old-row",
			StartedAtNs: now.Add(-48 * time.Hour).UnixNano(),
			ProxyType:   proxy.ProxyTypeForward,
			TargetHost:  "old.example.com",
			ReqHeaders:  []byte("old-headers"),
			RespHeaders: []byte("old-resp"),
		},
		{
			ID:          "new-row",
			StartedAtNs: now.Add(-1 * time.Hour).UnixNano(),
			ProxyType:   proxy.ProxyTypeForward,
			TargetHost:  "new.example.com",
			ReqHeaders:  []byte("new-headers"),
			RespHeaders: []byte("new-resp"),
		},
	}
	if n, err := repo.InsertBatch(entries); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	} else if n != 2 {
		t.Fatalf("InsertBatch inserted %d rows, want 2", n)
	}

	if err := repo.CleanupExpired(); err != nil {
		t.Fatalf("CleanupExpired: %v", err)
	}

	if got := countRows(t, repo, "request_logs"); got != 1 {
		t.Fatalf("request_logs rows after cleanup = %d, want 1", got)
	}
	if got := countRows(t, repo, "request_log_payloads"); got != 1 {
		t.Fatalf("request_log_payloads rows after cleanup = %d, want 1 (cascade)", got)
	}

	var id string
	if err := repo.activeDB.QueryRow("SELECT id FROM request_logs").Scan(&id); err != nil {
		t.Fatalf("select remaining id: %v", err)
	}
	if id != "new-row" {
		t.Fatalf("surviving row = %q, want new-row", id)
	}
}

// TestRepo_CleanupExpired_DisabledWhenRetentionZero locks in that a zero window
// keeps everything (opt-out behaviour).
func TestRepo_CleanupExpired_DisabledWhenRetentionZero(t *testing.T) {
	dir := t.TempDir()
	repo := NewRepo(dir, 64*1024*1024, 5)
	// Retention unset (0).
	if err := repo.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer repo.Close()

	old := makeFakeShard(t, dir, time.Now().Add(-1000*time.Hour).UnixMilli())

	if err := repo.CleanupExpired(); err != nil {
		t.Fatalf("CleanupExpired: %v", err)
	}
	if _, err := os.Stat(old); err != nil {
		t.Fatalf("shard must survive when retention is disabled: %v", err)
	}
}

func countRows(t *testing.T, repo *Repo, table string) int {
	t.Helper()
	var count int
	if err := repo.activeDB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

// TestDBFileTimestampMs covers the shard-name parser used by retention cleanup.
func TestDBFileTimestampMs(t *testing.T) {
	dir := t.TempDir()
	valid := filepath.Join(dir, "request_logs-1700000000000.db")
	if ms, ok := dbFileTimestampMs(valid); !ok || ms != 1700000000000 {
		t.Fatalf("dbFileTimestampMs(%q) = (%d,%v), want (1700000000000,true)", valid, ms, ok)
	}
	for _, path := range []string{
		filepath.Join(dir, "cache.db"),
		filepath.Join(dir, "request_logs-abc.db"),
		filepath.Join(dir, "request_logs-.db"),
	} {
		if _, ok := dbFileTimestampMs(path); ok {
			t.Fatalf("dbFileTimestampMs(%q) should not parse", path)
		}
	}
}

// TestRepo_TruncateWAL confirms the WAL of the active shard is truncated after a
// flush and that rows stay readable afterwards.
func TestRepo_TruncateWAL(t *testing.T) {
	dir := t.TempDir()
	repo := NewRepo(dir, 64*1024*1024, 5)
	if err := repo.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer repo.Close()

	now := time.Now()
	entries := make([]proxy.RequestLogEntry, 0, 50)
	for i := 0; i < 50; i++ {
		entries = append(entries, proxy.RequestLogEntry{
			ID:          "wal-" + strconv.Itoa(i),
			StartedAtNs: now.UnixNano(),
			ProxyType:   proxy.ProxyTypeForward,
			TargetHost:  "wal.example.com",
			ReqBody:     []byte("payload-payload-payload"),
		})
	}
	if _, err := repo.InsertBatch(entries); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	if err := repo.TruncateWAL(); err != nil {
		t.Fatalf("TruncateWAL: %v", err)
	}
	walPath := repo.activePath + "-wal"
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("wal size after truncation = %d, want 0", info.Size())
	}

	// Second call inside the throttle window is a no-op and must not error.
	if err := repo.TruncateWAL(); err != nil {
		t.Fatalf("throttled TruncateWAL: %v", err)
	}

	if got := countRows(t, repo, "request_logs"); got != len(entries) {
		t.Fatalf("row count = %d, want %d", got, len(entries))
	}
}
