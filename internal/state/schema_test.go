package state

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// TestOpenDB_AppliesWriteAmplificationPragmas locks in the connection settings
// that keep the WAL files bounded (cache.db-wal previously reached 125MB).
func TestOpenDB_AppliesWriteAmplificationPragmas(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pragma.db")

	db, err := OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	cases := []struct {
		pragma string
		want   int
	}{
		{"wal_autocheckpoint", WALAutoCheckpointPages},
		{"journal_size_limit", JournalSizeLimitBytes},
		{"cache_size", -PageCacheKiB},
		{"temp_store", 2}, // 2 == MEMORY
	}
	for _, tc := range cases {
		got, err := SchemaPragmaInt(db, tc.pragma)
		if err != nil {
			t.Fatalf("read pragma %s: %v", tc.pragma, err)
		}
		if got != tc.want {
			t.Fatalf("PRAGMA %s = %d, want %d", tc.pragma, got, tc.want)
		}
	}
}

// TestTruncateWAL_ShrinksWALFile verifies that the WAL is checkpointed and
// truncated, not just checkpointed.
func TestTruncateWAL_ShrinksWALFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wallog.db")

	db, err := OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, blob BLOB)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 200; i++ {
		if _, err := db.Exec("INSERT INTO t (blob) VALUES (randomblob(4096))"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	walPath := path + "-wal"
	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if before.Size() == 0 {
		t.Fatal("expected a non-empty WAL before truncation")
	}

	if err := TruncateWAL(db); err != nil {
		t.Fatalf("TruncateWAL: %v", err)
	}

	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal after truncate: %v", err)
	}
	if after.Size() != 0 {
		t.Fatalf("WAL size after truncation = %d, want 0", after.Size())
	}

	// Rows must still be readable after the truncating checkpoint.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 200 {
		t.Fatalf("row count after truncation = %d, want 200", count)
	}
}

// TestTruncateWAL_NilDB ensures the helper is safe for callers without a handle.
func TestTruncateWAL_NilDB(t *testing.T) {
	var db *sql.DB
	if err := TruncateWAL(db); err != nil {
		t.Fatalf("TruncateWAL(nil) = %v, want nil", err)
	}
}
