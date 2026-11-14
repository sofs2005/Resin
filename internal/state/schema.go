// Package state implements the persistence layer: SQLite repos, StateEngine,
// dirty-set flush, consistency repair, and bootstrap.
package state

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

const (
	// WALAutoCheckpointPages bounds how many WAL pages may accumulate before
	// SQLite checkpoints automatically. The default (1000 pages) is kept explicit
	// so a large dirty-set transaction cannot leave the WAL unbounded.
	WALAutoCheckpointPages = 1000
	// JournalSizeLimitBytes caps how much the WAL file may keep after a
	// checkpoint. SQLite's default (-1) never shrinks the WAL: cache.db-wal had
	// grown to 125MB and stayed there. 64MB is a hard ceiling, not a target.
	JournalSizeLimitBytes = 64 * 1024 * 1024
	// PageCacheKiB is the per-connection page cache size, expressed in KiB via
	// SQLite's negative cache_size convention (16MB).
	PageCacheKiB = 16384
)

// OpenDB opens (or creates) a SQLite database at path with recommended pragmas:
// WAL journal mode, synchronous=NORMAL, foreign_keys=ON, busy_timeout=5000,
// plus write-amplification controls (wal_autocheckpoint, journal_size_limit,
// cache_size, temp_store).
func OpenDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db %s: %w", path, err)
	}

	// Single-writer: only one connection needed.
	db.SetMaxOpenConns(1)

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", WALAutoCheckpointPages),
		fmt.Sprintf("PRAGMA journal_size_limit=%d", JournalSizeLimitBytes),
		fmt.Sprintf("PRAGMA cache_size=-%d", PageCacheKiB),
		"PRAGMA temp_store=MEMORY",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("exec %q on %s: %w", p, path, err)
		}
	}

	return db, nil
}

// TruncateWAL checkpoints the write-ahead log of db and truncates the WAL file
// back to zero bytes. It is meant for low-frequency call sites (after a batch
// flush), because a truncating checkpoint blocks writers while it runs.
//
// A busy database is not an error worth surfacing as a hard failure: the
// checkpoint simply did not complete, and the next call will retry. Callers are
// expected to log the returned error and keep going.
func TruncateWAL(db *sql.DB) error {
	if db == nil {
		return nil
	}

	var (
		busy       int
		logFrames  int
		checkpoint int
	)
	if err := db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpoint); err != nil {
		return fmt.Errorf("wal_checkpoint(TRUNCATE): %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("wal_checkpoint(TRUNCATE): database busy (%d of %d frames checkpointed)", checkpoint, logFrames)
	}
	return nil
}

// SchemaPragmaInt reads a single-integer pragma value, used by tests and by
// diagnostics that need to confirm the connection settings.
func SchemaPragmaInt(db *sql.DB, pragma string) (int, error) {
	var value int
	if err := db.QueryRow("PRAGMA " + pragma).Scan(&value); err != nil {
		return 0, fmt.Errorf("read pragma %s: %w", pragma, err)
	}
	return value, nil
}

// InitDB executes raw DDL statements on the given database.
// Used by non-state SQLite stores (for example metrics and request logs).
func InitDB(db *sql.DB, ddl string) error {
	_, err := db.Exec(ddl)
	return err
}

func ensureTableColumn(db *sql.DB, table, column, columnDDL string) error {
	exists, err := hasTableColumn(db, table, column)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", table, columnDDL)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("migrate %s.%s: %w", table, column, err)
	}
	return nil
}

func hasTableColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, fmt.Errorf("inspect table %s: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid       int
			name      string
			colType   string
			notNull   int
			defaultV  sql.NullString
			primaryID int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultV, &primaryID); err != nil {
			return false, fmt.Errorf("scan table_info(%s): %w", table, err)
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate table_info(%s): %w", table, err)
	}
	return false, nil
}
