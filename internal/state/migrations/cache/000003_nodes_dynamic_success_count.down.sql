-- SQLite cannot DROP COLUMN portably across older versions used in tests.
-- Down migration is intentionally a no-op; success_count is harmless if retained.
SELECT 1;
