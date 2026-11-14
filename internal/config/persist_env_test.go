package config

import (
	"testing"
)

// TestLoadEnvConfig_PersistenceModeDefaults locks in the memory-first defaults of
// the second disk-write reduction pass.
func TestLoadEnvConfig_PersistenceModeDefaults(t *testing.T) {
	setEnvs(t, requiredEnvs())

	cfg, err := LoadEnvConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.CachePersist {
		t.Fatal("CachePersist = true, want false (RESIN_CACHE_PERSIST default off)")
	}
	assertEqual(t, "RequestLogStorage", cfg.RequestLogStorage, StorageModeMemory)
	assertEqual(t, "RequestLogMemoryEntries", cfg.RequestLogMemoryEntries, 10000)
	assertEqual(t, "MetricsStorage", cfg.MetricsStorage, StorageModeMemory)
	assertEqual(t, "MetricsRetentionHours", cfg.MetricsRetentionHours, 24)
}

// TestLoadEnvConfig_PersistenceModeOverrides verifies the env overrides parse
// into typed values (case/space insensitive).
func TestLoadEnvConfig_PersistenceModeOverrides(t *testing.T) {
	envs := requiredEnvs()
	envs["RESIN_CACHE_PERSIST"] = " ON "
	envs["RESIN_REQUEST_LOG_STORAGE"] = "SQLite"
	envs["RESIN_REQUEST_LOG_MEMORY_ENTRIES"] = "250000"
	envs["RESIN_METRICS_STORAGE"] = "sqlite "
	envs["RESIN_METRICS_RETENTION_HOURS"] = "72"
	setEnvs(t, envs)

	cfg, err := LoadEnvConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !cfg.CachePersist {
		t.Fatal("CachePersist = false, want true")
	}
	assertEqual(t, "RequestLogStorage", cfg.RequestLogStorage, StorageModeSQLite)
	assertEqual(t, "RequestLogMemoryEntries", cfg.RequestLogMemoryEntries, 250000)
	assertEqual(t, "MetricsStorage", cfg.MetricsStorage, StorageModeSQLite)
	assertEqual(t, "MetricsRetentionHours", cfg.MetricsRetentionHours, 72)
}

// TestLoadEnvConfig_InvalidCachePersist pins that an illegal on/off value is
// rejected instead of silently picking a default.
func TestLoadEnvConfig_InvalidCachePersist(t *testing.T) {
	envs := requiredEnvs()
	envs["RESIN_CACHE_PERSIST"] = "yes"
	setEnvs(t, envs)

	if _, err := LoadEnvConfig(); err == nil {
		t.Fatal("expected error for invalid RESIN_CACHE_PERSIST")
	}
}

// TestLoadEnvConfig_InvalidStorageModes pins that unknown storage backends are
// rejected.
func TestLoadEnvConfig_InvalidStorageModes(t *testing.T) {
	cases := []struct{ key, value string }{
		{"RESIN_REQUEST_LOG_STORAGE", "disk"},
		{"RESIN_METRICS_STORAGE", "postgres"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			envs := requiredEnvs()
			envs[tc.key] = tc.value
			setEnvs(t, envs)
			if _, err := LoadEnvConfig(); err == nil {
				t.Fatalf("expected error for invalid %s", tc.key)
			}
		})
	}
}

// TestLoadEnvConfig_MemoryEntriesValidation pins the bounds of the ring buffer
// capacity knob.
func TestLoadEnvConfig_MemoryEntriesValidation(t *testing.T) {
	for _, value := range []string{"0", "-1", "1000001"} {
		t.Run(value, func(t *testing.T) {
			envs := requiredEnvs()
			envs["RESIN_REQUEST_LOG_MEMORY_ENTRIES"] = value
			setEnvs(t, envs)
			if _, err := LoadEnvConfig(); err == nil {
				t.Fatalf("expected error for RESIN_REQUEST_LOG_MEMORY_ENTRIES=%s", value)
			}
		})
	}
}

// TestLoadEnvConfig_MetricsRetentionValidation pins that the retention window
// must be positive.
func TestLoadEnvConfig_MetricsRetentionValidation(t *testing.T) {
	envs := requiredEnvs()
	envs["RESIN_METRICS_RETENTION_HOURS"] = "0"
	setEnvs(t, envs)
	if _, err := LoadEnvConfig(); err == nil {
		t.Fatal("expected error for RESIN_METRICS_RETENTION_HOURS=0")
	}
}

// TestLoadEnvConfig_StorageModeSwitchKeepsLegacyPaths verifies that switching a
// backend back to sqlite leaves the rest of the config untouched (the runtime
// still uses LogDir for request_logs-*.db / metrics.db).
func TestLoadEnvConfig_StorageModeSwitchKeepsLegacyPaths(t *testing.T) {
	envs := requiredEnvs()
	envs["RESIN_LOG_DIR"] = "/tmp/resin-logs"
	envs["RESIN_REQUEST_LOG_STORAGE"] = "sqlite"
	envs["RESIN_METRICS_STORAGE"] = "sqlite"
	setEnvs(t, envs)

	cfg, err := LoadEnvConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, "LogDir", cfg.LogDir, "/tmp/resin-logs")
	assertEqual(t, "RequestLogStorage", cfg.RequestLogStorage, StorageModeSQLite)
	assertEqual(t, "MetricsStorage", cfg.MetricsStorage, StorageModeSQLite)
}
