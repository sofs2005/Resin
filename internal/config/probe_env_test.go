package config

import (
	"testing"
	"time"
)

// TestLoadEnvConfig_ProbeWriteAmplificationDefaults locks in the defaults for the
// knobs introduced by the disk-write reduction change.
func TestLoadEnvConfig_ProbeWriteAmplificationDefaults(t *testing.T) {
	setEnvs(t, requiredEnvs())

	cfg, err := LoadEnvConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertEqual(t, "ProbeFailureLogMode", cfg.ProbeFailureLogMode, "limited")
	assertEqual(t, "ProbeScanBatch", cfg.ProbeScanBatch, 0)
	assertEqual(t, "RequestLogRetentionHours", cfg.RequestLogRetentionHours, 168)
	assertEqual(t, "MaxLatencyTestInterval", cfg.MaxLatencyTestInterval, time.Hour)
	assertEqual(t, "MaxEgressTestInterval", cfg.MaxEgressTestInterval, 6*time.Hour)
	assertEqual(t, "MaxAuthorityLatencyTestInterval", cfg.MaxAuthorityLatencyTestInterval, 3*time.Hour)
}

// TestLoadEnvConfig_ProbeWriteAmplificationOverrides verifies the env overrides
// parse into typed values.
func TestLoadEnvConfig_ProbeWriteAmplificationOverrides(t *testing.T) {
	envs := requiredEnvs()
	envs["RESIN_PROBE_FAILURE_LOG"] = "  ALL "
	envs["RESIN_PROBE_SCAN_BATCH"] = "512"
	envs["RESIN_REQUEST_LOG_RETENTION_HOURS"] = "0"
	envs["RESIN_MAX_LATENCY_TEST_INTERVAL"] = "90m"
	envs["RESIN_MAX_EGRESS_TEST_INTERVAL"] = "12h"
	envs["RESIN_MAX_AUTHORITY_LATENCY_TEST_INTERVAL"] = "45m"
	setEnvs(t, envs)

	cfg, err := LoadEnvConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertEqual(t, "ProbeFailureLogMode", cfg.ProbeFailureLogMode, "all")
	assertEqual(t, "ProbeScanBatch", cfg.ProbeScanBatch, 512)
	assertEqual(t, "RequestLogRetentionHours", cfg.RequestLogRetentionHours, 0)
	assertEqual(t, "MaxLatencyTestInterval", cfg.MaxLatencyTestInterval, 90*time.Minute)
	assertEqual(t, "MaxEgressTestInterval", cfg.MaxEgressTestInterval, 12*time.Hour)
	assertEqual(t, "MaxAuthorityLatencyTestInterval", cfg.MaxAuthorityLatencyTestInterval, 45*time.Minute)
}

// TestLoadEnvConfig_ProbeFailureLogModeIsCaseInsensitive pins the normalisation
// contract; invalid values must be rejected instead of silently defaulting.
func TestLoadEnvConfig_InvalidProbeFailureLogMode(t *testing.T) {
	envs := requiredEnvs()
	envs["RESIN_PROBE_FAILURE_LOG"] = "verbose"
	setEnvs(t, envs)

	_, err := LoadEnvConfig()
	if err == nil {
		t.Fatal("expected error for invalid RESIN_PROBE_FAILURE_LOG")
	}
	assertContains(t, err.Error(), "RESIN_PROBE_FAILURE_LOG")
}

// TestLoadEnvConfig_ProbeIntervalMinimum mirrors the 30s floor enforced by the
// runtime config validator so seeding cannot produce an invalid config.
func TestLoadEnvConfig_ProbeIntervalMinimum(t *testing.T) {
	for _, key := range []string{
		"RESIN_MAX_LATENCY_TEST_INTERVAL",
		"RESIN_MAX_EGRESS_TEST_INTERVAL",
		"RESIN_MAX_AUTHORITY_LATENCY_TEST_INTERVAL",
	} {
		t.Run(key, func(t *testing.T) {
			envs := requiredEnvs()
			envs[key] = "5s"
			setEnvs(t, envs)

			_, err := LoadEnvConfig()
			if err == nil {
				t.Fatalf("expected error for %s below the 30s floor", key)
			}
			assertContains(t, err.Error(), key)
		})
	}
}

// TestLoadEnvConfig_NegativeRetentionRejected guards against silently disabling
// retention through a typo.
func TestLoadEnvConfig_NegativeRetentionRejected(t *testing.T) {
	envs := requiredEnvs()
	envs["RESIN_REQUEST_LOG_RETENTION_HOURS"] = "-1"
	setEnvs(t, envs)

	_, err := LoadEnvConfig()
	if err == nil {
		t.Fatal("expected error for negative retention")
	}
	assertContains(t, err.Error(), "RESIN_REQUEST_LOG_RETENTION_HOURS")
}
