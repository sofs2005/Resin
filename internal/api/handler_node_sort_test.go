package api

import (
	"testing"

	"github.com/Resinat/Resin/internal/service"
)

func TestSortNodeSummaries_ReferenceLatencyEgressAndProbe(t *testing.T) {
	low := 10.0
	mid := 50.0
	high := 100.0

	nodes := []service.NodeSummary{
		{NodeHash: "missing-latency", EgressIP: "203.0.113.30", LastLatencyProbeAttempt: "2026-01-03T00:00:00Z"},
		{NodeHash: "high", ReferenceLatencyMs: &high, EgressIP: "203.0.113.20", LastLatencyProbeAttempt: "2026-01-02T00:00:00Z"},
		{NodeHash: "low", ReferenceLatencyMs: &low, EgressIP: "", LastLatencyProbeAttempt: ""},
		{NodeHash: "mid", ReferenceLatencyMs: &mid, EgressIP: "203.0.113.10", LastLatencyProbeAttempt: "2026-01-01T00:00:00Z"},
	}

	sortNodeSummaries(nodes, Sorting{SortBy: "reference_latency_ms", SortOrder: "asc"})
	wantLatencyAsc := []string{"low", "mid", "high", "missing-latency"}
	for i, want := range wantLatencyAsc {
		if nodes[i].NodeHash != want {
			t.Fatalf("reference_latency_ms asc[%d]=%s, want %s", i, nodes[i].NodeHash, want)
		}
	}

	sortNodeSummaries(nodes, Sorting{SortBy: "reference_latency_ms", SortOrder: "desc"})
	wantLatencyDesc := []string{"high", "mid", "low", "missing-latency"}
	for i, want := range wantLatencyDesc {
		if nodes[i].NodeHash != want {
			t.Fatalf("reference_latency_ms desc[%d]=%s, want %s", i, nodes[i].NodeHash, want)
		}
	}

	sortNodeSummaries(nodes, Sorting{SortBy: "egress_ip", SortOrder: "asc"})
	wantEgressAsc := []string{"mid", "high", "missing-latency", "low"}
	for i, want := range wantEgressAsc {
		if nodes[i].NodeHash != want {
			t.Fatalf("egress_ip asc[%d]=%s, want %s", i, nodes[i].NodeHash, want)
		}
	}

	sortNodeSummaries(nodes, Sorting{SortBy: "last_latency_probe_attempt", SortOrder: "asc"})
	wantProbeAsc := []string{"mid", "high", "missing-latency", "low"}
	for i, want := range wantProbeAsc {
		if nodes[i].NodeHash != want {
			t.Fatalf("last_latency_probe_attempt asc[%d]=%s, want %s", i, nodes[i].NodeHash, want)
		}
	}
}
