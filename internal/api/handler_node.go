package api

import (
	"cmp"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/service"
)

func nodeTagSortKey(n service.NodeSummary) string {
	if n.DisplayTag != "" {
		return n.DisplayTag
	}
	if len(n.Tags) == 0 {
		return ""
	}
	bestCreated := int64(math.MaxInt64)
	bestTag := ""
	for _, t := range n.Tags {
		if t.SubscriptionCreatedAtNs < bestCreated {
			bestCreated = t.SubscriptionCreatedAtNs
			bestTag = t.Tag
			continue
		}
		if t.SubscriptionCreatedAtNs == bestCreated && (bestTag == "" || t.Tag < bestTag) {
			bestTag = t.Tag
		}
	}
	return bestTag
}

func compareNodeSummaries(sortBy string, a, b service.NodeSummary) int {
	order := 0
	switch sortBy {
	case "created_at":
		order = strings.Compare(a.CreatedAt, b.CreatedAt)
	case "failure_count":
		order = cmp.Compare(a.FailureCount, b.FailureCount)
	case "region":
		order = strings.Compare(a.Region, b.Region)
	case "egress_ip":
		order = strings.Compare(a.EgressIP, b.EgressIP)
	case "reference_latency_ms":
		// Callers must handle missing values before flipping sort order.
		// Both present is guaranteed here for meaningful numeric compare.
		if a.ReferenceLatencyMs != nil && b.ReferenceLatencyMs != nil {
			order = cmp.Compare(*a.ReferenceLatencyMs, *b.ReferenceLatencyMs)
		}
	case "last_latency_probe_attempt":
		order = strings.Compare(a.LastLatencyProbeAttempt, b.LastLatencyProbeAttempt)
	default:
		order = strings.Compare(nodeTagSortKey(a), nodeTagSortKey(b))
	}
	if order != 0 {
		return order
	}
	return strings.Compare(a.NodeHash, b.NodeHash)
}

// compareMissingLast keeps empty/unset sortable values at the end for both asc and desc.
// Returns handled=true when one side is missing and the other is not.
func compareMissingLast(sortBy string, a, b service.NodeSummary) (int, bool) {
	var aMiss, bMiss bool
	switch sortBy {
	case "reference_latency_ms":
		aMiss = a.ReferenceLatencyMs == nil
		bMiss = b.ReferenceLatencyMs == nil
	case "egress_ip":
		aMiss = a.EgressIP == ""
		bMiss = b.EgressIP == ""
	case "last_latency_probe_attempt":
		aMiss = a.LastLatencyProbeAttempt == ""
		bMiss = b.LastLatencyProbeAttempt == ""
	default:
		return 0, false
	}
	if aMiss == bMiss {
		return 0, false
	}
	if aMiss {
		return 1, true
	}
	return -1, true
}

func sortNodeSummaries(nodes []service.NodeSummary, sorting Sorting) {
	slices.SortStableFunc(nodes, func(a, b service.NodeSummary) int {
		if order, handled := compareMissingLast(sorting.SortBy, a, b); handled {
			return order
		}
		return applySortOrder(compareNodeSummaries(sorting.SortBy, a, b), sorting.SortOrder)
	})
}

type nodeListPageResponse struct {
	Items                  []service.NodeSummary `json:"items"`
	Total                  int                   `json:"total"`
	Limit                  int                   `json:"limit"`
	Offset                 int                   `json:"offset"`
	UniqueEgressIPs        int                   `json:"unique_egress_ips"`
	UniqueHealthyEgressIPs int                   `json:"unique_healthy_egress_ips"`
}

func countUniqueEgressIPs(nodes []service.NodeSummary) int {
	seen := make(map[string]struct{})
	for _, n := range nodes {
		if n.EgressIP == "" {
			continue
		}
		seen[n.EgressIP] = struct{}{}
	}
	return len(seen)
}

func countUniqueHealthyAndEnabledEgressIPs(nodes []service.NodeSummary) int {
	seen := make(map[string]struct{})
	for _, n := range nodes {
		if n.EgressIP == "" {
			continue
		}
		if !n.IsHealthyAndEnabled() {
			continue
		}
		seen[n.EgressIP] = struct{}{}
	}
	return len(seen)
}

// HandleListNodes returns a handler for GET /api/v1/nodes.
func HandleListNodes(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filters := service.NodeFilters{}

		platformID, ok := parseOptionalUUIDQuery(w, r, "platform_id", "platform_id")
		if !ok {
			return
		}
		filters.PlatformID = platformID

		subscriptionID, ok := parseOptionalUUIDQuery(w, r, "subscription_id", "subscription_id")
		if !ok {
			return
		}
		filters.SubscriptionID = subscriptionID

		if v := q.Get("region"); v != "" {
			filters.Region = &v
		}
		if v := q.Get("egress_ip"); v != "" {
			filters.EgressIP = &v
		}
		if v := strings.TrimSpace(q.Get("tag_keyword")); v != "" {
			filters.TagKeyword = &v
		}

		circuitOpen, ok := parseBoolQueryOrWriteInvalid(w, r, "circuit_open")
		if !ok {
			return
		}
		filters.CircuitOpen = circuitOpen

		hasOutbound, ok := parseBoolQueryOrWriteInvalid(w, r, "has_outbound")
		if !ok {
			return
		}
		filters.HasOutbound = hasOutbound

		enabled, ok := parseBoolQueryOrWriteInvalid(w, r, "enabled")
		if !ok {
			return
		}
		filters.Enabled = enabled

		if v := strings.TrimSpace(q.Get("health_status")); v != "" {
			switch strings.ToLower(v) {
			case "healthy", "available", "unhealthy":
				lower := strings.ToLower(v)
				filters.HealthStatus = &lower
			default:
				writeInvalidArgument(w, "health_status: must be healthy, available, or unhealthy")
				return
			}
		}

		if v := q.Get("probed_since"); v != "" {
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				writeInvalidArgument(w, "probed_since: invalid RFC3339 timestamp")
				return
			}
			filters.ProbedSince = &t
		}

		nodes, err := cp.ListNodes(filters)
		if err != nil {
			writeServiceError(w, err)
			return
		}

		sorting, ok := parseSortingOrWriteInvalid(w, r, []string{
			"tag",
			"created_at",
			"failure_count",
			"region",
			"egress_ip",
			"reference_latency_ms",
			"last_latency_probe_attempt",
		}, "reference_latency_ms", "asc")
		if !ok {
			return
		}
		sortNodeSummaries(nodes, sorting)

		pg, ok := parsePaginationOrWriteInvalid(w, r)
		if !ok {
			return
		}
		WriteJSON(w, http.StatusOK, nodeListPageResponse{
			Items:                  PaginateSlice(nodes, pg),
			Total:                  len(nodes),
			Limit:                  pg.Limit,
			Offset:                 pg.Offset,
			UniqueEgressIPs:        countUniqueEgressIPs(nodes),
			UniqueHealthyEgressIPs: countUniqueHealthyAndEnabledEgressIPs(nodes),
		})
	}
}

// HandleGetNode returns a handler for GET /api/v1/nodes/{hash}.
func HandleGetNode(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash := PathParam(r, "hash")
		n, err := cp.GetNode(hash)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, n)
	}
}

// HandleProbeEgress returns a handler for POST /api/v1/nodes/{hash}/actions/probe-egress.
func HandleProbeEgress(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash := PathParam(r, "hash")
		result, err := cp.ProbeEgress(hash)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, result)
	}
}

// HandleProbeLatency returns a handler for POST /api/v1/nodes/{hash}/actions/probe-latency.
func HandleProbeLatency(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash := PathParam(r, "hash")
		result, err := cp.ProbeLatency(hash)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, result)
	}
}
