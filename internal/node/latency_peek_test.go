package node

import (
	"testing"
	"time"
)

// TestLatencyTable_PeekDomainStats_DoesNotTouchLRU verifies that peeking leaves
// the LRU access order untouched, so persistence-gating reads cannot change which
// regular domain gets evicted.
func TestLatencyTable_PeekDomainStats_DoesNotTouchLRU(t *testing.T) {
	lt := NewLatencyTable(2)

	lt.Update("a.com", 10*time.Millisecond, 30*time.Second)
	lt.Update("b.com", 20*time.Millisecond, 30*time.Second)

	// Peeking repeatedly must not make a.com "recent"? Both entries were just
	// written, so force the access-touch throttle to expire before peeking.
	time.Sleep(latencyReadTouchMinInterval + 10*time.Millisecond)

	for i := 0; i < 3; i++ {
		stats, ok := lt.PeekDomainStats("a.com")
		if !ok {
			t.Fatal("a.com should be present")
		}
		if stats.Ewma != 10*time.Millisecond {
			t.Fatalf("a.com Ewma = %v, want 10ms", stats.Ewma)
		}
	}

	// a.com keeps its old access timestamp, so inserting c.com evicts a.com.
	lt.Update("c.com", 30*time.Millisecond, 30*time.Second)

	if _, ok := lt.PeekDomainStats("a.com"); ok {
		t.Fatal("a.com should have been evicted: peeks must not refresh LRU")
	}
	if _, ok := lt.PeekDomainStats("b.com"); !ok {
		t.Fatal("b.com should still be present")
	}
	if _, ok := lt.PeekDomainStats("c.com"); !ok {
		t.Fatal("c.com should be present")
	}
}

// TestLatencyTable_GetDomainStats_TouchesLRU is the contrasting case: reads
// through GetDomainStats do refresh the access timestamp.
func TestLatencyTable_GetDomainStats_TouchesLRU(t *testing.T) {
	lt := NewLatencyTable(2)

	lt.Update("a.com", 10*time.Millisecond, 30*time.Second)
	lt.Update("b.com", 20*time.Millisecond, 30*time.Second)
	time.Sleep(latencyReadTouchMinInterval + 10*time.Millisecond)

	if _, ok := lt.GetDomainStats("a.com"); !ok {
		t.Fatal("a.com should be present")
	}

	// a.com is now the most recently accessed entry, so b.com is evicted.
	lt.Update("c.com", 30*time.Millisecond, 30*time.Second)

	if _, ok := lt.PeekDomainStats("a.com"); !ok {
		t.Fatal("a.com should survive: GetDomainStats refreshed its LRU timestamp")
	}
	if _, ok := lt.PeekDomainStats("b.com"); ok {
		t.Fatal("b.com should have been evicted")
	}
}

// TestLatencyTable_PeekDomainStats_Missing covers the empty lookup.
func TestLatencyTable_PeekDomainStats_Missing(t *testing.T) {
	lt := NewLatencyTable(4)
	if _, ok := lt.PeekDomainStats("nope.com"); ok {
		t.Fatal("missing domain must not be reported as found")
	}

	lt.Update("nope.com", 5*time.Millisecond, 30*time.Second)
	stats, ok := lt.PeekDomainStats("nope.com")
	if !ok {
		t.Fatal("domain should be present after update")
	}
	if stats.Ewma != 5*time.Millisecond {
		t.Fatalf("Ewma = %v, want 5ms", stats.Ewma)
	}
	if stats.LastUpdated.IsZero() {
		t.Fatal("LastUpdated should be populated")
	}
}
