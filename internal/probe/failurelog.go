package probe

import (
	"log"
	"strings"
	"sync"
	"time"
)

// Failure log modes accepted by RESIN_PROBE_FAILURE_LOG / ProbeConfig.FailureLogMode.
const (
	// FailureLogModeLimited prints at most failureLogMaxLines probe failure
	// lines per failureLogWindow, reporting how many were suppressed.
	FailureLogModeLimited = "limited"
	// FailureLogModeOff drops probe failure lines entirely.
	FailureLogModeOff = "off"
	// FailureLogModeAll prints every probe failure line (debugging only).
	FailureLogModeAll = "all"
)

const (
	// failureLogWindow is the rolling window over which failure lines are capped.
	failureLogWindow = 30 * time.Second
	// failureLogMaxLines is the maximum number of failure lines per window.
	failureLogMaxLines = 5
)

type failureLogMode int

const (
	failureLogModeLimited failureLogMode = iota
	failureLogModeOff
	failureLogModeAll
)

// parseFailureLogMode normalizes the configured mode string.
// Unknown or empty values fall back to "limited", which is the safe default:
// a typo must not silently silence probe diagnostics, and it must not flood the
// log (and the disk behind it) either.
func parseFailureLogMode(raw string) failureLogMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case FailureLogModeOff:
		return failureLogModeOff
	case FailureLogModeAll:
		return failureLogModeAll
	default:
		return failureLogModeLimited
	}
}

// failureLogLimiter rate-limits repetitive probe failure log lines.
//
// A large pool of dead nodes produces one failure line per probe, i.e. tens of
// thousands of lines per probe cycle; writing them is itself disk I/O. The
// limiter allows a short burst per window, drops the rest, and reports the
// number of suppressed lines on the next emitted line.
type failureLogLimiter struct {
	mode         failureLogMode
	window       time.Duration
	maxPerWindow int

	// now and emit are injectable for deterministic tests.
	now  func() time.Time
	emit func(format string, args ...any)

	mu         sync.Mutex
	windowEnd  time.Time
	emitted    int
	suppressed int
}

// newFailureLogLimiter builds a limiter for the given mode string.
func newFailureLogLimiter(mode string) *failureLogLimiter {
	l := &failureLogLimiter{
		mode:         parseFailureLogMode(mode),
		window:       failureLogWindow,
		maxPerWindow: failureLogMaxLines,
		now:          time.Now,
		emit:         log.Printf,
	}
	l.windowEnd = l.now().Add(l.window)
	return l
}

// printf emits one failure log line subject to the configured mode.
// It never blocks longer than a mutex acquisition and never returns an error:
// logging must not influence probe error handling.
func (l *failureLogLimiter) printf(format string, args ...any) {
	if l == nil {
		log.Printf(format, args...)
		return
	}
	switch l.mode {
	case failureLogModeOff:
		return
	case failureLogModeAll:
		l.emit(format, args...)
		return
	}

	l.mu.Lock()
	now := l.now()
	if !now.Before(l.windowEnd) {
		l.windowEnd = now.Add(l.window)
		l.emitted = 0
	}
	if l.emitted >= l.maxPerWindow {
		l.suppressed++
		l.mu.Unlock()
		return
	}
	l.emitted++
	suppressed := l.suppressed
	l.suppressed = 0
	l.mu.Unlock()

	if suppressed > 0 {
		suffix := make([]any, 0, len(args)+1)
		suffix = append(suffix, args...)
		suffix = append(suffix, suppressed)
		l.emit(format+" (suppressed %d)", suffix...)
		return
	}
	l.emit(format, args...)
}

// SuppressedCount reports how many lines have been dropped since the last
// emitted line. Exposed for tests and diagnostics.
func (l *failureLogLimiter) SuppressedCount() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.suppressed
}
