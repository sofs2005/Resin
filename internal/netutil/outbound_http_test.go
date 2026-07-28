package netutil

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/testutil"
)

func TestHTTPGetViaOutbound_RequireStatusOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: true,
	})
	if err == nil {
		t.Fatal("expected non-2xx status to return error")
	}
	if !strings.Contains(err.Error(), "unexpected status 404") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

// generate_204-style probes return 204 No Content; RequireStatusOK must accept any 2xx.
func TestHTTPGetViaOutbound_RequireStatusOK_Accepts204(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	body, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: true,
	})
	if err != nil {
		t.Fatalf("expected 204 to pass RequireStatusOK, got: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("expected empty body for 204, got %q", string(body))
	}
}

func TestHTTPGetViaOutbound_AllowNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("probe-body"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	body, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: false,
	})
	if err != nil {
		t.Fatalf("expected non-200 response to pass through, got: %v", err)
	}
	if string(body) != "probe-body" {
		t.Fatalf("unexpected body %q", string(body))
	}
}

// TestHTTPGetViaOutbound_HTTPSRequireStatusOK exercises the production probe path:
// outbound DialContext → TLS handshake → HTTPS GET with RequireStatusOK.
// For HTTP-proxy outbounds, sing-box performs CONNECT under DialContext; this
// test validates the shared HTTPS client stack that all probes rely on.
func TestHTTPGetViaOutbound_HTTPSRequireStatusOK(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method: got %s, want GET", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ip=203.0.113.9\nloc=us\n"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, latency, err := HTTPGetViaOutbound(ctx, ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: true,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	})
	if err != nil {
		t.Fatalf("HTTPS GET via outbound failed: %v", err)
	}
	if !strings.Contains(string(body), "ip=203.0.113.9") {
		t.Fatalf("unexpected body %q", string(body))
	}
	// TLSHandshakeDone should record a positive latency sample on success.
	if latency <= 0 {
		t.Fatalf("expected positive TLS handshake latency, got %v", latency)
	}
}

func TestHTTPGetViaOutbound_HTTPSNon200Rejected(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("unavailable"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}

	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: true,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	})
	if err == nil {
		t.Fatal("expected non-200 HTTPS status to return error")
	}
	if !strings.Contains(err.Error(), "unexpected status 503") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

func TestConnCloseHook_CloseIsIdempotentAndConcurrentSafe(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	var onCloseCount atomic.Int32
	hook := &connCloseHook{
		Conn: client,
		onClose: func() {
			onCloseCount.Add(1)
		},
	}

	const closers = 32
	var wg sync.WaitGroup
	wg.Add(closers)
	for i := 0; i < closers; i++ {
		go func() {
			defer wg.Done()
			_ = hook.Close()
		}()
	}
	wg.Wait()

	if got := onCloseCount.Load(); got != 1 {
		t.Fatalf("onClose called %d times, want 1", got)
	}
}
