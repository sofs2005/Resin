package outbound

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

func TestTryBuildSimpleOutbound_Routing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		raw      string
		handled  bool
		wantErr  bool
		wantType string
		wantTag  string
	}{
		{
			name:     "http ok",
			raw:      `{"type":"http","tag":"h1","server":"127.0.0.1","server_port":8080}`,
			handled:  true,
			wantType: "http",
			wantTag:  "h1",
		},
		{
			name:     "socks ok with auth",
			raw:      `{"type":"socks","server":"10.0.0.1","server_port":1080,"username":"u","password":"p"}`,
			handled:  true,
			wantType: "socks",
			wantTag:  "socks",
		},
		{
			name:     "port alias",
			raw:      `{"type":"http","server":"127.0.0.1","port":3128}`,
			handled:  true,
			wantType: "http",
			wantTag:  "http",
		},
		{
			name:    "ss falls through",
			raw:     `{"type":"shadowsocks","server":"1.2.3.4","server_port":443}`,
			handled: false,
		},
		{
			name:    "vmess falls through",
			raw:     `{"type":"vmess","server":"1.2.3.4","server_port":443}`,
			handled: false,
		},
		{
			name:    "http missing server",
			raw:     `{"type":"http","server_port":8080}`,
			handled: true,
			wantErr: true,
		},
		{
			name:    "socks4 rejected",
			raw:     `{"type":"socks","server":"127.0.0.1","server_port":1080,"version":"4"}`,
			handled: true,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ob, handled, err := tryBuildSimpleOutbound(json.RawMessage(tc.raw))
			if handled != tc.handled {
				t.Fatalf("handled=%v want %v (err=%v)", handled, tc.handled, err)
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !tc.handled {
				if ob != nil {
					t.Fatal("expected nil outbound for unhandled type")
				}
				return
			}
			if ob.Type() != tc.wantType {
				t.Fatalf("type=%q want %q", ob.Type(), tc.wantType)
			}
			if ob.Tag() != tc.wantTag {
				t.Fatalf("tag=%q want %q", ob.Tag(), tc.wantTag)
			}
		})
	}
}

type fallbackBuilder struct {
	calls atomic.Int32
	err   error
	ob    adapter.Outbound
}

func (f *fallbackBuilder) Build(json.RawMessage) (adapter.Outbound, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	return f.ob, nil
}

func TestDualOutboundBuilder_RoutesHTTPToSimple(t *testing.T) {
	t.Parallel()

	fb := &fallbackBuilder{err: fmt.Errorf("fallback should not be called for http")}
	b := NewDualOutboundBuilder(fb)

	ob, err := b.Build(json.RawMessage(`{"type":"http","server":"127.0.0.1","server_port":9}`))
	if err != nil {
		t.Fatalf("Build http: %v", err)
	}
	if ob.Type() != "http" {
		t.Fatalf("type=%q", ob.Type())
	}
	if fb.calls.Load() != 0 {
		t.Fatalf("fallback called %d times", fb.calls.Load())
	}
}

func TestDualOutboundBuilder_FallsThroughComplex(t *testing.T) {
	t.Parallel()

	fb := &fallbackBuilder{err: fmt.Errorf("ss needs sing-box")}
	b := NewDualOutboundBuilder(fb)

	_, err := b.Build(json.RawMessage(`{"type":"shadowsocks","server":"1.1.1.1","server_port":443}`))
	if err == nil {
		t.Fatal("expected fallback error for ss")
	}
	if fb.calls.Load() != 1 {
		t.Fatalf("fallback calls=%d want 1", fb.calls.Load())
	}
	if !strings.Contains(err.Error(), "ss needs sing-box") {
		t.Fatalf("err=%v", err)
	}
}

func TestDualOutboundBuilder_SimpleConfigErrorNoFallback(t *testing.T) {
	t.Parallel()

	fb := &fallbackBuilder{err: fmt.Errorf("should not run")}
	b := NewDualOutboundBuilder(fb)

	_, err := b.Build(json.RawMessage(`{"type":"http","server_port":8080}`))
	if err == nil {
		t.Fatal("expected config error")
	}
	if fb.calls.Load() != 0 {
		t.Fatalf("fallback should not run on simple config error, calls=%d", fb.calls.Load())
	}
}

// startHTTPConnectProxy starts a minimal HTTP CONNECT proxy. On CONNECT it
// dials the requested host and pipes bytes both ways. Optional basic auth.
func startHTTPConnectProxy(t *testing.T, user, pass string) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodConnect {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if user != "" || pass != "" {
				const prefix = "Basic "
				h := r.Header.Get("Proxy-Authorization")
				if !strings.HasPrefix(h, prefix) {
					w.Header().Set("Proxy-Authenticate", "Basic")
					http.Error(w, "auth required", http.StatusProxyAuthRequired)
					return
				}
				decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(h, prefix))
				if err != nil || string(decoded) != user+":"+pass {
					http.Error(w, "forbidden", http.StatusForbidden)
					return
				}
			}
			dest, err := net.DialTimeout("tcp", r.Host, 3*time.Second)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			hj, ok := w.(http.Hijacker)
			if !ok {
				_ = dest.Close()
				http.Error(w, "no hijack", http.StatusInternalServerError)
				return
			}
			client, _, err := hj.Hijack()
			if err != nil {
				_ = dest.Close()
				return
			}
			_, _ = client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
			go func() { _, _ = io.Copy(dest, client); _ = dest.Close(); _ = client.Close() }()
			_, _ = io.Copy(client, dest)
			_ = dest.Close()
			_ = client.Close()
		}),
	}
	go func() { _ = srv.Serve(ln) }()

	return ln.Addr().String(), func() {
		_ = srv.Close()
		_ = ln.Close()
	}
}

// startSOCKS5Proxy starts a minimal SOCKS5 (no-auth or user/pass) CONNECT proxy.
func startSOCKS5Proxy(t *testing.T, user, pass string) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					return
				}
			}
			go handleSOCKS5(conn, user, pass)
		}
	}()
	return ln.Addr().String(), func() {
		close(done)
		_ = ln.Close()
	}
}

func handleSOCKS5(conn net.Conn, user, pass string) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil || hdr[0] != 0x05 {
		return
	}
	methods := make([]byte, int(hdr[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	needAuth := user != "" || pass != ""
	chosen := byte(0xff)
	for _, m := range methods {
		if !needAuth && m == 0x00 {
			chosen = 0x00
			break
		}
		if needAuth && m == 0x02 {
			chosen = 0x02
			break
		}
	}
	if _, err := conn.Write([]byte{0x05, chosen}); err != nil || chosen == 0xff {
		return
	}
	if chosen == 0x02 {
		var ver [1]byte
		if _, err := io.ReadFull(conn, ver[:]); err != nil || ver[0] != 0x01 {
			return
		}
		var ulen [1]byte
		if _, err := io.ReadFull(conn, ulen[:]); err != nil {
			return
		}
		ubuf := make([]byte, int(ulen[0]))
		if _, err := io.ReadFull(conn, ubuf); err != nil {
			return
		}
		var plen [1]byte
		if _, err := io.ReadFull(conn, plen[:]); err != nil {
			return
		}
		pbuf := make([]byte, int(plen[0]))
		if _, err := io.ReadFull(conn, pbuf); err != nil {
			return
		}
		status := byte(0x00)
		if string(ubuf) != user || string(pbuf) != pass {
			status = 0x01
		}
		_, _ = conn.Write([]byte{0x01, status})
		if status != 0x00 {
			return
		}
	}

	var req [4]byte
	if _, err := io.ReadFull(conn, req[:]); err != nil || req[0] != 0x05 || req[1] != 0x01 {
		return
	}
	var host string
	switch req[3] {
	case 0x01:
		var ip [4]byte
		if _, err := io.ReadFull(conn, ip[:]); err != nil {
			return
		}
		host = net.IP(ip[:]).String()
	case 0x03:
		var n [1]byte
		if _, err := io.ReadFull(conn, n[:]); err != nil {
			return
		}
		dom := make([]byte, int(n[0]))
		if _, err := io.ReadFull(conn, dom); err != nil {
			return
		}
		host = string(dom)
	case 0x04:
		var ip [16]byte
		if _, err := io.ReadFull(conn, ip[:]); err != nil {
			return
		}
		host = net.IP(ip[:]).String()
	default:
		return
	}
	var portBuf [2]byte
	if _, err := io.ReadFull(conn, portBuf[:]); err != nil {
		return
	}
	port := binary.BigEndian.Uint16(portBuf[:])
	dest, err := net.DialTimeout("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)), 3*time.Second)
	if err != nil {
		// general failure
		_, _ = conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer dest.Close()
	// success, bind 0.0.0.0:0
	_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	_ = conn.SetDeadline(time.Time{})
	go func() { _, _ = io.Copy(dest, conn); _ = dest.Close(); _ = conn.Close() }()
	_, _ = io.Copy(conn, dest)
}

func TestSimpleOutbound_HTTPConnect_EndToEnd(t *testing.T) {
	t.Parallel()

	// Target origin that returns a fixed body.
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok-via-http-proxy")
	}))
	t.Cleanup(origin.Close)

	proxyAddr, closeProxy := startHTTPConnectProxy(t, "alice", "secret")
	t.Cleanup(closeProxy)

	host, portStr, err := net.SplitHostPort(proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	raw, _ := json.Marshal(map[string]any{
		"type":        "http",
		"tag":         "test-http",
		"server":      host,
		"server_port": port,
		"username":    "alice",
		"password":    "secret",
	})
	ob, handled, err := tryBuildSimpleOutbound(raw)
	if !handled || err != nil {
		t.Fatalf("build: handled=%v err=%v", handled, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body, _, err := netutil.HTTPGetViaOutbound(ctx, ob, origin.URL, netutil.OutboundHTTPOptions{
		RequireStatusOK: true,
	})
	if err != nil {
		t.Fatalf("HTTPGetViaOutbound: %v", err)
	}
	if string(body) != "ok-via-http-proxy" {
		t.Fatalf("body=%q", body)
	}
}

func TestSimpleOutbound_SOCKS5_EndToEnd(t *testing.T) {
	t.Parallel()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok-via-socks")
	}))
	t.Cleanup(origin.Close)

	proxyAddr, closeProxy := startSOCKS5Proxy(t, "bob", "tok")
	t.Cleanup(closeProxy)

	host, portStr, err := net.SplitHostPort(proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	raw, _ := json.Marshal(map[string]any{
		"type":        "socks",
		"server":      host,
		"server_port": port,
		"username":    "bob",
		"password":    "tok",
		"version":     "5",
	})
	ob, handled, err := tryBuildSimpleOutbound(raw)
	if !handled || err != nil {
		t.Fatalf("build: handled=%v err=%v", handled, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body, _, err := netutil.HTTPGetViaOutbound(ctx, ob, origin.URL, netutil.OutboundHTTPOptions{
		RequireStatusOK: true,
	})
	if err != nil {
		t.Fatalf("HTTPGetViaOutbound: %v", err)
	}
	if string(body) != "ok-via-socks" {
		t.Fatalf("body=%q", body)
	}
}

func TestSimpleOutbound_HonorsContextDeadline(t *testing.T) {
	t.Parallel()

	// Blackhole listener: accept never (or accept and hang).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	raw, _ := json.Marshal(map[string]any{
		"type":        "http",
		"server":      host,
		"server_port": port,
	})
	ob, handled, err := tryBuildSimpleOutbound(raw)
	if !handled || err != nil {
		t.Fatalf("build: handled=%v err=%v", handled, err)
	}

	// Context shorter than sing-box's old 5s default — must still fail by our deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = ob.DialContext(ctx, "tcp", M.ParseSocksaddr("example.com:443"))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("dial took %v, expected context deadline ~400ms (not sing-box 5s)", elapsed)
	}
}
