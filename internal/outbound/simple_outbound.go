package outbound

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// simpleOutbound is a lightweight TCP-only dialer for plain HTTP CONNECT and
// SOCKS5 proxies. It intentionally bypasses sing-box so dial/handshake honor
// the caller's full context deadline (RESIN_PROBE_TIMEOUT) instead of
// sing-box's default 5s TCPConnectTimeout.
type simpleOutbound struct {
	kind     string // "http" or "socks"
	tag      string
	proxyTCP string // host:port of the proxy server
	username string
	password string
}

type simpleOutboundConfig struct {
	Type       string `json:"type"`
	Tag        string `json:"tag"`
	Server     string `json:"server"`
	ServerPort any    `json:"server_port"`
	// Some loose feeds still use "port".
	Port     any    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Version  string `json:"version"`
}

func tryBuildSimpleOutbound(rawOptions json.RawMessage) (adapter.Outbound, bool, error) {
	var peek struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rawOptions, &peek); err != nil {
		return nil, false, nil
	}
	kind := strings.ToLower(strings.TrimSpace(peek.Type))
	switch kind {
	case "http", "socks":
	default:
		return nil, false, nil
	}

	var cfg simpleOutboundConfig
	if err := json.Unmarshal(rawOptions, &cfg); err != nil {
		return nil, true, fmt.Errorf("parse %s outbound: %w", kind, err)
	}
	server := strings.TrimSpace(cfg.Server)
	if server == "" {
		return nil, true, fmt.Errorf("%s outbound: empty server", kind)
	}
	port, err := parsePortAny(cfg.ServerPort)
	if err != nil || port == 0 {
		port, err = parsePortAny(cfg.Port)
	}
	if err != nil {
		return nil, true, fmt.Errorf("%s outbound: invalid port: %w", kind, err)
	}
	if port == 0 {
		return nil, true, fmt.Errorf("%s outbound: missing server_port", kind)
	}
	if kind == "socks" {
		ver := strings.TrimSpace(cfg.Version)
		if ver != "" && ver != "5" {
			return nil, true, fmt.Errorf("socks outbound: only version 5 is supported, got %q", ver)
		}
	}

	tag := strings.TrimSpace(cfg.Tag)
	if tag == "" {
		tag = kind
	}
	return &simpleOutbound{
		kind:     kind,
		tag:      tag,
		proxyTCP: net.JoinHostPort(server, strconv.Itoa(port)),
		username: cfg.Username,
		password: cfg.Password,
	}, true, nil
}

func parsePortAny(v any) (int, error) {
	switch p := v.(type) {
	case nil:
		return 0, nil
	case float64:
		if p < 0 || p > 65535 || p != float64(int(p)) {
			return 0, fmt.Errorf("out of range: %v", p)
		}
		return int(p), nil
	case json.Number:
		i, err := p.Int64()
		if err != nil {
			return 0, err
		}
		if i < 0 || i > 65535 {
			return 0, fmt.Errorf("out of range: %d", i)
		}
		return int(i), nil
	case string:
		s := strings.TrimSpace(p)
		if s == "" {
			return 0, nil
		}
		i, err := strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
		if i < 0 || i > 65535 {
			return 0, fmt.Errorf("out of range: %d", i)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("unsupported port type %T", v)
	}
}

func (o *simpleOutbound) Type() string           { return o.kind }
func (o *simpleOutbound) Tag() string            { return o.tag }
func (o *simpleOutbound) Network() []string      { return []string{N.NetworkTCP} }
func (o *simpleOutbound) Dependencies() []string { return nil }

func (o *simpleOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("%s outbound: network %s not supported", o.kind, network)
	}
	if !destination.IsValid() {
		return nil, fmt.Errorf("%s outbound: invalid destination", o.kind)
	}

	// No fixed dialer.Timeout: honor the full caller context (probe timeout).
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", o.proxyTCP)
	if err != nil {
		return nil, fmt.Errorf("%s outbound: dial proxy %s: %w", o.kind, o.proxyTCP, err)
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	var (
		tunneled net.Conn
		hsErr    error
	)
	switch o.kind {
	case "http":
		tunneled, hsErr = o.httpConnect(conn, destination)
	case "socks":
		hsErr = o.socks5Connect(conn, destination)
		tunneled = conn
	default:
		hsErr = fmt.Errorf("unknown simple outbound kind %q", o.kind)
	}
	if hsErr != nil {
		_ = conn.Close()
		return nil, hsErr
	}

	// Clear handshake deadline so the upper HTTP client owns I/O deadlines via ctx.
	_ = tunneled.SetDeadline(time.Time{})
	return tunneled, nil
}

func (o *simpleOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, fmt.Errorf("%s outbound: UDP not supported", o.kind)
}

func (o *simpleOutbound) Close() error { return nil }

func (o *simpleOutbound) httpConnect(conn net.Conn, destination M.Socksaddr) (net.Conn, error) {
	target := destination.String()
	var b strings.Builder
	b.WriteString("CONNECT ")
	b.WriteString(target)
	b.WriteString(" HTTP/1.1\r\nHost: ")
	b.WriteString(target)
	b.WriteString("\r\n")
	if o.username != "" || o.password != "" {
		token := base64.StdEncoding.EncodeToString([]byte(o.username + ":" + o.password))
		b.WriteString("Proxy-Authorization: Basic ")
		b.WriteString(token)
		b.WriteString("\r\n")
	}
	b.WriteString("User-Agent: Resin/1.0\r\n")
	b.WriteString("Proxy-Connection: Keep-Alive\r\n\r\n")

	if _, err := io.WriteString(conn, b.String()); err != nil {
		return nil, fmt.Errorf("http outbound: write CONNECT: %w", err)
	}

	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("http outbound: read CONNECT status: %w", err)
	}
	statusLine = strings.TrimSpace(statusLine)
	// HTTP/1.x 200 ...
	parts := strings.SplitN(statusLine, " ", 3)
	if len(parts) < 2 || !strings.HasPrefix(parts[0], "HTTP/") {
		return nil, fmt.Errorf("http outbound: bad CONNECT status line %q", statusLine)
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil || code < 200 || code >= 300 {
		return nil, fmt.Errorf("http outbound: CONNECT rejected: %s", statusLine)
	}
	// Drain headers.
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("http outbound: read CONNECT headers: %w", err)
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	// Any buffered body bytes after headers must remain available to the caller.
	if br.Buffered() > 0 {
		return &bufferedConn{Conn: conn, r: br}, nil
	}
	return conn, nil
}

// bufferedConn prepends bytes already read by a bufio.Reader during CONNECT.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if c.r != nil {
		if c.r.Buffered() > 0 {
			return c.r.Read(p)
		}
		c.r = nil
	}
	return c.Conn.Read(p)
}

func (o *simpleOutbound) socks5Connect(conn net.Conn, destination M.Socksaddr) error {
	// Offer no-auth always; also user/pass when credentials are present so the
	// server can pick either method.
	var greeting []byte
	if o.username != "" || o.password != "" {
		greeting = []byte{0x05, 0x02, 0x00, 0x02}
	} else {
		greeting = []byte{0x05, 0x01, 0x00}
	}
	if _, err := conn.Write(greeting); err != nil {
		return fmt.Errorf("socks outbound: write greeting: %w", err)
	}

	var methodResp [2]byte
	if _, err := io.ReadFull(conn, methodResp[:]); err != nil {
		return fmt.Errorf("socks outbound: read method: %w", err)
	}
	if methodResp[0] != 0x05 {
		return fmt.Errorf("socks outbound: unexpected version %d", methodResp[0])
	}
	switch methodResp[1] {
	case 0x00:
		// no auth
	case 0x02:
		if err := o.socks5UserPass(conn); err != nil {
			return err
		}
	case 0xff:
		return fmt.Errorf("socks outbound: no acceptable auth method")
	default:
		return fmt.Errorf("socks outbound: unsupported auth method %d", methodResp[1])
	}

	req, err := buildSocks5ConnectRequest(destination)
	if err != nil {
		return err
	}
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("socks outbound: write connect: %w", err)
	}

	// ver, rep, rsv, atyp
	var hdr [4]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return fmt.Errorf("socks outbound: read connect reply: %w", err)
	}
	if hdr[0] != 0x05 {
		return fmt.Errorf("socks outbound: bad reply version %d", hdr[0])
	}
	if hdr[1] != 0x00 {
		return fmt.Errorf("socks outbound: connect failed, rep=%d", hdr[1])
	}
	if err := discardSocks5Addr(conn, hdr[3]); err != nil {
		return fmt.Errorf("socks outbound: read bind addr: %w", err)
	}
	return nil
}

func (o *simpleOutbound) socks5UserPass(conn net.Conn) error {
	user := []byte(o.username)
	pass := []byte(o.password)
	if len(user) > 255 || len(pass) > 255 {
		return fmt.Errorf("socks outbound: username/password too long")
	}
	auth := make([]byte, 0, 3+len(user)+len(pass))
	auth = append(auth, 0x01, byte(len(user)))
	auth = append(auth, user...)
	auth = append(auth, byte(len(pass)))
	auth = append(auth, pass...)
	if _, err := conn.Write(auth); err != nil {
		return fmt.Errorf("socks outbound: write auth: %w", err)
	}
	var resp [2]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		return fmt.Errorf("socks outbound: read auth: %w", err)
	}
	if resp[1] != 0x00 {
		return fmt.Errorf("socks outbound: auth rejected")
	}
	return nil
}

func buildSocks5ConnectRequest(destination M.Socksaddr) ([]byte, error) {
	host := destination.AddrString()
	port := destination.Port
	var req []byte
	req = append(req, 0x05, 0x01, 0x00) // ver, cmd=connect, rsv

	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			req = append(req, 0x01)
			req = append(req, v4...)
		} else {
			v6 := ip.To16()
			if v6 == nil {
				return nil, fmt.Errorf("socks outbound: bad IP %q", host)
			}
			req = append(req, 0x04)
			req = append(req, v6...)
		}
	} else {
		if len(host) > 255 {
			return nil, fmt.Errorf("socks outbound: domain too long")
		}
		req = append(req, 0x03, byte(len(host)))
		req = append(req, host...)
	}

	var portBytes [2]byte
	binary.BigEndian.PutUint16(portBytes[:], port)
	req = append(req, portBytes[:]...)
	return req, nil
}

func discardSocks5Addr(r io.Reader, atyp byte) error {
	switch atyp {
	case 0x01: // IPv4
		var b [4 + 2]byte
		_, err := io.ReadFull(r, b[:])
		return err
	case 0x04: // IPv6
		var b [16 + 2]byte
		_, err := io.ReadFull(r, b[:])
		return err
	case 0x03: // domain
		var n [1]byte
		if _, err := io.ReadFull(r, n[:]); err != nil {
			return err
		}
		b := make([]byte, int(n[0])+2)
		_, err := io.ReadFull(r, b)
		return err
	default:
		return fmt.Errorf("unknown atyp %d", atyp)
	}
}
