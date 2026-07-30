package outbound

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/sagernet/sing-box/adapter"
)

// DualOutboundBuilder routes plain http/socks nodes through a lightweight native
// dialer and leaves all other protocols to a fallback builder (typically
// SingboxBuilder). This avoids sing-box's hard-coded 5s TCPConnectTimeout for
// the common http/socks probe path.
type DualOutboundBuilder struct {
	Fallback OutboundBuilder
}

// NewDualOutboundBuilder wraps fallback with a simple http/socks front path.
func NewDualOutboundBuilder(fallback OutboundBuilder) *DualOutboundBuilder {
	return &DualOutboundBuilder{Fallback: fallback}
}

// Build tries the simple http/socks path first. Non-simple types fall through to
// Fallback. Simple-type parse/config errors are returned directly (no fallback)
// so bad configs don't get a second, more opaque failure from sing-box.
func (b *DualOutboundBuilder) Build(rawOptions json.RawMessage) (adapter.Outbound, error) {
	ob, handled, err := tryBuildSimpleOutbound(rawOptions)
	if handled {
		if err != nil {
			return nil, err
		}
		return ob, nil
	}
	if b.Fallback == nil {
		return nil, fmt.Errorf("dual outbound builder: no fallback for non-simple outbound")
	}
	return b.Fallback.Build(rawOptions)
}

// Close shuts down the fallback builder when it is an io.Closer (e.g. SingboxBuilder).
func (b *DualOutboundBuilder) Close() error {
	if b == nil || b.Fallback == nil {
		return nil
	}
	if c, ok := b.Fallback.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
