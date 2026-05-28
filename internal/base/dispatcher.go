package base

import (
	"log/slog"
	"net/url"
)

// Dispatcher routes outbound messages to the appropriate transport
// by matching the address scheme against registered transports.
type Dispatcher struct {
	routes map[string]Transport
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{routes: make(map[string]Transport)}
}

// Register associates a transport with one or more URL schemes.
func (d *Dispatcher) Register(t Transport, schemes ...string) {
	for _, s := range schemes {
		d.routes[s] = t
	}
}

func (d *Dispatcher) Send(address string, payload []byte) {
	u, err := url.Parse(address)
	if err != nil {
		slog.Warn("dispatcher: invalid address", "address", address, "err", err)
		return
	}
	t, ok := d.routes[u.Scheme]
	if !ok {
		slog.Warn("dispatcher: no transport for scheme, message dropped", "scheme", u.Scheme)
		return
	}
	t.Send(address, payload)
}
