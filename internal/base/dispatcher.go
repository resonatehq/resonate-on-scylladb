package base

import (
	"log"
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
		log.Printf("dispatcher: invalid address %q: %v", address, err)
		return
	}
	t, ok := d.routes[u.Scheme]
	if !ok {
		log.Printf("dispatcher: no transport for scheme %q, message dropped", u.Scheme)
		return
	}
	t.Send(address, payload)
}
