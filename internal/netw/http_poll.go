package netw

import (
	"log"
	"math/rand/v2"
	"net/url"
	"sync"
	"sync/atomic"
)

// HttpPoll routes messages addressed to poll://<cast>@<group>[/<id>] to
// long-lived SSE clients registered against a group. Implements
// base.Transport (Send) and base.Background (Init/Stop).
//
// Lifecycle:
//   - Init must be called before any other method.
//   - Register / Deregister run on each SSE connection's lifetime.
//   - Send is called by the dispatcher and is non-blocking: a full
//     receiver buffer or missing target is logged and dropped.
//   - Stop closes the shutdown channel returned by Shutdown so SSE
//     handlers can terminate their loops.
type HttpPoll struct {
	// MaxConnections caps the total number of registered connections
	// across all groups. Zero falls back to defaultMaxConnections.
	MaxConnections int
	// BufferSize is the per-connection message buffer. Zero falls back
	// to defaultBufferSize.
	BufferSize int

	mu           sync.Mutex
	conns        map[string][]*connection
	nextConnID   atomic.Uint64
	shutdownC    chan struct{}
	shutdownOnce sync.Once
}

type connection struct {
	connID uint64
	id     string
	tx     chan []byte
}

const (
	defaultMaxConnections = 1000
	defaultBufferSize     = 16
)

func (h *HttpPoll) Init() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns = make(map[string][]*connection)
	h.shutdownC = make(chan struct{})
	h.shutdownOnce = sync.Once{}
	if h.MaxConnections <= 0 {
		h.MaxConnections = defaultMaxConnections
	}
	if h.BufferSize <= 0 {
		h.BufferSize = defaultBufferSize
	}
}

func (h *HttpPoll) Stop() {
	h.shutdownOnce.Do(func() {
		close(h.shutdownC)
	})
}

// Shutdown returns a channel closed when Stop is called. SSE handlers
// select on it to exit cleanly during graceful shutdown.
func (h *HttpPoll) Shutdown() <-chan struct{} {
	return h.shutdownC
}

// Register adds a connection to the group pool. Returns the connection
// id (used later for Deregister), a receive-only message channel, and
// ok=false if MaxConnections is reached.
func (h *HttpPoll) Register(group, id string) (uint64, <-chan []byte, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	total := 0
	for _, gc := range h.conns {
		total += len(gc)
	}
	if total >= h.MaxConnections {
		return 0, nil, false
	}

	connID := h.nextConnID.Add(1)
	c := &connection{
		connID: connID,
		id:     id,
		tx:     make(chan []byte, h.BufferSize),
	}
	h.conns[group] = append(h.conns[group], c)
	return connID, c.tx, true
}

// Deregister removes a connection from its group. Idempotent: calling
// with an unknown connID is a no-op.
func (h *HttpPoll) Deregister(group string, connID uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	gc := h.conns[group]
	out := gc[:0]
	for _, c := range gc {
		if c.connID != connID {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		delete(h.conns, group)
	} else {
		h.conns[group] = out
	}
}

func (h *HttpPoll) Send(address string, payload []byte) {
	addr, ok := parsePollAddress(address)
	if !ok {
		log.Printf("HttpPoll: invalid address %q", address)
		return
	}

	target := h.pick(addr)
	if target == nil {
		log.Printf("HttpPoll: no matching connection for %q, message dropped", address)
		return
	}

	// Non-blocking send: a slow consumer with a full buffer drops the
	// message rather than blocking the dispatcher. A late Send to a
	// just-deregistered connection lands in a buffer no one reads —
	// benign, since the chan is GC'd once the receiver is gone.
	select {
	case target.tx <- payload:
	default:
		log.Printf("HttpPoll: buffer full for group=%q id=%q, message dropped",
			addr.Group, target.id)
	}
}

// pick selects the target connection per the cast rules, releasing the
// lock before any chan send happens.
func (h *HttpPoll) pick(addr PollAddress) *connection {
	h.mu.Lock()
	defer h.mu.Unlock()

	gc := h.conns[addr.Group]
	if len(gc) == 0 {
		return nil
	}

	switch addr.Cast {
	case PollCastUni:
		if addr.ID == "" {
			return nil
		}
		for _, c := range gc {
			if c.id == addr.ID {
				return c
			}
		}
		return nil
	case PollCastAny:
		if addr.ID != "" {
			for _, c := range gc {
				if c.id == addr.ID {
					return c
				}
			}
		}
		return gc[rand.IntN(len(gc))]
	}
	return nil
}

// PollCast distinguishes unicast (specific connection) from anycast
// (load-balanced across a group).
type PollCast int

const (
	PollCastUni PollCast = iota
	PollCastAny
)

// PollAddress is a parsed poll:// address.
//
// Grammar: poll://<cast>@<group>[/<id>]
//
//	cast  = "uni" | "any"
//	group = host portion (load-balancing pool name)
//	id    = optional path segment (specific connection within the pool)
type PollAddress struct {
	Cast  PollCast
	Group string
	ID    string
}

// parsePollAddress parses an address of the form poll://cast@group[/id].
// Returns ok=false on any malformed input.
func parsePollAddress(address string) (PollAddress, bool) {
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "poll" || u.User == nil {
		return PollAddress{}, false
	}
	var cast PollCast
	switch u.User.Username() {
	case "uni":
		cast = PollCastUni
	case "any":
		cast = PollCastAny
	default:
		return PollAddress{}, false
	}
	if u.Host == "" {
		return PollAddress{}, false
	}
	id := ""
	if len(u.Path) > 1 {
		id = u.Path[1:]
	}
	return PollAddress{Cast: cast, Group: u.Host, ID: id}, true
}
