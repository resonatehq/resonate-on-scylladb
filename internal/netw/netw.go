package netw

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
)

// reqBufPool pools buffers for reading request bodies.
var reqBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

type Server struct {
	handler *core.Handler
	poll    *HttpPoll
	mux     *http.ServeMux
}

// NewServer builds the HTTP handler. If poll is non-nil, GET /poll/{group}/{id}
// streams SSE events from the poll registry; otherwise that route is unmapped
// and the path falls through to a 404 from the mux.
func NewServer(h *core.Handler, poll *HttpPoll) *Server {
	s := &Server{handler: h, poll: poll, mux: http.NewServeMux()}
	if poll != nil {
		s.mux.HandleFunc("GET /poll/{group}/{id}", s.handlePoll)
	}
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("/", s.handleRPC)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	buf := reqBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer reqBufPool.Put(buf)

	if _, err := buf.ReadFrom(r.Body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	yield := func(string) {}

	out, err := s.handler.Handle(buf.Bytes(), yield)
	if err != nil {
		if errors.Is(err, core.ErrUnauthorized) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}

func (s *Server) handlePoll(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	id := r.PathValue("id")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	connID, rx, ok := s.poll.Register(group, id)
	if !ok {
		http.Error(w, "poll registry at capacity", http.StatusServiceUnavailable)
		return
	}
	defer s.poll.Deregister(group, connID)

	// SSE connections are long-lived; clear the server-level write deadline so
	// the connection isn't killed by WriteTimeout on the http.Server.
	rc := http.NewResponseController(w)
	rc.SetWriteDeadline(time.Time{})

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	shutdown := s.poll.Shutdown()

	for {
		select {
		case msg := <-rx:
			if err := writeSSE(w, msg); err != nil {
				return
			}
			flusher.Flush()
		case <-ctx.Done():
			return
		case <-shutdown:
			return
		}
	}
}

// writeSSE writes an SSE `data:` event. Multi-line payloads are split so
// each line carries the `data: ` prefix as required by the SSE spec.
func writeSSE(w http.ResponseWriter, payload []byte) error {
	for _, line := range bytes.Split(payload, []byte("\n")) {
		if _, err := fmt.Fprintf(w, "data: %s\n", line); err != nil {
			return err
		}
	}
	_, err := fmt.Fprint(w, "\n")
	return err
}
