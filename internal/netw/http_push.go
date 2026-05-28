package netw

import (
	"bytes"
	"log/slog"
	"net/http"
	"time"
)

// HttpPush delivers messages via outbound HTTP POST.
// Implements base.Transport and base.Background.
type HttpPush struct {
	client *http.Client
}

// Send fires an HTTP POST to address with payload as the body.
// Delivery is asynchronous and best-effort; errors are logged.
func (h *HttpPush) Send(address string, payload []byte) {
	go func() {
		resp, err := h.client.Post(address, "application/json", bytes.NewReader(payload))
		if err != nil {
			slog.Warn("HttpPush: POST error", "address", address, "err", err)
			return
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			slog.Warn("HttpPush: POST non-2xx status", "address", address, "status", resp.StatusCode)
		}
	}()
}

func (h *HttpPush) Init() {
	h.client = &http.Client{Timeout: 10 * time.Second}
}

func (h *HttpPush) Stop() {}
