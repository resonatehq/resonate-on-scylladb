package base

// Background is a long-running service that can be started and stopped.
// Implementations include background processors (timeouts, repair)
// and transports (http push, http poll).
type Background interface {
	Init()
	Stop()
}

// Transport delivers a message to an address.
// Errors are handled internally — callers fire and forget.
type Transport interface {
	Send(address string, payload []byte)
}
