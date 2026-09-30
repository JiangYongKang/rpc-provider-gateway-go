// Package provider defines the backend provider abstraction.
package provider

import (
	"context"
	"encoding/json"
)

// Provider is a backend JSON-RPC provider.
type Provider interface {
	Name() string
	// Call invokes a single JSON-RPC method. A result is returned when the
	// upstream replied with "result"; an error is returned for transport,
	// timeout, HTTP-status or JSON-RPC error failures.
	Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error)
	Available() bool
	SetAvailable(bool)
}

// Stat describes selection of a provider (used for logging/tests).
type Stat struct {
	Provider string
	Attempt  int
	Reason   string
}
