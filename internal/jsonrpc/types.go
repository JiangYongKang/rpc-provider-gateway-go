// Package jsonrpc defines JSON-RPC 2.0 wire types and codec helpers.
package jsonrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Version is the supported JSON-RPC protocol version string.
const Version = "2.0"

// RawID is a JSON-RPC request id. It is kept as raw JSON so that strings,
// numbers and null round-trip exactly and responses are never matched to the
// wrong request. A nil RawID means the id field was absent (notification).
type RawID []byte

// MarshalJSON emits the raw id verbatim.
func (id RawID) MarshalJSON() ([]byte, error) {
	if len(id) == 0 {
		return []byte("null"), nil
	}
	return id, nil
}

// UnmarshalJSON captures the raw JSON token for the id.
func (id *RawID) UnmarshalJSON(data []byte) error {
	*id = append(RawID(nil), data...)
	return nil
}

// String returns the raw id text (without quotes for strings).
func (id RawID) String() string {
	if len(id) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(id, &s); err == nil {
		return s
	}
	return string(id)
}

// Request is a JSON-RPC 2.0 request object.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      RawID           `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification reports whether the request is a notification (no id).
func (r *Request) IsNotification() bool { return len(r.ID) == 0 }

// Valid reports whether the request is a well-formed JSON-RPC 2.0 request.
func (r *Request) Valid() bool {
	return r.JSONRPC == Version && r.Method != ""
}

// Response is a JSON-RPC 2.0 response object.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      RawID           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is a JSON-RPC 2.0 error object.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Error implements the error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("jsonrpc error %d: %s", e.Code, e.Message)
}

// MakeResult builds a successful response.
func MakeResult(id RawID, result json.RawMessage) *Response {
	if len(result) == 0 {
		result = json.RawMessage("null")
	}
	return &Response{JSONRPC: Version, ID: id, Result: result}
}

// MakeError builds an error response carrying the given JSON-RPC error.
func MakeError(id RawID, e *Error) *Response {
	return &Response{JSONRPC: Version, ID: id, Error: e}
}

// ParseRequests decodes either a single request object or a batch.
//
// batch is true when the body was a JSON array. A parse/validation error is
// returned for malformed JSON, an empty batch or an invalid request object so
// the caller can answer with a single JSON-RPC error envelope.
func ParseRequests(body []byte) (reqs []*Request, batch bool, err error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, false, fmt.Errorf("empty request body")
	}
	switch trimmed[0] {
	case '{':
		var r Request
		if e := json.Unmarshal(trimmed, &r); e != nil {
			return nil, false, fmt.Errorf("invalid request object: %w", e)
		}
		if !r.Valid() {
			return nil, false, fmt.Errorf("invalid JSON-RPC 2.0 request")
		}
		return []*Request{&r}, false, nil
	case '[':
		var raw []json.RawMessage
		if e := json.Unmarshal(trimmed, &raw); e != nil {
			return nil, true, fmt.Errorf("invalid batch: %w", e)
		}
		if len(raw) == 0 {
			return nil, true, fmt.Errorf("empty batch")
		}
		reqs = make([]*Request, 0, len(raw))
		for i, item := range raw {
			var r Request
			if e := json.Unmarshal(item, &r); e != nil {
				return nil, true, fmt.Errorf("invalid batch item %d: %w", i, e)
			}
			if !r.Valid() {
				return nil, true, fmt.Errorf("invalid JSON-RPC 2.0 request at batch item %d", i)
			}
			reqs = append(reqs, &r)
		}
		return reqs, true, nil
	default:
		return nil, false, fmt.Errorf("request must be a JSON object or array")
	}
}
