package config

import (
	"encoding/json"
	"time"
)

// Duration wraps time.Duration so it can be unmarshaled from human-readable
// JSON strings such as "5s" or "500ms" (and also from raw nanosecond numbers).
type Duration struct {
	time.Duration
}

// MarshalJSON emits the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON accepts a duration string ("2s", "500ms") or a number
// interpreted as nanoseconds.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch val := v.(type) {
	case string:
		parsed, err := time.ParseDuration(val)
		if err != nil {
			return err
		}
		d.Duration = parsed
	case float64:
		d.Duration = time.Duration(val)
	default:
		return &json.UnmarshalTypeError{Value: string(b), Type: nil}
	}
	return nil
}
