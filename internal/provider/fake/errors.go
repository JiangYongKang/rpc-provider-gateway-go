package fake

import "errors"

var (
	errSimulatedTransport  = errors.New("simulated transport failure")
	errProviderUnavailable = errors.New("provider unavailable")
)
