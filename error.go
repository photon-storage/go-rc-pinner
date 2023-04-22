package rcpinner

import "errors"

var (
	ErrEmptyKey             = errors.New("key is empty")
	ErrInvalidValue         = errors.New("invalid index value")
	ErrUpdateUnsupported    = errors.New("update function not supported")
	ErrDirectPinUnsupported = errors.New("direct pin not supported")
	ErrPinCountUnderflow    = errors.New("pin count underflows")
	ErrPinCountOverflow     = errors.New("pin count overflows")
)
