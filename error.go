package rcpinner

import "errors"

var (
	ErrEmptyKey     = errors.New("key is empty")
	ErrEmptyValue   = errors.New("value is empty")
	ErrNotSupported = errors.New("function not supported")
)
