package rcpinner

import "errors"

var (
	ErrEmptyKey     = errors.New("key is empty")
	ErrInvalidValue = errors.New("invalid index value")
	ErrNotSupported = errors.New("function not supported")
)
