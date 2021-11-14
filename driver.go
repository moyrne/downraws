package downraws

import (
	"io"
)

var Drivers = map[string]Driver{}

type Driver interface {
	NewWriter() Writer
	Suffix() string
}

type Writer interface {
	Write(...interface{}) error
	WriteTo(io.Writer) (int64, error)
}
