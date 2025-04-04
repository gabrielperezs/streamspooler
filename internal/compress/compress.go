package compress

import (
	"github.com/golang/snappy"
)

// Bytes compress []bytes
func Bytes(b []byte) []byte {
	return snappy.Encode(nil, b)
}
