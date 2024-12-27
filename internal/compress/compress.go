// from github.com/gallir/smart-relayer/lib/compress/compress.go
package compress

import (
	"github.com/golang/snappy"
)

const (
	MinCompressSize  = 256
	CompressPageSize = 256
)

var (
	magicSnappy = []byte("$sy$")
)

// Bytes compress []bytes
func Bytes(b []byte) []byte {
	n := snappy.MaxEncodedLen(len(b)) + len(magicSnappy)
	// Create the required slice once
	buf := make([]byte, (n/CompressPageSize+1)*CompressPageSize)
	copy(buf, magicSnappy)
	c := snappy.Encode(buf[len(magicSnappy):], b)
	return buf[:len(c)+len(magicSnappy)]
}
