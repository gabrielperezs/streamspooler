package kinesisPool

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/spaolacci/murmur3"
)

var (
	raw = RandStringRunes(1024 * 1)
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(letterRunes[rand.Intn(len(letterRunes))])
	}
	return b
}

func BenchmarkMurmur3Size128(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		m1, m2 := murmur3.Sum128(raw)
		hash := fmt.Sprintf("%02x%02x", m1, m2)
		b.Logf("Hash: %s (%d)", hash, len(hash))
	}
}

func BenchmarkMurmur3Size128Seed(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		m1, m2 := murmur3.Sum128WithSeed(raw, uint32(time.Now().Unix()))
		hash := fmt.Sprintf("%02x%02x", m1, m2)
		b.Logf("Hash: %s (%d)", hash, len(hash))
	}
}

func BenchmarkMurmur3Size128Hex(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		m1, m2 := murmur3.Sum128(raw)
		var hash []byte
		hash = strconv.AppendUint(hash, m1, 16)
		hash = strconv.AppendUint(hash, m2, 16)
		b.Logf("Hash: %s (%d)", hash, len(hash))
	}
}

func BenchmarkMurmur3Size128SeedHex(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		m1, m2 := murmur3.Sum128WithSeed(raw, uint32(time.Now().Unix()))
		var hash []byte
		hash = strconv.AppendUint(hash, m1, 16)
		hash = strconv.AppendUint(hash, m2, 16)
		b.Logf("Hash: %s (%d)", hash, len(hash))
	}
}
