// Package rel defines the in-memory relational data model used by the
// runtime-filter demo: typed values, rows, blocks and tables.
package rel

import (
	"encoding/binary"
	"math"
)

// Kind identifies the physical type of a Value.
type Kind uint8

const (
	KindNull Kind = iota
	KindInt
	KindStr
)

// Value is a nullable scalar. Null values have Kind == KindNull.
type Value struct {
	Kind Kind
	I    int64
	S    string
}

func Null() Value              { return Value{Kind: KindNull} }
func Int(i int64) Value        { return Value{Kind: KindInt, I: i} }
func Str(s string) Value       { return Value{Kind: KindStr, S: s} }
func (v Value) IsNull() bool   { return v.Kind == KindNull }
func (v Value) String() string { return v.StringN() }

func (v Value) StringN() string {
	switch v.Kind {
	case KindNull:
		return "NULL"
	case KindInt:
		return itoa(v.I)
	default:
		return "'" + v.S + "'"
	}
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	var neg bool
	var b [20]byte
	p := len(b)
	u := uint64(i)
	if i < 0 {
		neg = true
		u = uint64(-i)
	}
	for u > 0 {
		p--
		b[p] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

// Row is one tuple.
type Row []Value

// Compare orders two non-null values of the same kind. Nulls sort before
// every non-null value (SQL ASC NULLS FIRST ordering).
func Compare(a, b Value) int {
	if a.Kind != b.Kind {
		return int(a.Kind) - int(b.Kind)
	}
	switch a.Kind {
	case KindNull:
		return 0
	case KindInt:
		switch {
		case a.I < b.I:
			return -1
		case a.I > b.I:
			return 1
		default:
			return 0
		}
	default:
		switch {
		case a.S < b.S:
			return -1
		case a.S > b.S:
			return 1
		default:
			return 0
		}
	}
}

// EncodeKey serializes a (possibly composite, possibly nullable) key into a
// self-describing, unambiguous byte sequence. Component framing uses a type
// tag plus a fixed width (int) or length prefix (string), so two different
// tuples can never collide.
func EncodeKey(vals []Value, buf []byte) []byte {
	buf = buf[:0]
	for _, v := range vals {
		switch v.Kind {
		case KindNull:
			buf = append(buf, 0)
		case KindInt:
			buf = append(buf, 1)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(v.I))
			buf = append(buf, b[:]...)
		default:
			buf = append(buf, 2)
			var lb [binary.MaxVarintLen64]byte
			n := binary.PutUvarint(lb[:], uint64(len(v.S)))
			buf = append(buf, lb[:n]...)
			buf = append(buf, v.S...)
		}
	}
	return buf
}

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

// Hash64 hashes an encoded key with FNV-1a.
func Hash64(b []byte) uint64 {
	h := uint64(fnvOffset)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime
	}
	return h
}

// Hash64Seed is FNV-1a with a different starting offset, used as the second
// hash for double hashing in the bloom filter.
func Hash64Seed(b []byte, seed uint64) uint64 {
	h := seed
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime
	}
	return h
}

// DoubleHash returns the i-th of k hashes via Kirsch-Mitzenmacher double
// hashing: h_i = h1 + i*h2 (mod m).
func DoubleHash(h1, h2 uint64, i, m uint64) uint64 {
	return (h1 + uint64(i)*h2) % m
}

// MinInt / MaxInt helpers.
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// CeilDiv returns ceil(a/b) for positive b.
func CeilDiv(a, b int) int { return (a + b - 1) / b }

// BloomFPR returns the theoretical false-positive rate of a bloom filter
// with n inserted keys, m bits and k hash functions.
func BloomFPR(n int, m, k uint64) float64 {
	if n == 0 || m == 0 {
		return 0
	}
	load := math.Exp(-float64(k) * float64(n) / float64(m))
	return math.Pow(1-load, float64(k))
}
