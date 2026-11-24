package rf

import (
	"math"

	"runtimefilter/rel"
)

// exactFilter stores the precise set of encoded build-side keys.
type exactFilter struct {
	keys map[string]struct{}
}

func newExact(estimated int) *exactFilter {
	return &exactFilter{keys: make(map[string]struct{}, estimated)}
}

func (e *exactFilter) add(enc []byte) { e.keys[string(enc)] = struct{}{} }

func (e *exactFilter) contains(enc []byte, _ []rel.Value) bool {
	_, ok := e.keys[string(enc)]
	return ok
}

func (e *exactFilter) sizeBytes() int {
	return exactHeaderBytes + len(e.keys)*mapEntryBytes
}

// bitmapFilter applies to a single integer column whose build-side values
// form a dense run [base, base+span). The probe test is one bit lookup.
type bitmapFilter struct {
	base int64
	bits []uint64
	seen int
}

func newBitmap(min, max int64) *bitmapFilter {
	span := uint64(max-min) + 1
	return &bitmapFilter{
		base: min,
		bits: make([]uint64, (span+63)/64),
	}
}

func (b *bitmapFilter) set(v int64) {
	idx := uint64(v - b.base)
	b.bits[idx>>6] |= 1 << (idx & 63)
	b.seen++
}

func (b *bitmapFilter) contains(_ []byte, vals []rel.Value) bool {
	if len(vals) != 1 || vals[0].Kind != rel.KindInt {
		return false
	}
	v := vals[0].I
	idx := uint64(v - b.base)
	if idx >= uint64(len(b.bits))*64 {
		return false
	}
	return b.bits[idx>>6]&(1<<(idx&63)) != 0
}

func (b *bitmapFilter) sizeBytes() int {
	return bitmapHeaderByte + len(b.bits)*8
}

// bloomFilter is a classic bit-array bloom filter using double hashing.
type bloomFilter struct {
	bits    []uint64
	m       uint64 // number of bits
	k       uint64 // number of hash functions
	seed    uint64
	entries int
}

func newBloom(estimated int, targetFPR float64) *bloomFilter {
	if estimated < 1 {
		estimated = 1
	}
	// m = -n ln(p) / (ln 2)^2, rounded up to whole 64-bit words.
	mf := -float64(estimated) * math.Log(targetFPR) / (math.Ln2 * math.Ln2)
	m := uint64(mf)
	if m < 64 {
		m = 64
	}
	words := (m + 63) / 64
	m = words * 64
	k := uint64(float64(m) / float64(estimated) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return &bloomFilter{bits: make([]uint64, words), m: m, k: k, seed: fnvSeed}
}

const fnvSeed = 0x9e3779b97f4a7c15

func (b *bloomFilter) add(enc []byte) {
	h1 := rel.Hash64(enc)
	h2 := rel.Hash64Seed(enc, b.seed)
	for i := uint64(0); i < b.k; i++ {
		pos := rel.DoubleHash(h1, h2, i, b.m)
		b.bits[pos>>6] |= 1 << (pos & 63)
	}
	b.entries++
}

func (b *bloomFilter) contains(enc []byte, _ []rel.Value) bool {
	h1 := rel.Hash64(enc)
	h2 := rel.Hash64Seed(enc, b.seed)
	for i := uint64(0); i < b.k; i++ {
		pos := rel.DoubleHash(h1, h2, i, b.m)
		if b.bits[pos>>6]&(1<<(pos&63)) == 0 {
			return false
		}
	}
	return true
}

func (b *bloomFilter) sizeBytes() int {
	return bloomHeaderBytes + len(b.bits)*8
}
