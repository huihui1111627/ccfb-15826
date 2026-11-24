// Package rf implements runtime filters built from the actual join keys of
// a hash join's build side and pushed down to the probe-side scan.
//
// Four physical representations exist:
//
//	ModeExact  - exact set of encoded keys (map), no false positives
//	ModeBitmap - bitmap for contiguous integer single-column keys
//	ModeBloom  - bloom filter with a bounded false-positive rate
//	ModeMinMax - degraded mode keeping only per-column [min,max] ranges
//
// Every mode additionally carries the min/max range (used for block-level
// pruning) and a NullPasses flag implementing join-null semantics.
package rf

import "runtimefilter/rel"

// Mode identifies the physical representation of a runtime filter.
type Mode uint8

const (
	ModeMinMax Mode = iota
	ModeExact
	ModeBitmap
	ModeBloom
)

func (m Mode) String() string {
	switch m {
	case ModeExact:
		return "EXACT"
	case ModeBitmap:
		return "BITMAP"
	case ModeBloom:
		return "BLOOM"
	default:
		return "MINMAX"
	}
}

// Config limits filter size and drives representation choice / degradation.
type Config struct {
	// MaxBytes is the serialized-size budget for a filter. Oversized
	// filters degrade to min/max ranges.
	MaxBytes int
	// ExactThreshold: build sides with at most this many distinct keys use
	// an exact set (or bitmap when the key is a contiguous integer).
	ExactThreshold int
	// BloomTargetFPR is the desired bloom-filter false-positive rate.
	BloomTargetFPR float64
	// BitmapMaxSpan is the maximum [min,max] span eligible for a bitmap.
	BitmapMaxSpan int64
	// MaxWaitMS is how long a probe scan waits for the build-side filter
	// before proceeding unfiltered.
	MaxWaitMS int
	// DisableRejectFrac: adaptive kill switch. If the first
	// DisableCheckRows tested rows reject fewer than this fraction, the
	// per-row filter is disabled (block-level min/max pruning stays on).
	DisableRejectFrac float64
	DisableCheckRows  int64
}

// DefaultConfig returns the demo defaults.
func DefaultConfig() Config {
	return Config{
		MaxBytes:          64 * 1024,
		ExactThreshold:    8192,
		BloomTargetFPR:    0.02,
		BitmapMaxSpan:     1 << 20,
		MaxWaitMS:         20,
		DisableRejectFrac: 0.05,
		DisableCheckRows:  4096,
	}
}

// Stats records the observed effectiveness of one runtime filter, populated
// on the scan side as rows flow through it.
type Stats struct {
	// BlocksSeen / BlocksSkipped are block-level min/max pruning counters.
	BlocksSeen    int64
	BlocksSkipped int64
	// RowsScanned is the number of rows produced after block pruning.
	RowsScanned int64
	// RowsTested reached the per-row filter (non-null key).
	RowsTested   int64
	RowsPassed   int64
	RowsRejected int64
	// NullRows are rows whose probe key contains NULL.
	NullRows int64
	// LateRows were scanned before the filter arrived (wait timeout).
	LateRows int64
	// Disabled is set by the adaptive kill switch.
	Disabled bool
	// Degraded is set when the filter exceeded the size budget and was
	// reduced to min/max.
	Degraded bool
}

// ObservedFPR returns the measured false-positive rate among tested rows:
// rows that pass the filter but do not join are false positives. It is only
// meaningful when the probe side materializes match information; the
// executor fills MatchedRows.
type MatchStats struct {
	Passed    int64
	Matched   int64
	HasReport bool
}

// Size estimate constants (approximate wire/footprint accounting).
const (
	mapEntryBytes    = 48 // map bucket overhead per live entry (Go runtime estimate)
	bloomHeaderBytes = 32
	exactHeaderBytes = 24
	rangeEntryBytes  = 32
	bitmapHeaderByte = 16
)

// inner is the representation-specific membership test.
type inner interface {
	contains(enc []byte, vals []rel.Value) bool
	sizeBytes() int
}
