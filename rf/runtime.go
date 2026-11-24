package rf

import (
	"fmt"
	"sync"

	"runtimefilter/rel"
)

// ColRange is the [min,max] summary of one key component on the build side.
type ColRange struct {
	Has  bool
	Kind rel.Kind
	Min  rel.Value
	Max  rel.Value
}

func (r ColRange) contains(v rel.Value) bool {
	if !r.Has {
		return true
	}
	return rel.Compare(v, r.Min) >= 0 && rel.Compare(v, r.Max) <= 0
}

// Hop records one pushdown step of a filter through the plan tree, for the
// visualization page ("generated where, pushed where").
type Hop struct {
	FromNodeID int    `json:"fromNodeId"`
	FromNode   string `json:"fromNode"`
	ToNodeID   int    `json:"toNodeId"`
	ToNode     string `json:"toNode"`
	Action     string `json:"action"` // PUSHED / BLOCKED / DEGRADED
	Note       string `json:"note"`
}

// RuntimeFilter is the filter object handed from a hash join's build side to
// its probe-side scans.
type RuntimeFilter struct {
	ID           int    `json:"id"`
	JoinNodeID   int    `json:"joinNodeId"`
	JoinName     string `json:"joinName"`
	BuildRows    int    `json:"buildRows"`
	DistinctKeys int    `json:"distinctKeys"`
	Mode         Mode   `json:"mode"`
	Ranges       []ColRange
	NullPasses   bool    `json:"nullPasses"`
	SizeBytes    int     `json:"sizeBytes"`
	EstimatedFPR float64 `json:"estimatedFpr"` // -1 for range-only filters
	KeyKinds     []rel.Kind
	Hops         []Hop      `json:"hops"`
	Stats        Stats      `json:"stats"`
	Match        MatchStats `json:"-"`
	rep          inner
	buf          []byte
	mu           sync.Mutex
}

// Stats helpers serialize concurrent updates from the probe-pump goroutine
// (scan counters) and the join goroutine (actual match counters).
func (f *RuntimeFilter) AddStat(delta func(*Stats)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delta(&f.Stats)
}

func (f *RuntimeFilter) AddMatch() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Match.HasReport = true
	f.Match.Matched++
}

func (f *RuntimeFilter) AddPassed() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Match.HasReport = true
	f.Match.Passed++
}

func (f *RuntimeFilter) SetDisabled() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Stats.Disabled = true
}

func (f *RuntimeFilter) SetDegraded() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Stats.Degraded = true
}

func (f *RuntimeFilter) SetLateRows(n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Stats.LateRows = n
}

// ContainsNull reports whether a composite key contains a NULL component.
func ContainsNull(vals []rel.Value) bool {
	for _, v := range vals {
		if v.IsNull() {
			return true
		}
	}
	return false
}

// TestRow applies the per-row membership test. Returns true when the row may
// probe the hash table, false when it can be skipped.
func (f *RuntimeFilter) TestRow(vals []rel.Value) bool {
	if ContainsNull(vals) {
		// SQL equality: NULL never joins, except the join type explicitly
		// preserves all rows of this probe side (NullPasses set then).
		return f.NullPasses
	}
	for i, v := range vals {
		if !f.Ranges[i].contains(v) {
			return false
		}
	}
	if f.rep == nil {
		return true // minmax-only mode
	}
	f.buf = rel.EncodeKey(vals, f.buf)
	return f.rep.contains(f.buf, vals)
}

// BlockCanSkip applies block-level min/max pruning. colStats has one entry
// per key component, taken from the scan table's block statistics.
func (f *RuntimeFilter) BlockCanSkip(colStats []rel.BlockStats) bool {
	for i, st := range colStats {
		r := f.Ranges[i]
		// A NULL in the block and NULLs pass this filter: keep the block.
		if st.HasNull && f.NullPasses {
			return false
		}
		if !r.Has {
			continue
		}
		// Block column with no non-null values and nulls do not pass:
		// every row fails this component.
		if !st.HasMin {
			return true
		}
		if !rel.RangeOverlap(st, r.Min, r.Max) {
			return true
		}
	}
	return false
}

// ObservedFPR returns the measured false-positive rate among rows that
// passed the filter: (passed - actually_matched) / passed.
func (f *RuntimeFilter) ObservedFPR() (float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.Match.HasReport || f.Match.Passed == 0 {
		return 0, false
	}
	fp := f.Match.Passed - f.Match.Matched
	if fp < 0 {
		fp = 0
	}
	return float64(fp) / float64(f.Match.Passed), true
}

// RejectFraction is the observed fraction of tested rows filtered out.
func (f *RuntimeFilter) RejectFraction() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Stats.RowsTested == 0 {
		return 0
	}
	return float64(f.Stats.RowsRejected) / float64(f.Stats.RowsTested)
}

// SnapshotStats returns a copy of the runtime stats for safe reporting.
func (f *RuntimeFilter) SnapshotStats() Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Stats
}

func (f *RuntimeFilter) label() string {
	return fmt.Sprintf("RF#%d(%s)", f.ID, f.Mode)
}

// ShardBuilder accumulates one parallel build partition's keys. Every
// partition keeps the exact encoded-key set plus range summaries; the final
// representation is chosen when per-shard filters are merged.
type ShardBuilder struct {
	cfg      Config
	keyKinds []rel.Kind
	exact    map[string]struct{}
	ranges   []ColRange
	rows     int
	nullKeys int64
	buf      []byte
}

// NewShardBuilder creates a build-side collector for n key columns.
func NewShardBuilder(cfg Config, keyKinds []rel.Kind) *ShardBuilder {
	ranges := make([]ColRange, len(keyKinds))
	for i, k := range keyKinds {
		ranges[i].Kind = k
	}
	return &ShardBuilder{
		cfg:      cfg,
		keyKinds: keyKinds,
		exact:    make(map[string]struct{}),
		ranges:   ranges,
	}
}

// DistinctLen returns the number of distinct non-null keys seen so far.
func (b *ShardBuilder) DistinctLen() int { return len(b.exact) }

// AddKey inserts one build-side key.
func (b *ShardBuilder) AddKey(vals []rel.Value) {
	b.rows++
	if ContainsNull(vals) {
		b.nullKeys++
		return
	}
	for i, v := range vals {
		r := &b.ranges[i]
		if !r.Has || rel.Compare(v, r.Min) < 0 {
			r.Has = true
			r.Min = v
		}
		if !r.Has || rel.Compare(v, r.Max) > 0 {
			r.Has = true
			r.Max = v
		}
	}
	b.buf = rel.EncodeKey(vals, b.buf)
	b.exact[string(b.buf)] = struct{}{}
}

// Build turns one partition's accumulation into a standalone filter. All
// partitions of one join receive the same estimated distinct count, so the
// chosen mode and bloom geometry match across partitions.
func (b *ShardBuilder) Build(id, joinID int, joinName string, estDistinct int) *RuntimeFilter {
	f := &RuntimeFilter{
		ID:           id,
		JoinNodeID:   joinID,
		JoinName:     joinName,
		BuildRows:    b.rows,
		DistinctKeys: len(b.exact),
		Ranges:       cloneRanges(b.ranges),
		KeyKinds:     b.keyKinds,
		EstimatedFPR: 0,
	}
	if estDistinct <= b.cfg.ExactThreshold {
		f.Mode = ModeExact
		ex := newExact(len(b.exact))
		for k := range b.exact {
			ex.keys[k] = struct{}{}
		}
		f.rep = ex
	} else {
		f.Mode = ModeBloom
		bl := newBloom(estDistinct, b.cfg.BloomTargetFPR)
		for k := range b.exact {
			bl.add([]byte(k))
		}
		f.rep = bl
		f.EstimatedFPR = rel.BloomFPR(len(b.exact), bl.m, bl.k)
	}
	f.SizeBytes = f.rep.sizeBytes() + len(f.Ranges)*rangeEntryBytes
	return f
}

func cloneRanges(in []ColRange) []ColRange {
	out := make([]ColRange, len(in))
	copy(out, in)
	return out
}

// MergeRuntimeFilters combines the filters produced by parallel build
// partitions into one filter, then applies bitmap promotion and the size
// budget / degradation policy.
func MergeRuntimeFilters(cfg Config, parts []*RuntimeFilter) *RuntimeFilter {
	if len(parts) == 0 {
		return nil
	}
	base := parts[0]
	out := &RuntimeFilter{
		ID:           base.ID,
		JoinNodeID:   base.JoinNodeID,
		JoinName:     base.JoinName,
		Ranges:       cloneRanges(base.Ranges),
		NullPasses:   base.NullPasses,
		KeyKinds:     base.KeyKinds,
		EstimatedFPR: 0,
	}
	for _, p := range parts[1:] {
		out.BuildRows += p.BuildRows
		mergeRanges(out.Ranges, p.Ranges)
		out.NullPasses = out.NullPasses || p.NullPasses
	}
	out.BuildRows += base.BuildRows

	allExact, allBloom, allBitmap := true, true, true
	var bitmapBase int64
	var bitmapMax int64
	for i, p := range parts {
		switch p.Mode {
		case ModeExact:
			allBloom, allBitmap = false, false
		case ModeBloom:
			allExact, allBitmap = false, false
		case ModeBitmap:
			allExact, allBloom = false, false
			bm := p.rep.(*bitmapFilter)
			if i == 0 {
				bitmapBase, bitmapMax = bm.base, bm.base+int64(len(bm.bits))*64-1
			} else if bm.base != bitmapBase {
				allBitmap = false
			}
		default:
			allExact, allBloom, allBitmap = false, false, false
		}
	}

	switch {
	case allExact:
		union := newExact(totalDistinct(parts))
		for _, p := range parts {
			for k := range p.rep.(*exactFilter).keys {
				union.keys[k] = struct{}{}
			}
		}
		out.DistinctKeys = len(union.keys)
		out.Mode = ModeExact
		out.rep = union
		out.MaybePromoteBitmap(cfg)
	case allBitmap:
		merged := newBitmap(bitmapBase, bitmapMax)
		for _, p := range parts {
			bm := p.rep.(*bitmapFilter)
			for i, w := range bm.bits {
				merged.bits[i] |= w
			}
			merged.seen += bm.seen
		}
		out.Mode = ModeBitmap
		out.rep = merged
		out.DistinctKeys = merged.seen
	case allBloom:
		var geom *bloomFilter
		for _, p := range parts {
			bl := p.rep.(*bloomFilter)
			if geom == nil {
				geom = newBloom(int(bl.m), cfg.BloomTargetFPR)
				geom.m, geom.k, geom.bits = bl.m, bl.k, make([]uint64, len(bl.bits))
			}
			for i, w := range bl.bits {
				geom.bits[i] |= w
			}
		}
		out.Mode = ModeBloom
		out.rep = geom
		out.DistinctKeys = geom.entries
		out.EstimatedFPR = rel.BloomFPR(maxDistinct(parts), geom.m, geom.k)
	default:
		// Mixed geometry: rebuild a bloom from whatever can be enumerated.
		ex := newExact(totalDistinct(parts))
		for _, p := range parts {
			switch t := p.rep.(type) {
			case *exactFilter:
				for k := range t.keys {
					ex.keys[k] = struct{}{}
				}
			case *bitmapFilter:
				for wi, w := range t.bits {
					for b := 0; b < 64; b++ {
						if w&(1<<b) != 0 {
							v := t.base + int64(wi*64+b)
							ex.keys[string(rel.EncodeKey([]rel.Value{rel.Int(v)}, nil))] = struct{}{}
						}
					}
				}
			}
		}
		if len(ex.keys) == 0 {
			out.degrade()
			return out
		}
		bl := newBloom(len(ex.keys), cfg.BloomTargetFPR)
		for k := range ex.keys {
			bl.add([]byte(k))
		}
		out.Mode, out.rep, out.DistinctKeys = ModeBloom, bl, len(ex.keys)
		out.EstimatedFPR = rel.BloomFPR(bl.entries, bl.m, bl.k)
	}

	out.finalize(cfg)
	return out
}

// MaybePromoteBitmap converts an exact filter into a bitmap when the key is
// a single, densely-populated integer column with a bounded span.
func (f *RuntimeFilter) MaybePromoteBitmap(cfg Config) {
	if f.Mode != ModeExact || len(f.KeyKinds) != 1 || f.KeyKinds[0] != rel.KindInt {
		return
	}
	ex := f.rep.(*exactFilter)
	r := f.Ranges[0]
	if !r.Has {
		return
	}
	span := r.Max.I - r.Min.I + 1
	if span <= 0 || span > cfg.BitmapMaxSpan {
		return
	}
	// Dense check: every value in [min,max] must exist.
	if int64(len(ex.keys)) != span {
		return
	}
	bm := newBitmap(r.Min.I, r.Max.I)
	for k := range ex.keys {
		// Keys of a single int column decode as tag(1) + 8 big-endian bytes.
		v := decodeSingleInt(k)
		bm.set(v)
	}
	f.Mode = ModeBitmap
	f.rep = bm
}

func decodeSingleInt(k string) int64 {
	// layout: 0x01 followed by 8 bytes big endian
	var v uint64
	for i := 1; i <= 8; i++ {
		v = v<<8 | uint64(k[i])
	}
	return int64(v)
}

func (f *RuntimeFilter) finalize(cfg Config) {
	f.SizeBytes = f.rep.sizeBytes() + len(f.Ranges)*rangeEntryBytes
	if f.SizeBytes > cfg.MaxBytes {
		f.degrade()
		return
	}
	// Exact sets that grew beyond the threshold after merge become bloom.
	if f.Mode == ModeExact && f.DistinctKeys > cfg.ExactThreshold {
		bl := newBloom(f.DistinctKeys, cfg.BloomTargetFPR)
		for k := range f.rep.(*exactFilter).keys {
			bl.add([]byte(k))
		}
		f.Mode, f.rep = ModeBloom, bl
		f.EstimatedFPR = rel.BloomFPR(bl.entries, bl.m, bl.k)
		f.SizeBytes = f.rep.sizeBytes() + len(f.Ranges)*rangeEntryBytes
		if f.SizeBytes > cfg.MaxBytes {
			f.degrade()
		}
	}
}

// degrade drops the membership structure and keeps only min/max ranges.
func (f *RuntimeFilter) degrade() {
	f.Mode = ModeMinMax
	f.rep = nil
	f.EstimatedFPR = -1
	f.SizeBytes = len(f.Ranges) * rangeEntryBytes
	f.Stats.Degraded = true
}

func mergeRanges(dst, src []ColRange) {
	for i := range dst {
		if !src[i].Has {
			continue
		}
		if !dst[i].Has || rel.Compare(src[i].Min, dst[i].Min) < 0 {
			dst[i].Has = true
			dst[i].Min = src[i].Min
		}
		if !dst[i].Has || rel.Compare(src[i].Max, dst[i].Max) > 0 {
			dst[i].Has = true
			dst[i].Max = src[i].Max
		}
	}
}

func totalDistinct(parts []*RuntimeFilter) int {
	n := 0
	for _, p := range parts {
		if e, ok := p.rep.(*exactFilter); ok {
			n += len(e.keys)
		}
	}
	return n
}

func maxDistinct(parts []*RuntimeFilter) int {
	n := 0
	for _, p := range parts {
		if bl, ok := p.rep.(*bloomFilter); ok && bl.entries > n {
			n = bl.entries
		}
	}
	return n
}
