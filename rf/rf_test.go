package rf

import (
	"testing"

	"runtimefilter/rel"
)

func TestExactFilterMembership(t *testing.T) {
	cfg := DefaultConfig()
	b := NewShardBuilder(cfg, []rel.Kind{rel.KindInt})
	for i := 0; i < 100; i++ {
		b.AddKey([]rel.Value{rel.Int(int64(i))})
	}
	f := b.Build(1, 1, "j", 100)
	if f.Mode != ModeExact {
		t.Fatalf("want EXACT, got %s", f.Mode)
	}
	if !f.TestRow([]rel.Value{rel.Int(50)}) {
		t.Error("present key rejected")
	}
	if f.TestRow([]rel.Value{rel.Int(100)}) {
		t.Error("absent key accepted")
	}
	if f.TestRow([]rel.Value{rel.Null()}) {
		t.Error("null must not pass inner-join filter")
	}
}

func TestBitmapPromotionAndBitTest(t *testing.T) {
	cfg := DefaultConfig()
	b := NewShardBuilder(cfg, []rel.Kind{rel.KindInt})
	for i := 100; i < 1100; i++ { // dense contiguous run
		b.AddKey([]rel.Value{rel.Int(int64(i))})
	}
	f := b.Build(1, 1, "j", 1000)
	merged := MergeRuntimeFilters(cfg, []*RuntimeFilter{f})
	if merged.Mode != ModeBitmap {
		t.Fatalf("want BITMAP for dense int key, got %s", merged.Mode)
	}
	if !merged.TestRow([]rel.Value{rel.Int(500)}) || merged.TestRow([]rel.Value{rel.Int(99)}) {
		t.Error("bitmap membership wrong")
	}
}

func TestBloomNoFalseNegatives(t *testing.T) {
	cfg := DefaultConfig()
	// distinct count above exact threshold forces bloom
	b := NewShardBuilder(cfg, []rel.Kind{rel.KindInt})
	for i := 0; i < 20000; i++ {
		b.AddKey([]rel.Value{rel.Int(int64(i * 2))})
	}
	f := b.Build(1, 1, "j", 20000)
	merged := MergeRuntimeFilters(cfg, []*RuntimeFilter{f})
	if merged.Mode != ModeBloom {
		t.Fatalf("want BLOOM, got %s", merged.Mode)
	}
	for i := 0; i < 20000; i++ {
		if !merged.TestRow([]rel.Value{rel.Int(int64(i * 2))}) {
			t.Fatalf("false negative at %d", i*2)
		}
	}
}

func TestShardMergeUnion(t *testing.T) {
	cfg := DefaultConfig()
	var parts []*RuntimeFilter
	for shard := 0; shard < 4; shard++ {
		b := NewShardBuilder(cfg, []rel.Kind{rel.KindInt})
		for i := 0; i < 100; i++ {
			b.AddKey([]rel.Value{rel.Int(int64(shard*100 + i))})
		}
		parts = append(parts, b.Build(1, 1, "j", 400))
	}
	merged := MergeRuntimeFilters(cfg, parts)
	if merged.DistinctKeys != 400 {
		t.Fatalf("merged distinct = %d, want 400", merged.DistinctKeys)
	}
	for i := 0; i < 400; i++ {
		if !merged.TestRow([]rel.Value{rel.Int(int64(i))}) {
			t.Fatalf("merged filter missing key %d", i)
		}
	}
}

func TestCompositeAndStringKeys(t *testing.T) {
	cfg := DefaultConfig()
	b := NewShardBuilder(cfg, []rel.Kind{rel.KindInt, rel.KindStr})
	b.AddKey([]rel.Value{rel.Int(1), rel.Str("a")})
	b.AddKey([]rel.Value{rel.Int(1), rel.Str("b")})
	f := MergeRuntimeFilters(cfg, []*RuntimeFilter{b.Build(1, 1, "j", 2)})
	if !f.TestRow([]rel.Value{rel.Int(1), rel.Str("a")}) {
		t.Error("composite present key rejected")
	}
	if f.TestRow([]rel.Value{rel.Int(1), rel.Str("c")}) {
		t.Error("composite absent key accepted")
	}
	// framing must distinguish (1,"ab") from ("1a","b")-style collisions
	if f.TestRow([]rel.Value{rel.Null(), rel.Str("a")}) {
		t.Error("null component must not pass")
	}
}

func TestDegradeToMinMaxOnSizeBudget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxBytes = 128 // tiny budget
	b := NewShardBuilder(cfg, []rel.Kind{rel.KindInt})
	for i := 0; i < 5000; i++ {
		b.AddKey([]rel.Value{rel.Int(int64(i))})
	}
	f := MergeRuntimeFilters(cfg, []*RuntimeFilter{b.Build(1, 1, "j", 5000)})
	if f.Mode != ModeMinMax || !f.Stats.Degraded {
		t.Fatalf("want degraded MINMAX, got %s", f.Mode)
	}
	// range-only filter still rejects clearly out-of-range values
	if f.TestRow([]rel.Value{rel.Int(1 << 30)}) {
		t.Error("out-of-range value passed minmax filter")
	}
	if !f.TestRow([]rel.Value{rel.Int(10)}) {
		t.Error("in-range value should pass minmax filter")
	}
}

func TestHandleWaitAndTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxWaitMS = 5
	h := NewHandle(cfg)
	if _, ok := h.Wait(); ok {
		t.Error("empty handle should time out")
	}
	f := &RuntimeFilter{Mode: ModeExact}
	h.Publish(f)
	got, ok := h.Wait()
	if !ok || got != f {
		t.Error("published filter not returned")
	}
}

func TestBlockCanSkip(t *testing.T) {
	cfg := DefaultConfig()
	b := NewShardBuilder(cfg, []rel.Kind{rel.KindInt})
	for i := 100; i < 200; i++ {
		b.AddKey([]rel.Value{rel.Int(int64(i))})
	}
	f := MergeRuntimeFilters(cfg, []*RuntimeFilter{b.Build(1, 1, "j", 100)})
	// block entirely above the range
	hi := rel.BlockStats{HasMin: true, HasMax: true, Min: rel.Int(500), Max: rel.Int(600)}
	if !f.BlockCanSkip([]rel.BlockStats{hi}) {
		t.Error("block above range should be skipped")
	}
	// block overlapping the range must be kept
	overlap := rel.BlockStats{HasMin: true, HasMax: true, Min: rel.Int(150), Max: rel.Int(250)}
	if f.BlockCanSkip([]rel.BlockStats{overlap}) {
		t.Error("overlapping block must not be skipped")
	}
}
