package gen

import (
	"math/rand"

	"runtimefilter/plan"
	"runtimefilter/rel"
)

// filter crosses a residual Filter node on the probe side.
func scenarioResidualFilter(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := intTable(r, "dim", 1500, 1000, block, 0.0, false)
	fact := intTable(r, "fact", 60000, 5000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	preds := []plan.Predicate{{Col: "v", ColIdx: 2, Op: ">", Literal: rel.Int(1000)}}
	probe := filterNode(g, scan(g, fact, "fact"), preds)
	probeNL := filterNode(g, scan(g, fact, "fact"), preds)
	root := hashJoin(g, scan(g, dim, "dim"), probe, k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), probeNL, k, k, plan.JoinInner)
	return root, nl, tables, block, 2
}

// filter crosses a Project node that keeps the key.
func scenarioThroughProjection(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := intTable(r, "dim", 1500, 1000, block, 0.0, false)
	fact := intTable(r, "fact", 60000, 5000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	probe := project(g, scan(g, fact, "fact"), []int{1, 2}) // k, v
	probeNL := project(g, scan(g, fact, "fact"), []int{1, 2})
	// projected k is now output index 0
	pk := []plan.KeyCol{intKey("k", 0)}
	root := hashJoin(g, scan(g, dim, "dim"), probe, k, pk,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), probeNL, k, pk, plan.JoinInner)
	return root, nl, tables, block, 2
}

// projection drops the key: filter must stop above the projection.
func scenarioProjectionEliminatesKey(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := intTable(r, "dim", 1500, 1000, block, 0.0, false)
	fact := intTable(r, "fact", 60000, 5000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	// join happens on k, then a top projection removes the fact key; the
	// filter originates below that projection so it still reaches the scan
	// via a second inner join placed above the projection.
	k := []plan.KeyCol{intKey("k", 1)}
	lower := hashJoin(g, scan(g, fact, "fact"), scan(g, dim, "dim"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: false, shards: 1})
	// output: fact_id, fact.k, fact.v, dim_id, dim.k, dim.v
	proj := project(g, lower, []int{0, 2, 3, 5}) // both k columns gone
	d2 := intTable(r, "dim2", 800, 700, block, 0.0, false)
	tables["dim2"] = d2
	root := hashJoin(g, proj, scan(g, d2, "dim2"),
		[]plan.KeyCol{intKey("v", 1)}, []plan.KeyCol{intKey("v", 2)},
		joinSpec{typ: plan.JoinInner, buildLeft: false, shards: 1})

	lowerNL := nlJoin(g, scan(g, fact, "fact"), scan(g, dim, "dim"), k, k, plan.JoinInner)
	projNL := project(g, lowerNL, []int{0, 2, 3, 5})
	nl := nlJoin(g, projNL, scan(g, d2, "dim2"),
		[]plan.KeyCol{intKey("v", 1)}, []plan.KeyCol{intKey("v", 2)}, plan.JoinInner)
	return root, nl, tables, block, 1
}

// outer join around an inner join: filter from the inner build reaches the
// inner probe but the outer-join filter is blocked on the preserved side.
func scenarioOuterAroundInner(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	fact := intTable(r, "fact", 50000, 4000, block, 0.0, false)
	d1 := intTable(r, "dim1", 1500, 1200, block, 0.0, false)
	d2 := intTable(r, "dim2", 2000, 3000, block, 0.0, false)
	tables := map[string]*rel.Table{"fact": fact, "dim1": d1, "dim2": d2}
	k := []plan.KeyCol{intKey("k", 1)}
	inner := hashJoin(g, scan(g, fact, "fact"), scan(g, d1, "dim1"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: false, shards: 2})
	// left outer: left side (inner) preserved, right side d2 not.
	root := hashJoin(g, inner, scan(g, d2, "dim2"),
		[]plan.KeyCol{intKey("fact.k", 1)}, k,
		joinSpec{typ: plan.JoinLeft, buildLeft: false, shards: 1})
	innerNL := nlJoin(g, scan(g, fact, "fact"), scan(g, d1, "dim1"), k, k, plan.JoinInner)
	nl := nlJoin(g, innerNL, scan(g, d2, "dim2"),
		[]plan.KeyCol{intKey("fact.k", 1)}, k, plan.JoinLeft)
	return root, nl, tables, block, 2
}

// force degradation: tiny MaxBytes is applied by the verifier via config
// override; here we just generate a big exact-size build side.
func scenarioOversizedDegrade(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 1024
	dim := intTable(r, "dim", 9000, 9000, block, 0.0, false)
	fact := intTable(r, "fact", 90000, 20000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 4})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 4
}

func scenarioShardedBloom(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 512
	dim := intTable(r, "dim", 30000, 30000, block, 0.0, false)
	fact := intTable(r, "fact", 90000, 50000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 4})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 4
}

func scenarioShardedExact(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 128
	dim := intTable(r, "dim", 3000, 2000, block, 0.0, false)
	fact := intTable(r, "fact", 40000, 6000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 8})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 8
}

// build delay forces the scan to time out and start unfiltered, then adopt
// the filter mid-flight.
func scenarioAsyncLate(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 128
	dim := intTable(r, "dim", 1500, 1000, block, 0.0, false)
	fact := intTable(r, "fact", 40000, 5000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2, buildDelay: 120})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 2
}

// nearly all probe keys exist in the build side: rejection fraction is below
// the kill-switch threshold and the filter self-disables.
func scenarioLowSelectivity(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := intTable(r, "dim", 4000, 4000, block, 0.0, false)
	fact := intTable(r, "fact", 50000, 3900, block, 0.0, false) // almost every key present
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 2
}

func scenarioSparseRange(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := sparseIntTable(r, "dim", 5000, 1_000_000, block)
	fact := intTable(r, "fact", 60000, 800000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 2
}

func scenarioOneToMany(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := intTable(r, "dim", 800, 500, block, 0.0, false)
	fact := intTable(r, "fact", 70000, 500, block, 0.0, false) // many rows per key
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 2
}

func scenarioEmptyBuild(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := rel.BuildBlocks("dim",
		[]string{"dim_id", "k", "v"},
		[]rel.Kind{rel.KindInt, rel.KindInt, rel.KindInt},
		nil, block)
	fact := intTable(r, "fact", 20000, 3000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 2
}

// sparseIntTable builds unique-ish keys spread across a huge range so bitmap
// promotion is impossible and exact/bloom choice depends on cardinality.
func sparseIntTable(r *rand.Rand, name string, n, span int, blockSz int) *rel.Table {
	cols := []string{name + "_id", "k", "v"}
	kinds := []rel.Kind{rel.KindInt, rel.KindInt, rel.KindInt}
	rows := make([]rel.Row, n)
	seen := make(map[int64]struct{}, n)
	for i := range rows {
		var v int64
		for {
			v = r.Int63n(int64(span))
			if _, dup := seen[v]; !dup {
				seen[v] = struct{}{}
				break
			}
		}
		rows[i] = rel.Row{rel.Int(int64(i)), rel.Int(v), rel.Int(r.Int63n(100000))}
	}
	return rel.BuildBlocks(name, cols, kinds, rows, blockSz)
}

// clusteredIntTable builds a probe-like table whose join key is sorted, so
// each block covers a narrow, disjoint key range — the layout where block
// min/max pruning is effective (ordered/clustered storage).
func clusteredIntTable(name string, n, keyDomain, blockSz int) *rel.Table {
	cols := []string{name + "_id", "k", "v"}
	kinds := []rel.Kind{rel.KindInt, rel.KindInt, rel.KindInt}
	rows := make([]rel.Row, n)
	for i := range rows {
		// sorted keys with light repetition; occasional gaps so not every
		// value is present
		k := int64((i * keyDomain) / n)
		rows[i] = rel.Row{rel.Int(int64(i)), rel.Int(k), rel.Int(int64(i * 3 % 100000))}
	}
	return rel.BuildBlocks(name, cols, kinds, rows, blockSz)
}

// scenarioBlockPrune: small exact build filter over a clustered probe table;
// many whole blocks fall outside the build key range and are skipped.
func scenarioBlockPrune(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 128
	dim := intTable(r, "dim", 1200, 900, block, 0.0, false) // keys 0..899
	fact := clusteredIntTable("fact", 80000, 8000, block)   // sorted keys 0..7999
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 2
}

// scenarioBlockPruneBloom: large build (bloom) over a clustered probe table.
func scenarioBlockPruneBloom(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := intTable(r, "dim", 20000, 20000, block, 0.0, false) // keys 0..19999
	fact := clusteredIntTable("fact", 120000, 60000, block)    // sorted keys 0..59999
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	k := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 4})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), k, k, plan.JoinInner)
	return root, nl, tables, block, 4
}
