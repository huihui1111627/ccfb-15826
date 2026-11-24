// Package gen builds deterministic test tables and query scenarios with a
// fixed random seed: skew, nulls, composite/string keys, outer joins,
// multi-level joins and oversized build sides.
package gen

import (
	"fmt"
	"math/rand"

	"runtimefilter/plan"
	"runtimefilter/rel"
)

// Scenario is one generated test case.
type Scenario struct {
	ID      int
	Name    string
	Tags    []string
	Tables  map[string]*rel.Table
	Root    *plan.JoinNode // hash-join plan
	NLRoot  *plan.JoinNode // nested-loop reference plan
	Seed    int64
	BlockSz int
	Shards  int
	// BuildDelay forces the async wait-timeout path (milliseconds).
	BuildDelay int
}

// ID allocator shared while building one scenario.
type idGen struct{ n int }

func (g *idGen) next() int {
	g.n++
	return g.n
}

// NewSet returns the deterministic master generator set.
func NewSet(seed int64) []*Scenario {
	rng := rand.New(rand.NewSource(seed))
	var out []*Scenario
	nextID := 1
	add := func(name string, tags []string, build func(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int)) {
		g := &idGen{}
		root, nl, tables, blockSz, shards := build(g, rng)
		out = append(out, &Scenario{
			ID: nextID, Name: name, Tags: tags, Tables: tables,
			Root: root, NLRoot: nl, Seed: seed, BlockSz: blockSz, Shards: shards,
		})
		nextID++
	}

	add("01 inner join small build (exact filter)", []string{"inner", "exact"}, scenarioInnerSmallExact)
	add("02 inner join contiguous int key (bitmap)", []string{"inner", "bitmap"}, scenarioBitmapKey)
	add("03 inner join large build (bloom)", []string{"inner", "bloom", "large-build"}, scenarioLargeBuildBloom)
	add("04 skew: hot key dominates probe side", []string{"inner", "skew"}, scenarioSkew)
	add("05 left outer join, filter on right probe", []string{"left-outer"}, scenarioLeftOuter)
	add("06 right outer join, filter on left probe", []string{"right-outer"}, scenarioRightOuter)
	add("07 full outer join (filters blocked both sides)", []string{"full-outer", "blocked"}, scenarioFullOuter)
	add("08 two-level inner joins, filters through join", []string{"multi-join"}, scenarioTwoLevelJoin)
	add("09 three-level join with projection in between", []string{"multi-join", "projection"}, scenarioThreeLevelProject)
	add("10 composite int+string key", []string{"composite", "string"}, scenarioCompositeKey)
	add("11 single string key", []string{"string"}, scenarioStringKey)
	add("12 nulls on both sides, inner join", []string{"nulls"}, scenarioNullsInner)
	add("13 nulls on preserved side, left outer", []string{"nulls", "left-outer"}, scenarioNullsLeftOuter)
	add("14 filter crosses residual filter node", []string{"filter-node"}, scenarioResidualFilter)
	add("15 filter crosses projection node", []string{"projection"}, scenarioThroughProjection)
	add("16 projection eliminates key -> filter blocked", []string{"projection", "blocked"}, scenarioProjectionEliminatesKey)
	add("17 filter through inner join inside outer join", []string{"multi-join", "outer"}, scenarioOuterAroundInner)
	add("18 oversized filter degrades to minmax", []string{"degrade", "large-build"}, scenarioOversizedDegrade)
	add("19 parallel shards x4 merged filter", []string{"shards", "bloom"}, scenarioShardedBloom)
	add("20 parallel shards x8 exact merge", []string{"shards", "exact"}, scenarioShardedExact)
	add("21 async late filter (build delay)", []string{"async", "late"}, scenarioAsyncLate)
	add("22 low selectivity filter self-disables", []string{"adaptive", "disable"}, scenarioLowSelectivity)
	add("23 wide-range sparse int key stays bloom", []string{"bloom"}, scenarioSparseRange)
	add("24 duplicate keys build side (1:N)", []string{"one-to-many"}, scenarioOneToMany)
	add("25 empty build side inner join", []string{"empty"}, scenarioEmptyBuild)
	add("26 clustered probe table: block min/max pruning", []string{"block-prune", "clustered"}, scenarioBlockPrune)
	add("27 clustered wide probe with large build", []string{"block-prune", "bloom"}, scenarioBlockPruneBloom)
	return out
}

// ---------------------------------------------------------------------------
// Table generation helpers
// ---------------------------------------------------------------------------

func intTable(r *rand.Rand, name string, n int, keyDomain int, blockSz int, nullPct float64, skew bool) *rel.Table {
	cols := []string{name + "_id", "k", "v"}
	kinds := []rel.Kind{rel.KindInt, rel.KindInt, rel.KindInt}
	rows := make([]rel.Row, n)
	var zipf *rand.Zipf
	if skew {
		zipf = rand.NewZipf(r, 1.5, 1, uint64(keyDomain-1))
	}
	for i := 0; i < n; i++ {
		var k int64
		if skew {
			k = int64(zipf.Uint64())
		} else {
			k = int64(r.Intn(keyDomain))
		}
		var kv rel.Value
		if nullPct > 0 && r.Float64() < nullPct {
			kv = rel.Null()
		} else {
			kv = rel.Int(k)
		}
		rows[i] = rel.Row{rel.Int(int64(i)), kv, rel.Int(r.Int63n(100000))}
	}
	return rel.BuildBlocks(name, cols, kinds, rows, blockSz)
}

// contiguousTable builds k values covering [lo, hi] densely.
func contiguousTable(name string, lo, hi int64, blockSz int) *rel.Table {
	cols := []string{name + "_id", "k", "v"}
	kinds := []rel.Kind{rel.KindInt, rel.KindInt, rel.KindInt}
	n := int(hi-lo) + 1
	rows := make([]rel.Row, n)
	for i := range rows {
		rows[i] = rel.Row{rel.Int(int64(i)), rel.Int(lo + int64(i)), rel.Int(int64(i * 7))}
	}
	return rel.BuildBlocks(name, cols, kinds, rows, blockSz)
}

func stringTable(r *rand.Rand, name string, n, distinctWords int, blockSz int, nullPct float64) *rel.Table {
	cols := []string{name + "_id", "code", "v"}
	kinds := []rel.Kind{rel.KindInt, rel.KindStr, rel.KindInt}
	words := make([]string, distinctWords)
	for i := range words {
		words[i] = fmt.Sprintf("code-%05d", i)
	}
	rows := make([]rel.Row, n)
	for i := range rows {
		var code rel.Value
		if nullPct > 0 && r.Float64() < nullPct {
			code = rel.Null()
		} else {
			code = rel.Str(words[r.Intn(distinctWords)])
		}
		rows[i] = rel.Row{rel.Int(int64(i)), code, rel.Int(r.Int63n(100000))}
	}
	return rel.BuildBlocks(name, cols, kinds, rows, blockSz)
}

func compositeTable(r *rand.Rand, name string, n, domainA, domainB, distinctWords int, blockSz int) *rel.Table {
	cols := []string{name + "_id", "ka", "kb", "code", "v"}
	kinds := []rel.Kind{rel.KindInt, rel.KindInt, rel.KindInt, rel.KindStr, rel.KindInt}
	words := make([]string, distinctWords)
	for i := range words {
		words[i] = fmt.Sprintf("w%d", i)
	}
	rows := make([]rel.Row, n)
	for i := range rows {
		rows[i] = rel.Row{
			rel.Int(int64(i)),
			rel.Int(int64(r.Intn(domainA))),
			rel.Int(int64(r.Intn(domainB))),
			rel.Str(words[r.Intn(distinctWords)]),
			rel.Int(r.Int63n(100000)),
		}
	}
	return rel.BuildBlocks(name, cols, kinds, rows, blockSz)
}

// ---------------------------------------------------------------------------
// Plan construction helpers
// ---------------------------------------------------------------------------

func scan(g *idGen, t *rel.Table, alias string) *plan.ScanNode {
	return &plan.ScanNode{
		Base:  plan.NewBase(g.next(), alias, t.Kinds, t.Columns),
		Table: t,
	}
}

func project(g *idGen, child plan.Node, idxs []int) *plan.ProjectNode {
	types := make([]rel.Kind, len(idxs))
	cols := make([]string, len(idxs))
	for i, ix := range idxs {
		types[i] = child.OutTypes()[ix]
		cols[i] = child.OutNames()[ix]
	}
	return &plan.ProjectNode{
		Base:     plan.NewBase(g.next(), "Project", types, cols),
		Child:    child,
		InputIdx: idxs,
	}
}

func filterNode(g *idGen, child plan.Node, preds []plan.Predicate) *plan.FilterNode {
	return &plan.FilterNode{
		Base:  plan.NewBase(g.next(), "Filter", child.OutTypes(), child.OutNames()),
		Child: child, Preds: preds,
	}
}

// join builds a hash join and an equivalent nested-loop join sharing the
// same child plans (children are rebuilt as separate node trees by the
// caller-supplied builders so executions do not share iterator state).
type joinSpec struct {
	typ        plan.JoinType
	buildLeft  bool
	shards     int
	buildDelay int
}

func hashJoin(g *idGen, left, right plan.Node, lk, rk []plan.KeyCol, spec joinSpec) *plan.JoinNode {
	types := append(append([]rel.Kind{}, left.OutTypes()...), right.OutTypes()...)
	cols := append(append([]string{}, left.OutNames()...), right.OutNames()...)
	buildRows := 0
	if s, ok := left.(*plan.ScanNode); ok && spec.buildLeft {
		buildRows = s.Table.Rows
	} else if s, ok := right.(*plan.ScanNode); ok && !spec.buildLeft {
		buildRows = s.Table.Rows
	}
	return &plan.JoinNode{
		Base: plan.NewBase(g.next(), "HashJoin", types, cols),
		Left: left, Right: right, Type: spec.typ,
		LeftKeys: lk, RightKeys: rk, BuildLeft: spec.buildLeft,
		BuildRows: buildRows, Shards: spec.shards, BuildDelay: spec.buildDelay,
	}
}

func nlJoin(g *idGen, left, right plan.Node, lk, rk []plan.KeyCol, typ plan.JoinType) *plan.JoinNode {
	types := append(append([]rel.Kind{}, left.OutTypes()...), right.OutTypes()...)
	cols := append(append([]string{}, left.OutNames()...), right.OutNames()...)
	return &plan.JoinNode{
		Base: plan.NewBase(g.next(), "NestedLoop", types, cols),
		Left: left, Right: right, Type: typ,
		LeftKeys: lk, RightKeys: rk, NestedLoop: true,
	}
}

func intKey(col string, idx int) plan.KeyCol {
	return plan.KeyCol{Col: col, Index: idx, Kind: rel.KindInt}
}

func strKey(col string, idx int) plan.KeyCol {
	return plan.KeyCol{Col: col, Index: idx, Kind: rel.KindStr}
}

// ---------------------------------------------------------------------------
// Scenario builders. Each returns (hashRoot, nlRoot, tables, blockSize, shards).
// ---------------------------------------------------------------------------

func scenarioInnerSmallExact(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := intTable(r, "dim", 1500, 900, block, 0.02, false)
	fact := intTable(r, "fact", 60000, 5000, block, 0.03, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	lk := []plan.KeyCol{intKey("k", 1)}
	rk := []plan.KeyCol{intKey("k", 1)}
	ls, rs := scan(g, dim, "dim"), scan(g, fact, "fact")
	ln, rn := scan(g, dim, "dim"), scan(g, fact, "fact")
	root := hashJoin(g, ls, rs, lk, rk, joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, ln, rn, lk, rk, plan.JoinInner)
	return root, nl, tables, block, 2
}
