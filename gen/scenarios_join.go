package gen

import (
	"math/rand"

	"runtimefilter/plan"
	"runtimefilter/rel"
)

// two-level join: fact -> dim1 -> dim2; both build-side filters push into
// the fact scan (crossing the lower hash join's non-preserved side).
func scenarioTwoLevelJoin(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	d1 := intTable(r, "dim1", 1800, 1500, block, 0.0, false)
	d2 := intTable(r, "dim2", 900, 800, block, 0.0, false)
	fact := intTable(r, "fact", 70000, 6000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim1": d1, "dim2": d2, "fact": fact}

	k1 := []plan.KeyCol{intKey("k", 1)}
	lower := hashJoin(g, scan(g, fact, "fact"), scan(g, d1, "dim1"), k1, k1,
		joinSpec{typ: plan.JoinInner, buildLeft: false, shards: 2})
	// top join key: dim2.k (index 1) vs lower output's fact k (index 1)
	topL := []plan.KeyCol{intKey("k", 1)}
	topR := []plan.KeyCol{intKey("fact.k", 1)}
	root := hashJoin(g, scan(g, d2, "dim2"), lower, topL, topR,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 1})

	lowerNL := nlJoin(g, scan(g, fact, "fact"), scan(g, d1, "dim1"), k1, k1, plan.JoinInner)
	nl := nlJoin(g, scan(g, d2, "dim2"), lowerNL, topL, topR, plan.JoinInner)
	return root, nl, tables, block, 2
}

// three-level join with a projection between two joins.
func scenarioThreeLevelProject(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	d1 := intTable(r, "dim1", 1200, 1000, block, 0.0, false)
	d2 := intTable(r, "dim2", 700, 600, block, 0.0, false)
	d3 := intTable(r, "dim3", 400, 350, block, 0.0, false)
	fact := intTable(r, "fact", 60000, 5000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim1": d1, "dim2": d2, "dim3": d3, "fact": fact}

	k1 := []plan.KeyCol{intKey("k", 1)}
	j1 := hashJoin(g, scan(g, fact, "fact"), scan(g, d1, "dim1"), k1, k1,
		joinSpec{typ: plan.JoinInner, buildLeft: false, shards: 2})
	// keep fact_id, fact.k (needed downstream), v columns
	proj := project(g, j1, []int{0, 1, 2, 3, 4, 5})
	j2 := hashJoin(g, proj, scan(g, d2, "dim2"),
		[]plan.KeyCol{intKey("fact.k", 1)}, []plan.KeyCol{intKey("k", 1)},
		joinSpec{typ: plan.JoinInner, buildLeft: false, shards: 1})
	root := hashJoin(g, scan(g, d3, "dim3"), j2,
		[]plan.KeyCol{intKey("k", 1)}, []plan.KeyCol{intKey("fact.k", 1)},
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 1})

	j1n := nlJoin(g, scan(g, fact, "fact"), scan(g, d1, "dim1"), k1, k1, plan.JoinInner)
	pn := project(g, j1n, []int{0, 1, 2, 3, 4, 5})
	j2n := nlJoin(g, pn, scan(g, d2, "dim2"),
		[]plan.KeyCol{intKey("fact.k", 1)}, []plan.KeyCol{intKey("k", 1)}, plan.JoinInner)
	nl := nlJoin(g, scan(g, d3, "dim3"), j2n,
		[]plan.KeyCol{intKey("k", 1)}, []plan.KeyCol{intKey("fact.k", 1)}, plan.JoinInner)
	return root, nl, tables, block, 2
}

// composite int+string key.
func scenarioCompositeKey(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := compositeTable(r, "dim", 2000, 50, 50, 8, block)
	fact := compositeTable(r, "fact", 50000, 60, 60, 10, block)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	lk := []plan.KeyCol{intKey("ka", 1), intKey("kb", 2), strKey("code", 3)}
	rk := []plan.KeyCol{intKey("ka", 1), intKey("kb", 2), strKey("code", 3)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk, plan.JoinInner)
	return root, nl, tables, block, 2
}

func scenarioStringKey(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := stringTable(r, "dim", 1500, 900, block, 0.02)
	fact := stringTable(r, "fact", 50000, 3000, block, 0.03)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	lk := []plan.KeyCol{strKey("code", 1)}
	rk := []plan.KeyCol{strKey("code", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk, plan.JoinInner)
	return root, nl, tables, block, 2
}

func scenarioNullsInner(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := intTable(r, "dim", 2000, 1500, block, 0.08, false)
	fact := intTable(r, "fact", 60000, 5000, block, 0.12, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	lk := []plan.KeyCol{intKey("k", 1)}
	rk := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk, plan.JoinInner)
	return root, nl, tables, block, 2
}

func scenarioNullsLeftOuter(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	cust := intTable(r, "cust", 4000, 3000, block, 0.1, false)
	ord := intTable(r, "ord", 50000, 4000, block, 0.1, false)
	tables := map[string]*rel.Table{"cust": cust, "ord": ord}
	lk := []plan.KeyCol{intKey("k", 1)}
	rk := []plan.KeyCol{intKey("k", 1)}
	// build=right (ord); probe=left preserved => NullPasses must be true.
	root := hashJoin(g, scan(g, cust, "cust"), scan(g, ord, "ord"), lk, rk,
		joinSpec{typ: plan.JoinLeft, buildLeft: false, shards: 2})
	nl := nlJoin(g, scan(g, cust, "cust"), scan(g, ord, "ord"), lk, rk, plan.JoinLeft)
	return root, nl, tables, block, 2
}
