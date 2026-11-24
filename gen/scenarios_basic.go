package gen

import (
	"math/rand"

	"runtimefilter/plan"
	"runtimefilter/rel"
)

// scenarioBitmapKey: dense contiguous build key -> bitmap promotion.
func scenarioBitmapKey(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := contiguousTable("dim", 100, 1050, block) // 951 dense keys
	fact := intTable(r, "fact", 50000, 4000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	lk := []plan.KeyCol{intKey("k", 1)}
	rk := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 1})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk, plan.JoinInner)
	return root, nl, tables, block, 1
}

// scenarioLargeBuildBloom: distinct keys beyond exact threshold -> bloom.
func scenarioLargeBuildBloom(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 512
	dim := intTable(r, "dim", 40000, 40000, block, 0.0, false)
	fact := intTable(r, "fact", 120000, 60000, block, 0.0, false)
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	lk := []plan.KeyCol{intKey("k", 1)}
	rk := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 4})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk, plan.JoinInner)
	return root, nl, tables, block, 4
}

func scenarioSkew(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	dim := intTable(r, "dim", 2000, 1200, block, 0.0, false)
	fact := intTable(r, "fact", 80000, 3000, block, 0.0, true) // Zipf hot keys
	tables := map[string]*rel.Table{"dim": dim, "fact": fact}
	lk := []plan.KeyCol{intKey("k", 1)}
	rk := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk,
		joinSpec{typ: plan.JoinInner, buildLeft: true, shards: 3})
	nl := nlJoin(g, scan(g, dim, "dim"), scan(g, fact, "fact"), lk, rk, plan.JoinInner)
	return root, nl, tables, block, 3
}

// scenarioLeftOuter: left rows preserved; build=right so the probe (left)
// side receives the filter and is not preserved.
func scenarioLeftOuter(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	cust := intTable(r, "cust", 3000, 2500, block, 0.01, false)
	ord := intTable(r, "ord", 60000, 4000, block, 0.02, false)
	tables := map[string]*rel.Table{"cust": cust, "ord": ord}
	lk := []plan.KeyCol{intKey("k", 1)}
	rk := []plan.KeyCol{intKey("k", 1)}
	spec := joinSpec{typ: plan.JoinLeft, buildLeft: false, shards: 2}
	root := hashJoin(g, scan(g, cust, "cust"), scan(g, ord, "ord"), lk, rk, spec)
	nl := nlJoin(g, scan(g, cust, "cust"), scan(g, ord, "ord"), lk, rk, plan.JoinLeft)
	return root, nl, tables, block, 2
}

func scenarioRightOuter(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	ord := intTable(r, "ord", 60000, 4000, block, 0.02, false)
	ship := intTable(r, "ship", 2500, 2000, block, 0.01, false)
	tables := map[string]*rel.Table{"ord": ord, "ship": ship}
	lk := []plan.KeyCol{intKey("k", 1)}
	rk := []plan.KeyCol{intKey("k", 1)}
	// build=left (ship is right and preserved); probe=right would be
	// preserved -> blocked. Instead build=right (ship), probe=left (ord,
	// not preserved).
	root := hashJoin(g, scan(g, ord, "ord"), scan(g, ship, "ship"), lk, rk,
		joinSpec{typ: plan.JoinRight, buildLeft: false, shards: 2})
	nl := nlJoin(g, scan(g, ord, "ord"), scan(g, ship, "ship"), lk, rk, plan.JoinRight)
	return root, nl, tables, block, 2
}

// scenarioFullOuter: both sides preserved, runtime filter cannot push.
func scenarioFullOuter(g *idGen, r *rand.Rand) (*plan.JoinNode, *plan.JoinNode, map[string]*rel.Table, int, int) {
	const block = 256
	a := intTable(r, "a", 5000, 4000, block, 0.02, false)
	b := intTable(r, "b", 5000, 4000, block, 0.02, false)
	tables := map[string]*rel.Table{"a": a, "b": b}
	lk := []plan.KeyCol{intKey("k", 1)}
	rk := []plan.KeyCol{intKey("k", 1)}
	root := hashJoin(g, scan(g, a, "a"), scan(g, b, "b"), lk, rk,
		joinSpec{typ: plan.JoinFull, buildLeft: true, shards: 2})
	nl := nlJoin(g, scan(g, a, "a"), scan(g, b, "b"), lk, rk, plan.JoinFull)
	return root, nl, tables, block, 2
}
