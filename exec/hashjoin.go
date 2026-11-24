package exec

import (
	"io"
	"sync"
	"time"

	"runtimefilter/plan"
	"runtimefilter/rel"
	"runtimefilter/rf"
)

// hashJoinExec implements sharded parallel hash join. Each shard owns a
// hash table partition and a ShardBuilder; shard filters are merged into one
// runtime filter and published to probe-side scans before probing starts.
type hashJoinExec struct {
	node  *plan.JoinNode
	q     *QueryContext
	left  Executor
	right Executor
	stats *OpStats

	shards []*joinShard

	initialized bool
	probeDone   bool

	buildKeyCols []int
	probeKeyCols []int
	buildLeft    bool
	probeLeft    bool
	leftWidth    int
	rightWidth   int
	buildWidth   int
	probeWidth   int

	// buffered output rows waiting to be returned
	outBuf []rel.Row

	// async build coordination: the build runs in a background goroutine so
	// probe-side scans can start before the filter is published.
	buildDone chan struct{}
	buildErr  error
	buildWait bool

	// probe pump: a goroutine drains the probe child into a buffered channel
	// so scanning continues (and unfiltered rows flow) while the build is
	// still in flight, mirroring pipeline parallelism.
	probeCh    chan probeItem
	probeStart sync.Once

	// preservation bookkeeping
	rightRows    []rel.Row // full right-side rows (for right/full outer)
	rightMatched []bool
	rightIdx     int
	buildIdx     int
	rightKeyIdx  map[string][]int
	// probe-right marker: when right is the probe side, matched probe keys
	// are recorded here so unmatched right rows can be emitted afterwards.
	probeRightMatched map[string]bool
	// build-side (left) preservation for full outer joins with buildLeft.
	buildMatched []bool
	store        *sharedStore
}

type joinShard struct {
	table map[string][]int // encoded key -> indexes into buildRows
	build *rf.ShardBuilder
	rows  int
}

// buildRows stores every build-side row; shard tables reference indexes.
type buildStore struct {
	rows []rel.Row
}

func newHashJoin(q *QueryContext, j *plan.JoinNode) *hashJoinExec {
	shards := j.Shards
	if shards < 1 {
		shards = 1
	}
	e := &hashJoinExec{
		node:       j,
		q:          q,
		left:       Build(q, j.Left),
		right:      Build(q, j.Right),
		stats:      newStats(j, "HashJoin"),
		shards:     make([]*joinShard, shards),
		buildLeft:  j.BuildLeft,
		probeLeft:  !j.BuildLeft,
		leftWidth:  len(j.Left.OutTypes()),
		rightWidth: len(j.Right.OutTypes()),
	}
	buildKeys, probeKeys := plan.JoinKeyOf(j)
	e.buildKeyCols = make([]int, len(buildKeys))
	e.probeKeyCols = make([]int, len(probeKeys))
	for i, k := range buildKeys {
		e.buildKeyCols[i] = k.Index
	}
	for i, k := range probeKeys {
		e.probeKeyCols[i] = k.Index
	}
	for i := range e.shards {
		e.shards[i] = &joinShard{
			table: make(map[string][]int),
			build: rf.NewShardBuilder(q.Cfg, keyKinds(j)),
		}
	}
	return e
}

type probeItem struct {
	row rel.Row
	err error
}

func keyKinds(j *plan.JoinNode) []rel.Kind {
	buildKeys, _ := plan.JoinKeyOf(j)
	kinds := make([]rel.Kind, len(buildKeys))
	for i, k := range buildKeys {
		kinds[i] = k.Kind
	}
	return kinds
}

func (e *hashJoinExec) Children() []Executor { return []Executor{e.left, e.right} }
func (e *hashJoinExec) Stats() *OpStats      { return e.stats }
func (e *hashJoinExec) Close() {
	// Ensure background goroutines have stopped so no executor outlives the
	// build/probe children it reads from.
	if e.initialized {
		<-e.buildDone
	}
	if e.probeCh != nil {
		for range e.probeCh {
		}
	}
	e.left.Close()
	e.right.Close()
	e.stats.end()
}

func (e *hashJoinExec) buildChild() Executor {
	if e.buildLeft {
		return e.left
	}
	return e.right
}

func (e *hashJoinExec) probeChild() Executor {
	if e.buildLeft {
		return e.right
	}
	return e.left
}

func (e *hashJoinExec) init() error {
	e.initialized = true
	e.buildWidth = len(e.node.Left.OutTypes())
	if !e.buildLeft {
		e.buildWidth = len(e.node.Right.OutTypes())
	}
	e.probeWidth = e.leftWidth + e.rightWidth - e.buildWidth
	e.buildDone = make(chan struct{})

	// Build in the background: probe-side scans start immediately and only
	// block on the hash table once they have a row in hand. This reproduces
	// real pipeline parallelism and exercises the bounded filter wait.
	go func() {
		defer close(e.buildDone)
		t0 := e.stats.buildStart()
		if err := e.runBuild(); err != nil {
			e.buildErr = err
			e.stats.buildEnd(t0)
			return
		}
		if e.node.BuildDelay > 0 {
			time.Sleep(time.Duration(e.node.BuildDelay) * time.Millisecond)
		}
		e.publishFilter()
		e.stats.buildEnd(t0)
		e.prepareOuterState()
	}()
	return nil
}

// prepareOuterState snapshots rows needed to emit preserved unmatched tuples.
func (e *hashJoinExec) prepareOuterState() {
	if e.node.Type == plan.JoinRight || e.node.Type == plan.JoinFull {
		// When right is the build side, use the complete store: NULL-key
		// rows never enter the hash tables but are still preserved rows.
		if !e.buildLeft {
			e.rightRows = append(e.rightRows, e.store.rows...)
			e.rightMatched = make([]bool, len(e.rightRows))
		}
		// When right is the probe side, rows are captured on the fly during
		// probing (see processProbeRow).
		if e.buildLeft {
			e.probeRightMatched = make(map[string]bool)
			if e.node.Type == plan.JoinFull {
				// Every build row may need preservation as an unmatched
				// left row of a full outer join.
				e.buildMatched = make([]bool, len(e.store.rows))
			}
		}
	}
}

// buildRowsOf returns the stored build row at global index idx. Rows are
// stored contiguously across shards in insertion order; each shard keeps a
// local offset, resolved via the shared store below.
func (e *hashJoinExec) buildRowsOf(idx int) rel.Row {
	return e.store.rows[idx]
}

// store is the shared build row store, populated by runBuild.
type sharedStore struct{ rows []rel.Row }

// runBuild pulls the build child and routes each row to its key-hash shard
// via per-shard channels. Routing must match the probe side's
// hash(key)%nshards lookup exactly, so rows can never be consumed by a
// different shard goroutine.
func (e *hashJoinExec) runBuild() error {
	child := e.buildChild()
	n := len(e.shards)
	chans := make([]chan rel.Row, n)
	for i := range chans {
		chans[i] = make(chan rel.Row, 256)
	}
	errCh := make(chan error, 1)
	store := &sharedStore{}
	e.store = store

	var storeMu sync.Mutex
	var wg sync.WaitGroup
	for i := range e.shards {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sh := e.shards[idx]
			var buf []byte
			for row := range chans[idx] {
				vals := keyVals(row, e.buildKeyCols)
				sh.build.AddKey(vals)
				storeMu.Lock()
				gidx := len(store.rows)
				store.rows = append(store.rows, row)
				storeMu.Unlock()
				if !rf.ContainsNull(vals) {
					buf = rel.EncodeKey(vals, buf)
					sh.table[string(buf)] = append(sh.table[string(buf)], gidx)
				}
				sh.rows++
			}
		}(i)
	}
	go func() {
		var buf []byte
		for {
			r, err := child.Next()
			if err == io.EOF {
				for _, c := range chans {
					close(c)
				}
				return
			}
			if err != nil {
				errCh <- err
				for _, c := range chans {
					close(c)
				}
				return
			}
			vals := keyVals(r, e.buildKeyCols)
			idx := 0
			if !rf.ContainsNull(vals) {
				buf = rel.EncodeKey(vals, buf)
				idx = int(rel.Hash64(buf) % uint64(n))
			} else {
				// NULL-key rows never enter the table but still feed the
				// shard builders; route to shard 0 deterministically.
				idx = 0
			}
			chans[idx] <- r
		}
	}()
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	return nil
}

// publishFilter merges shard filters and hands them to the probe handle.
func (e *hashJoinExec) publishFilter() {
	if !e.q.EnableRF || e.q.Planner == nil {
		return
	}
	handle, ok := e.q.Planner.Handles[e.node.ID()]
	if !ok {
		return
	}
	est := 0
	for _, sh := range e.shards {
		if d := sh.build.DistinctLen(); d > est {
			est = d
		}
	}
	est *= len(e.shards)
	if est == 0 {
		est = 1
	}
	parts := make([]*rf.RuntimeFilter, 0, len(e.shards))
	id := e.q.allocFilterID()
	for _, sh := range e.shards {
		parts = append(parts, sh.build.Build(id, e.node.ID(), e.node.Name(), est))
	}
	merged := rf.MergeRuntimeFilters(e.q.Cfg, parts)
	if merged == nil {
		handle.PublishEmpty()
		return
	}
	merged.NullPasses = e.probeNullPasses()
	for _, binding := range e.q.Planner.Bindings {
		for _, att := range binding.Items {
			if att.Handle == handle {
				merged.Hops = append(merged.Hops, att.Hops...)
			}
		}
		for _, hop := range binding.Blocked {
			if hop.FromNodeID == e.node.ID() {
				merged.Hops = append(merged.Hops, hop)
			}
		}
	}
	for _, hop := range e.q.Planner.BlockedHops {
		if hop.FromNodeID == e.node.ID() {
			merged.Hops = append(merged.Hops, hop)
		}
	}
	// When the pushdown was blocked everywhere (e.g. full outer join) no
	// scan holds this handle; still register the filter for the dashboard,
	// but do not publish it to any scan (correctness unchanged).
	delivered := false
	for _, binding := range e.q.Planner.Bindings {
		for _, att := range binding.Items {
			if att.Handle == handle {
				delivered = true
			}
		}
	}
	e.q.registerFilter(merged)
	if delivered {
		handle.Publish(merged)
	} else {
		handle.PublishEmpty()
	}
}

// probeNullPasses encodes join semantics: a probe-side NULL key only passes
// when the join preserves all rows of the probe side.
func (e *hashJoinExec) probeNullPasses() bool {
	switch e.node.Type {
	case plan.JoinLeft:
		return e.probeLeft
	case plan.JoinRight:
		return !e.probeLeft
	case plan.JoinFull:
		return true
	default:
		return false
	}
}

func (e *hashJoinExec) Next() (rel.Row, error) {
	if !e.initialized {
		if err := e.init(); err != nil {
			return nil, err
		}
	}
	t0 := e.stats.probeStart()
	defer e.stats.probeEnd(t0)

	for {
		if len(e.outBuf) > 0 {
			r := e.outBuf[0]
			e.outBuf = e.outBuf[1:]
			e.stats.Rows++
			return r, nil
		}
		if e.probeDone {
			if err := e.awaitBuild(); err != nil {
				return nil, err
			}
			if r, ok := e.nextUnmatchedRight(); ok {
				e.stats.Rows++
				return r, nil
			}
			return nil, io.EOF
		}
		// Pull the next probe row from the pump. The pump keeps the scan
		// running while the background build finishes, so rows emitted
		// before filter arrival are genuinely unfiltered (late) rows.
		it, ok := <-e.probeChan()
		if !ok {
			e.probeDone = true
			continue
		}
		if it.err == io.EOF {
			e.probeDone = true
			continue
		}
		if it.err != nil {
			return nil, it.err
		}
		pr := it.row
		// The hash table must be complete before probing it.
		if err := e.awaitBuild(); err != nil {
			return nil, err
		}
		e.processProbeRow(pr)
	}
}

// probeChan lazily starts the probe-pump goroutine.
func (e *hashJoinExec) probeChan() <-chan probeItem {
	e.probeStart.Do(func() {
		e.probeCh = make(chan probeItem, 128)
		go func() {
			defer close(e.probeCh)
			for {
				r, err := e.probeChild().Next()
				if err == io.EOF {
					e.probeCh <- probeItem{err: io.EOF}
					return
				}
				e.probeCh <- probeItem{row: r, err: err}
				if err != nil {
					return
				}
			}
		}()
	})
	return e.probeCh
}

// awaitBuild blocks until the background build goroutine finishes.
func (e *hashJoinExec) awaitBuild() error {
	if e.buildWait {
		return e.buildErr
	}
	<-e.buildDone
	e.buildWait = true
	return e.buildErr
}

func (e *hashJoinExec) processProbeRow(pr rel.Row) {
	// When right is the probe side and is preserved, snapshot every probe
	// row and mark it matched/unmatched exactly like the nested-loop
	// reference (no separate null-padded emission path for these rows).
	rightProbePreserved := e.buildLeft &&
		(e.node.Type == plan.JoinRight || e.node.Type == plan.JoinFull)
	rightSnapIdx := -1
	if rightProbePreserved {
		rightSnapIdx = len(e.rightRows)
		e.rightRows = append(e.rightRows, pr)
		e.rightMatched = append(e.rightMatched, false)
	}

	vals := keyVals(pr, e.probeKeyCols)
	nullKey := rf.ContainsNull(vals)

	var matched []rel.Row
	if !nullKey {
		buf := rel.EncodeKey(vals, nil)
		shard := e.shards[rel.Hash64(buf)%uint64(len(e.shards))]
		idxs, found := shard.table[string(buf)]
		if found {
			for _, gidx := range idxs {
				br := e.store.rows[gidx]
				matched = append(matched, e.combine(pr, br))
				e.markMatched(br, pr)
				if e.buildMatched != nil {
					e.buildMatched[gidx] = true
				}
			}
			if rightSnapIdx >= 0 {
				e.rightMatched[rightSnapIdx] = true
			}
			e.reportMatch()
		}
	}

	if len(matched) > 0 {
		e.outBuf = append(e.outBuf, matched...)
		return
	}
	// No match. Only emit a null-padded probe row when the probe side is
	// preserved but NOT already covered by the right-side snapshot pass
	// (that pass emits preserved right rows in nextUnmatchedRight).
	if e.probePreserved() && !rightProbePreserved {
		e.outBuf = append(e.outBuf, e.nullPaddedProbe(pr))
	}
}

// combine assembles a joined row with schema = left columns + right columns.
func (e *hashJoinExec) combine(probe, build rel.Row) rel.Row {
	out := make(rel.Row, e.leftWidth+e.rightWidth)
	if e.buildLeft {
		copy(out[:len(build)], build)
		copy(out[len(build):], probe)
	} else {
		copy(out[:len(probe)], probe)
		copy(out[len(probe):], build)
	}
	return out
}

func (e *hashJoinExec) probePreserved() bool {
	switch e.node.Type {
	case plan.JoinLeft:
		return e.probeLeft
	case plan.JoinRight:
		return !e.probeLeft
	case plan.JoinFull:
		return true
	default:
		return false
	}
}

func (e *hashJoinExec) nullPaddedProbe(probe rel.Row) rel.Row {
	out := make(rel.Row, e.leftWidth+e.rightWidth)
	for i := range out {
		out[i] = rel.Null()
	}
	if e.probeLeft {
		copy(out[:len(probe)], probe)
	} else {
		copy(out[e.leftWidth:], probe)
	}
	return out
}

func (e *hashJoinExec) markMatched(build rel.Row, probe rel.Row) {
	if e.node.Type != plan.JoinRight && e.node.Type != plan.JoinFull {
		return
	}
	if e.buildLeft {
		// right side is the probe side: remember the probe row's key; rows
		// sharing that key are all considered matched (multiset semantics
		// are preserved because every such probe row produced output).
		key := string(rel.EncodeKey(keyVals(probe, e.probeKeyCols), nil))
		e.probeRightMatched[key] = true
		return
	}
	// right side is the build side: mark positions in rightRows by key.
	key := string(rel.EncodeKey(keyVals(build, e.buildKeyCols), nil))
	e.markRightByKey(key)
}

// rightKeyIndex maps an encoded right key to indexes in rightRows. Built
// lazily once, reused for every match.
func (e *hashJoinExec) ensureRightKeyIndex() map[string][]int {
	if e.rightKeyIdx != nil {
		return e.rightKeyIdx
	}
	idx := make(map[string][]int, len(e.rightRows))
	rightCols := e.rightKeyCols()
	var buf []byte
	for i, r := range e.rightRows {
		buf = rel.EncodeKey(keyVals(r, rightCols), buf)
		idx[string(buf)] = append(idx[string(buf)], i)
	}
	e.rightKeyIdx = idx
	return idx
}

func (e *hashJoinExec) rightKeyCols() []int {
	if e.buildLeft {
		return e.probeKeyCols
	}
	return e.buildKeyCols
}

func (e *hashJoinExec) markRightByKey(key string) {
	idx := e.ensureRightKeyIndex()
	for _, i := range idx[key] {
		e.rightMatched[i] = true
	}
}

// nextUnmatchedRight emits preserved right-side rows that never matched,
// followed by unmatched build-side (left) rows for full outer joins.
func (e *hashJoinExec) nextUnmatchedRight() (rel.Row, bool) {
	if e.node.Type != plan.JoinRight && e.node.Type != plan.JoinFull {
		return nil, false
	}
	for e.rightIdx < len(e.rightRows) {
		r := e.rightRows[e.rightIdx]
		matched := e.rightMatched[e.rightIdx]
		if e.buildLeft && e.probeRightMatched != nil {
			key := string(rel.EncodeKey(keyVals(r, e.rightKeyCols()), nil))
			matched = e.probeRightMatched[key]
		}
		e.rightIdx++
		if matched {
			continue
		}
		out := make(rel.Row, e.leftWidth+e.rightWidth)
		for i := range out {
			out[i] = rel.Null()
		}
		copy(out[e.leftWidth:], r)
		return out, true
	}
	// Full outer with build=left: emit unmatched build (left) rows,
	// including NULL-key rows (FULL OUTER preserves every build row).
	if e.node.Type == plan.JoinFull && e.buildLeft {
		for e.buildIdx < len(e.store.rows) {
			gidx := e.buildIdx
			e.buildIdx++
			if e.buildMatched[gidx] {
				continue
			}
			br := e.store.rows[gidx]
			out := make(rel.Row, e.leftWidth+e.rightWidth)
			for i := range out {
				out[i] = rel.Null()
			}
			copy(out[:len(br)], br)
			return out, true
		}
	}
	return nil, false
}

// reportMatch feeds observed FPR counters of this join's runtime filter.
func (e *hashJoinExec) reportMatch() {
	if !e.q.EnableRF {
		return
	}
	for _, f := range e.q.AllFilters() {
		if f.JoinNodeID != e.node.ID() {
			continue
		}
		f.AddMatch()
	}
}

func drainAll(ex Executor) []rel.Row {
	var out []rel.Row
	for {
		r, err := ex.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			return out
		}
		out = append(out, r)
	}
}
