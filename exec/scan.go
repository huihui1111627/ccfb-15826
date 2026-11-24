package exec

import (
	"io"

	"runtimefilter/plan"
	"runtimefilter/rel"
	"runtimefilter/rf"
)

type scanExec struct {
	node    *plan.ScanNode
	q       *QueryContext
	binding *ScanBinding
	stats   *OpStats

	blockIdx int
	rowIdx   int
	started  bool
	done     bool

	// Per-attachment runtime bookkeeping.
	states []*scanAttachState
}

type scanAttachState struct {
	att       *Attachment
	adapt     *rfAdaptive
	lateCount int64
	disabled  bool
}

// rfAdaptive wraps the shared kill-switch window for one filter instance.
type rfAdaptive struct {
	tested   int64
	rejected int64
	disabled bool
}

func (a *rfAdaptive) observe(tested, rejected int64, cfg rf.Config) bool {
	if a.disabled {
		return true
	}
	a.tested += tested
	a.rejected += rejected
	if a.tested >= cfg.DisableCheckRows {
		if float64(a.rejected)/float64(a.tested) < cfg.DisableRejectFrac {
			a.disabled = true
		}
	}
	return a.disabled
}

func newScan(q *QueryContext, s *plan.ScanNode) *scanExec {
	e := &scanExec{node: s, q: q, stats: newStats(s, "Scan")}
	if q.EnableRF && q.Planner != nil {
		e.binding = q.Planner.Bindings[s.ID()]
		if e.binding != nil {
			for _, att := range e.binding.Items {
				e.states = append(e.states, &scanAttachState{att: att, adapt: &rfAdaptive{}})
			}
		}
	}
	return e
}

func (e *scanExec) Children() []Executor { return nil }
func (e *scanExec) Stats() *OpStats      { return e.stats }
func (e *scanExec) Close() {
	for _, st := range e.states {
		if st.att.Filter != nil {
			st.att.Filter.SetLateRows(st.lateCount)
		}
	}
	e.stats.end()
}

func (e *scanExec) Next() (rel.Row, error) {
	if e.done {
		return nil, io.EOF
	}
	e.stats.begin()
	defer e.stats.end()

	if !e.started {
		e.started = true
		// Initial bounded wait: give build-side filters a chance to arrive.
		e.waitForFilters()
	}

	for e.blockIdx < len(e.node.Table.Blocks) {
		blk := e.node.Table.Blocks[e.blockIdx]
		if e.rowIdx == 0 {
			if e.skipBlock(blk) {
				e.stats.Extra["blocksSkipped"]++
				e.blockIdx++
				continue
			}
			e.stats.Extra["blocksSeen"]++
		}
		for e.rowIdx < len(blk.Rows) {
			r := blk.Rows[e.rowIdx]
			e.rowIdx++
			ok, err := e.rowPasses(r)
			if err != nil {
				return nil, err
			}
			if ok {
				e.stats.Rows++
				return r, nil
			}
		}
		e.blockIdx++
		e.rowIdx = 0
	}
	e.done = true
	return nil, io.EOF
}

// waitForFilters performs the one-time bounded wait at scan start.
func (e *scanExec) waitForFilters() {
	if !e.q.EnableRF || e.binding == nil {
		return
	}
	for _, st := range e.states {
		if f, ok := st.att.Handle.Wait(); ok {
			st.att.Filter = f
		}
	}
}

// skipBlock applies block-level min/max pruning for every attached filter.
func (e *scanExec) skipBlock(blk *rel.Block) bool {
	if !e.q.EnableRF || e.binding == nil {
		return false
	}
	for _, st := range e.states {
		f := st.att.Filter
		if f == nil {
			if g, ok := st.att.Handle.TryGet(); ok {
				f, st.att.Filter = g, g
			}
		}
		if f == nil {
			continue // not arrived yet: cannot prune
		}
		colStats := make([]rel.BlockStats, len(st.att.KeyCols))
		for i, c := range st.att.KeyCols {
			colStats[i] = blk.Stats[c]
		}
		skip := f.BlockCanSkip(colStats)
		f.AddStat(func(s *rf.Stats) {
			s.BlocksSeen++
			if skip {
				s.BlocksSkipped++
			}
		})
		if skip {
			return true
		}
	}
	return false
}

// rowPasses applies every attached per-row filter.
func (e *scanExec) rowPasses(r rel.Row) (bool, error) {
	if !e.q.EnableRF || e.binding == nil {
		return true, nil
	}
	for _, st := range e.states {
		f := st.att.Filter
		if f == nil {
			if g, ok := st.att.Handle.TryGet(); ok {
				f, st.att.Filter = g, g
			}
		}
		if f == nil {
			// Filter still in flight after the initial wait: row goes
			// through unfiltered and is counted as late.
			st.lateCount++
			e.stats.Extra["lateRows"]++
			continue
		}
		vals := keyVals(r, st.att.KeyCols)
		if rf.ContainsNull(vals) {
			f.AddStat(func(s *rf.Stats) { s.NullRows++ })
			if f.NullPasses {
				continue
			}
			f.AddStat(func(s *rf.Stats) { s.RowsScanned++; s.RowsRejected++ })
			return false, nil
		}
		f.AddStat(func(s *rf.Stats) { s.RowsScanned++ })
		if st.adapt.disabled {
			// Kill switch active: skip membership work, keep min/max pruning.
			f.AddStat(func(s *rf.Stats) { s.RowsPassed++ })
			f.AddPassed()
			continue
		}
		f.AddStat(func(s *rf.Stats) { s.RowsTested++ })
		if f.TestRow(vals) {
			f.AddStat(func(s *rf.Stats) { s.RowsPassed++ })
			// Every row the filter lets through is a candidate match; the
			// hash join increments Matched when it actually finds a key, so
			// observed FPR = (Passed - Matched) / Passed.
			f.AddPassed()
			if st.adapt.observe(1, 0, e.q.Cfg) {
				st.disabled = true
				f.SetDisabled()
			}
		} else {
			f.AddStat(func(s *rf.Stats) { s.RowsRejected++ })
			if st.adapt.observe(1, 1, e.q.Cfg) {
				st.disabled = true
				f.SetDisabled()
			}
			return false, nil
		}
	}
	return true, nil
}
