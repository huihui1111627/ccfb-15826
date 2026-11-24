package exec

import (
	"sync"
	"time"

	"runtimefilter/plan"
	"runtimefilter/rel"
	"runtimefilter/rf"
)

// Executor is the row-iterator interface shared by all operators.
type Executor interface {
	// Next returns the next row or io.EOF when exhausted.
	Next() (rel.Row, error)
	Stats() *OpStats
	Close()
	Children() []Executor
}

// OpStats records wall-clock time and row counts per operator.
type OpStats struct {
	NodeID    int
	NodeName  string
	Kind      string
	Ms        float64
	Rows      int64
	BuildMs   float64
	ProbeMs   float64
	Extra     map[string]float64
	startedAt time.Time
	totalDur  time.Duration
	buildDur  time.Duration
	probeDur  time.Duration
}

func (s *OpStats) begin() { s.startedAt = time.Now() }
func (s *OpStats) end() {
	s.totalDur += time.Since(s.startedAt)
	s.Ms = float64(s.totalDur.Microseconds()) / 1000
}
func (s *OpStats) buildStart() time.Time { return time.Now() }
func (s *OpStats) buildEnd(t time.Time) {
	s.buildDur += time.Since(t)
	s.BuildMs = float64(s.buildDur.Microseconds()) / 1000
}
func (s *OpStats) probeStart() time.Time { return time.Now() }
func (s *OpStats) probeEnd(t time.Time) {
	s.probeDur += time.Since(t)
	s.ProbeMs = float64(s.probeDur.Microseconds()) / 1000
}

// QueryContext holds execution-wide state.
type QueryContext struct {
	Cfg        rf.Config
	Planner    *PlannerContext
	EnableRF   bool
	Handles    map[int]*rf.Handle
	filters    map[int]*rf.RuntimeFilter // join id -> merged filter
	filterMu   sync.Mutex
	filterSeq  int
	allFilters []*rf.RuntimeFilter
}

// AllFilters returns every materialized runtime filter for the dashboard.
func (q *QueryContext) AllFilters() []*rf.RuntimeFilter { return q.allFilters }

func (q *QueryContext) allocFilterID() int {
	q.filterSeq++
	return q.filterSeq
}

func (q *QueryContext) registerFilter(f *rf.RuntimeFilter) {
	q.filterMu.Lock()
	defer q.filterMu.Unlock()
	q.allFilters = append(q.allFilters, f)
}

// Build constructs the executor tree for a plan node.
func Build(q *QueryContext, n plan.Node) Executor {
	switch x := n.(type) {
	case *plan.ScanNode:
		return newScan(q, x)
	case *plan.FilterNode:
		return &filterExec{node: x, child: Build(q, x.Child), stats: newStats(x, "Filter")}
	case *plan.ProjectNode:
		return &projectExec{node: x, child: Build(q, x.Child), stats: newStats(x, "Project")}
	case *plan.JoinNode:
		if x.NestedLoop {
			return newNestedLoop(q, x)
		}
		return newHashJoin(q, x)
	}
	panic("unknown plan node: " + n.Name())
}

func newStats(n plan.Node, kind string) *OpStats {
	return &OpStats{NodeID: n.ID(), NodeName: n.Name(), Kind: kind, Extra: map[string]float64{}}
}

// CollectStats walks an executor tree and gathers per-operator stats.
func CollectStats(e Executor, out *[]*OpStats) {
	*out = append(*out, e.Stats())
	for _, c := range e.Children() {
		CollectStats(c, out)
	}
}

// keyVals extracts the key columns of a row.
func keyVals(r rel.Row, cols []int) []rel.Value {
	v := make([]rel.Value, len(cols))
	for i, c := range cols {
		v[i] = r[c]
	}
	return v
}
