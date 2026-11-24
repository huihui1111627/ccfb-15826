package exec

import (
	"io"
	"time"

	"runtimefilter/plan"
	"runtimefilter/rel"
	"runtimefilter/rf"
)

// Mode selects how a query is executed in the verification harness.
type Mode int

const (
	// ModeNoRF runs the hash-join plan without any runtime filter.
	ModeNoRF Mode = iota
	// ModeRF runs the hash-join plan with runtime filters enabled.
	ModeRF
	// ModeNL runs the reference nested-loop plan.
	ModeNL
)

// RunResult holds rows, per-operator stats and runtime-filter snapshots.
type RunResult struct {
	Mode    Mode
	Rows    []rel.Row
	Stats   []*OpStats
	Filters []*rf.RuntimeFilter
	Elapsed float64
}

// Run executes a plan to completion. When nlRoot is non-nil it is used for
// ModeNL instead of the hash-join root.
func Run(root plan.Node, nlRoot plan.Node, mode Mode, cfg rf.Config) *RunResult {
	qc := &QueryContext{Cfg: cfg, EnableRF: mode == ModeRF, Handles: map[int]*rf.Handle{}}
	var useRoot plan.Node
	if mode == ModeNL && nlRoot != nil {
		useRoot = nlRoot
	} else {
		useRoot = root
		if mode == ModeRF {
			pc := NewPlannerContext(cfg)
			pc.PlanFilters(root)
			qc.Planner = pc
			qc.Handles = pc.Handles
		}
	}
	rootExec := Build(qc, useRoot)
	start := time.Now()
	var rows []rel.Row
	for {
		r, err := rootExec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		rows = append(rows, r)
	}
	rootExec.Close()
	res := &RunResult{
		Mode:    mode,
		Rows:    rows,
		Elapsed: float64(time.Since(start).Microseconds()) / 1000,
		Filters: qc.AllFilters(),
	}
	CollectStats(rootExec, &res.Stats)
	return res
}
