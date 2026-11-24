// Package verify runs every generated scenario in three modes (no runtime
// filter, runtime filter, nested-loop reference), cross-checks result
// multisets and collects filtering/performance metrics.
package verify

import (
	"fmt"
	"sort"
	"strings"

	"runtimefilter/exec"
	"runtimefilter/gen"
	"runtimefilter/plan"
	"runtimefilter/rel"
	"runtimefilter/rf"
)

// ScenarioReport is the JSON-serializable result for one scenario.
type ScenarioReport struct {
	ID       int            `json:"id"`
	Name     string         `json:"name"`
	Tags     []string       `json:"tags"`
	PlanText string         `json:"planText"`
	Passed   bool           `json:"passed"`
	Error    string         `json:"error,omitempty"`
	RowCount int            `json:"rowCount"`
	NoRF     ModeReport     `json:"noRf"`
	RF       ModeReport     `json:"rf"`
	NL       ModeReport     `json:"nl"`
	Filters  []FilterReport `json:"filters"`
	Ops      []OpReport     `json:"ops"`
	Cfg      rf.Config      `json:"cfg"`
}

type ModeReport struct {
	ElapsedMS float64 `json:"elapsedMs"`
	Rows      int     `json:"rows"`
}

type FilterReport struct {
	ID            int      `json:"id"`
	JoinNodeID    int      `json:"joinNodeId"`
	JoinName      string   `json:"joinName"`
	Mode          string   `json:"mode"`
	BuildRows     int      `json:"buildRows"`
	DistinctKeys  int      `json:"distinctKeys"`
	SizeBytes     int      `json:"sizeBytes"`
	EstimatedFPR  float64  `json:"estimatedFpr"`
	ObservedFPR   float64  `json:"observedFpr"`
	HasObserved   bool     `json:"hasObserved"`
	NullPasses    bool     `json:"nullPasses"`
	Degraded      bool     `json:"degraded"`
	Disabled      bool     `json:"disabled"`
	BlocksSeen    int64    `json:"blocksSeen"`
	BlocksSkipped int64    `json:"blocksSkipped"`
	RowsScanned   int64    `json:"rowsScanned"`
	RowsTested    int64    `json:"rowsTested"`
	RowsPassed    int64    `json:"rowsPassed"`
	RowsRejected  int64    `json:"rowsRejected"`
	NullRows      int64    `json:"nullRows"`
	LateRows      int64    `json:"lateRows"`
	Ranges        []string `json:"ranges"`
	Hops          []rf.Hop `json:"hops"`
}

type OpReport struct {
	NodeID  int                `json:"nodeId"`
	Name    string             `json:"name"`
	Kind    string             `json:"kind"`
	TotalMS float64            `json:"totalMs"`
	BuildMS float64            `json:"buildMs"`
	ProbeMS float64            `json:"probeMs"`
	Rows    int64              `json:"rows"`
	Extras  map[string]float64 `json:"extras"`
}

// SuiteReport is the full verification output.
type SuiteReport struct {
	Seed      int64            `json:"seed"`
	Total     int              `json:"total"`
	Passed    int              `json:"passed"`
	Failed    int              `json:"failed"`
	Scenarios []ScenarioReport `json:"scenarios"`
}

// Options tweaks config per suite run.
type Options struct {
	Cfg rf.Config
	// ScenarioCfg lets individual scenarios override the config (e.g. tiny
	// size budget to force degradation).
	ScenarioCfg func(s *gen.Scenario, base rf.Config) rf.Config
}

// RunAll executes every scenario and returns the full report.
func RunAll(seed int64, opt Options) SuiteReport {
	scenarios := gen.NewSet(seed)
	rep := SuiteReport{Seed: seed, Total: len(scenarios)}
	for _, s := range scenarios {
		rep.Scenarios = append(rep.Scenarios, RunOne(s, opt))
	}
	for _, sr := range rep.Scenarios {
		if sr.Passed {
			rep.Passed++
		} else {
			rep.Failed++
		}
	}
	return rep
}

// RunOne executes and cross-checks a single scenario.
func RunOne(s *gen.Scenario, opt Options) ScenarioReport {
	cfg := opt.Cfg
	if opt.ScenarioCfg != nil {
		cfg = opt.ScenarioCfg(s, cfg)
	}
	out := ScenarioReport{
		ID: s.ID, Name: s.Name, Tags: s.Tags,
		PlanText: plan.Print(s.Root), Cfg: cfg,
	}

	noRF := exec.Run(s.Root, s.NLRoot, exec.ModeNoRF, cfg)
	withRF := exec.Run(s.Root, s.NLRoot, exec.ModeRF, cfg)
	nl := exec.Run(s.Root, s.NLRoot, exec.ModeNL, cfg)

	out.NoRF = ModeReport{ElapsedMS: noRF.Elapsed, Rows: len(noRF.Rows)}
	out.RF = ModeReport{ElapsedMS: withRF.Elapsed, Rows: len(withRF.Rows)}
	out.NL = ModeReport{ElapsedMS: nl.Elapsed, Rows: len(nl.Rows)}
	out.RowCount = len(nl.Rows)

	if diff, ok := compareMultisets(noRF.Rows, withRF.Rows); !ok {
		out.Passed = false
		out.Error = "RF vs no-RF mismatch: " + diff
		return out
	}
	if diff, ok := compareMultisets(noRF.Rows, nl.Rows); !ok {
		out.Passed = false
		out.Error = "hash join vs nested loop mismatch: " + diff
		return out
	}
	out.Passed = true

	for _, f := range withRF.Filters {
		fr := FilterReport{
			ID: f.ID, JoinNodeID: f.JoinNodeID, JoinName: f.JoinName,
			Mode: f.Mode.String(), BuildRows: f.BuildRows, DistinctKeys: f.DistinctKeys,
			SizeBytes: f.SizeBytes, EstimatedFPR: f.EstimatedFPR,
			NullPasses: f.NullPasses, Degraded: f.Stats.Degraded, Disabled: f.Stats.Disabled,
			BlocksSeen: f.Stats.BlocksSeen, BlocksSkipped: f.Stats.BlocksSkipped,
			RowsScanned: f.Stats.RowsScanned, RowsTested: f.Stats.RowsTested,
			RowsPassed: f.Stats.RowsPassed, RowsRejected: f.Stats.RowsRejected,
			NullRows: f.Stats.NullRows, LateRows: f.Stats.LateRows,
			Hops: f.Hops,
		}
		if v, ok := f.ObservedFPR(); ok {
			fr.ObservedFPR, fr.HasObserved = v, true
		}
		for _, r := range f.Ranges {
			if r.Has {
				fr.Ranges = append(fr.Ranges, fmt.Sprintf("[%s .. %s]", r.Min.StringN(), r.Max.StringN()))
			} else {
				fr.Ranges = append(fr.Ranges, "[empty]")
			}
		}
		out.Filters = append(out.Filters, fr)
	}
	for _, st := range withRF.Stats {
		out.Ops = append(out.Ops, OpReport{
			NodeID: st.NodeID, Name: st.NodeName, Kind: st.Kind,
			TotalMS: st.Ms, BuildMS: st.BuildMs, ProbeMS: st.ProbeMs,
			Rows: st.Rows, Extras: st.Extra,
		})
	}
	return out
}

// canonicalRow renders a row as an unambiguous comparable string.
func canonicalRow(r rel.Row) string {
	var sb strings.Builder
	for i, v := range r {
		if i > 0 {
			sb.WriteByte('|')
		}
		sb.WriteString(v.StringN())
	}
	return sb.String()
}

func compareMultisets(a, b []rel.Row) (string, bool) {
	if len(a) != len(b) {
		return fmt.Sprintf("row count %d != %d", len(a), len(b)), false
	}
	ca := make([]string, len(a))
	cb := make([]string, len(b))
	for i := range a {
		ca[i] = canonicalRow(a[i])
		cb[i] = canonicalRow(b[i])
	}
	sort.Strings(ca)
	sort.Strings(cb)
	for i := range ca {
		if ca[i] != cb[i] {
			return fmt.Sprintf("first differing row at sorted index %d: %q != %q", i, ca[i], cb[i]), false
		}
	}
	return "", true
}
