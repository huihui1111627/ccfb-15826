package exec

import (
	"sync"
	"testing"

	"runtimefilter/plan"
	"runtimefilter/rel"
	"runtimefilter/rf"
)

// tinyTables builds a small build/probe pair with an integer join key.
func tinyTables() (*rel.Table, *rel.Table) {
	cols := []string{"id", "k", "v"}
	kinds := []rel.Kind{rel.KindInt, rel.KindInt, rel.KindInt}
	dimRows := make([]rel.Row, 50)
	for i := range dimRows {
		dimRows[i] = rel.Row{rel.Int(int64(i)), rel.Int(int64(i)), rel.Int(int64(i))}
	}
	factRows := make([]rel.Row, 500)
	for i := range factRows {
		// most probe keys miss the build range (0..49)
		factRows[i] = rel.Row{rel.Int(int64(i)), rel.Int(int64(i % 200)), rel.Int(int64(i))}
	}
	dim := rel.BuildBlocks("dim", cols, kinds, dimRows, 16)
	fact := rel.BuildBlocks("fact", cols, kinds, factRows, 16)
	return dim, fact
}

func tinyJoin(dim, fact *rel.Table, delay int) *plan.JoinNode {
	ls := &plan.ScanNode{Base: plan.NewBase(1, "dim", dim.Kinds, dim.Columns), Table: dim}
	rs := &plan.ScanNode{Base: plan.NewBase(2, "fact", fact.Kinds, fact.Columns), Table: fact}
	k := []plan.KeyCol{{Col: "k", Index: 1, Kind: rel.KindInt}}
	return &plan.JoinNode{
		Base: plan.NewBase(3, "HashJoin",
			append(append([]rel.Kind{}, dim.Kinds...), fact.Kinds...),
			append(append([]string{}, dim.Columns...), fact.Columns...)),
		Left: ls, Right: rs, Type: plan.JoinInner, LeftKeys: k, RightKeys: k,
		BuildLeft: true, Shards: 3, BuildDelay: delay,
	}
}

// TestAsyncBuildRace exercises the background build goroutine, probe pump
// and filter handle under the race detector without the slow nested loop.
func TestAsyncBuildRace(t *testing.T) {
	dim, fact := tinyTables()
	cfg := rf.DefaultConfig()
	cfg.MaxWaitMS = 1
	var wg sync.WaitGroup
	for iter := 0; iter < 20; iter++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			root := tinyJoin(dim, fact, 5)
			qc := &QueryContext{Cfg: cfg, EnableRF: true, Handles: map[int]*rf.Handle{}}
			pc := NewPlannerContext(cfg)
			pc.PlanFilters(root)
			qc.Planner = pc
			qc.Handles = pc.Handles
			e := Build(qc, root)
			n := 0
			for {
				if _, err := e.Next(); err != nil {
					break
				}
				n++
			}
			e.Close()
			if n == 0 {
				t.Error("expected joined rows")
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentHandlePublishWait hammers Handle publish/wait concurrently.
func TestConcurrentHandlePublishWait(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		h := rf.NewHandle(rf.Config{MaxWaitMS: 1})
		go func() {
			defer wg.Done()
			h.Wait()
		}()
		go func() {
			defer wg.Done()
			h.Publish(&rf.RuntimeFilter{Mode: rf.ModeExact})
		}()
	}
	wg.Wait()
}
