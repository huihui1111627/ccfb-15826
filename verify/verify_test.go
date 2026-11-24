package verify

import (
	"sync"
	"testing"

	"runtimefilter/gen"
	"runtimefilter/rf"
)

// suiteCache runs the full (expensive, nested-loop-backed) suite exactly
// once per test binary and reuses the report in every assertion.
var (
	suiteOnce sync.Once
	suiteRep  SuiteReport
)

func cachedSuite() SuiteReport {
	suiteOnce.Do(func() {
		suiteRep = RunAll(20261124, Options{Cfg: rf.DefaultConfig(), ScenarioCfg: degradeConfig})
	})
	return suiteRep
}

func TestAllScenariosThreeWayMatch(t *testing.T) {
	rep := cachedSuite()
	if rep.Failed != 0 || rep.Passed != rep.Total {
		for _, s := range rep.Scenarios {
			if !s.Passed {
				t.Errorf("scenario %02d %s: %s", s.ID, s.Name, s.Error)
			}
		}
	}
}

func TestDeterministicAcrossRunsAndSeeds(t *testing.T) {
	cached := cachedSuite()
	opt := Options{Cfg: rf.DefaultConfig(), ScenarioCfg: degradeConfig}
	// Re-run a representative handful rather than the whole suite; the
	// nested-loop reference makes a full second run too slow for unit tests.
	for _, sc := range gen.NewSet(20261124) {
		if sc.ID != 1 && sc.ID != 10 && sc.ID != 26 {
			continue
		}
		r := RunOne(sc, opt)
		want := find(cached, sc.ID)
		if r.RowCount != want.RowCount || r.Passed != want.Passed {
			t.Fatalf("scenario %d non-deterministic: %d vs %d rows", sc.ID, r.RowCount, want.RowCount)
		}
	}
	// A different seed must still produce correct three-way results on a
	// small smoke subset.
	for _, s := range gen.NewSet(7) {
		if s.ID > 3 {
			continue
		}
		r := RunOne(s, opt)
		if !r.Passed {
			t.Errorf("seed=7 scenario %02d: %s", s.ID, r.Error)
		}
	}
}

func TestAdaptiveDisable(t *testing.T) {
	s := find(cachedSuite(), 22)
	if len(s.Filters) == 0 {
		t.Fatal("expected filters in low-selectivity scenario")
	}
	var anyDisabled bool
	for _, f := range s.Filters {
		if f.Disabled {
			anyDisabled = true
		}
	}
	if !anyDisabled {
		t.Errorf("expected at least one self-disabled filter, got %+v", s.Filters)
	}
}

func TestAsyncLateFilter(t *testing.T) {
	s := find(cachedSuite(), 21)
	if !s.Passed {
		t.Fatalf("late-filter scenario must stay correct: %s", s.Error)
	}
	var late int64
	for _, f := range s.Filters {
		late += f.LateRows
	}
	if late == 0 {
		t.Errorf("expected late rows from build delay, got 0")
	}
}

func TestOversizedDegradesToMinMax(t *testing.T) {
	s := find(cachedSuite(), 18)
	for _, f := range s.Filters {
		if f.Mode != "MINMAX" || !f.Degraded {
			t.Errorf("scenario 18 filter should degrade to MINMAX, got mode=%s degraded=%v", f.Mode, f.Degraded)
		}
	}
}

func TestBitmapPromotion(t *testing.T) {
	s := find(cachedSuite(), 2)
	if len(s.Filters) == 0 || s.Filters[0].Mode != "BITMAP" {
		t.Fatalf("expected BITMAP filter, got %+v", s.Filters)
	}
}

func TestBloomHasEstimatedFPR(t *testing.T) {
	s := find(cachedSuite(), 3)
	f := s.Filters[0]
	if f.Mode != "BLOOM" || f.EstimatedFPR <= 0 || f.EstimatedFPR > 0.2 {
		t.Fatalf("unexpected bloom filter: mode=%s fpr=%v", f.Mode, f.EstimatedFPR)
	}
}

func TestBlockPruningHappens(t *testing.T) {
	s := find(cachedSuite(), 26)
	var skipped int64
	for _, f := range s.Filters {
		skipped += f.BlocksSkipped
	}
	if skipped == 0 {
		t.Fatal("expected block-level skips in clustered scenario")
	}
}

func TestOuterJoinPushdownBlocked(t *testing.T) {
	for _, id := range []int{5, 7, 13} {
		s := find(cachedSuite(), id)
		for _, f := range s.Filters {
			for _, h := range f.Hops {
				if h.Action != "BLOCKED" {
					t.Fatalf("scenario %d filter on preserved side must be blocked, hop=%+v", id, h)
				}
			}
			if f.RowsScanned != 0 {
				t.Fatalf("scenario %d blocked filter must not be applied to rows, scanned=%d", id, f.RowsScanned)
			}
		}
	}
}

func TestPushdownLineageCaptured(t *testing.T) {
	s := find(cachedSuite(), 8)
	if len(s.Filters) == 0 {
		t.Fatal("expected filters in multi-join scenario")
	}
	maxHops := 0
	for _, f := range s.Filters {
		if len(f.Hops) > maxHops {
			maxHops = len(f.Hops)
		}
	}
	if maxHops < 2 {
		t.Fatalf("expected multi-hop lineage, got max %d hops", maxHops)
	}
}

func find(rep SuiteReport, id int) ScenarioReport {
	for _, s := range rep.Scenarios {
		if s.ID == id {
			return s
		}
	}
	return ScenarioReport{}
}

// degradeConfig shrinks the size budget for the explicit degradation
// scenario so its exact set exceeds the limit.
func degradeConfig(s *gen.Scenario, base rf.Config) rf.Config {
	c := base
	for _, tag := range s.Tags {
		if tag == "degrade" {
			c.MaxBytes = 4096
		}
	}
	return c
}
