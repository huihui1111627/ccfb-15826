// Package exec evaluates plan trees and implements runtime-filter
// generation, pushdown and adaptive management.
package exec

import (
	"fmt"

	"runtimefilter/plan"
	"runtimefilter/rf"
)

// Attachment binds one runtime-filter handle to the probe-side key columns
// at a particular scan.
type Attachment struct {
	Handle  *rf.Handle
	KeyCols []int // key column indexes in the scan's table schema
	Filter  *rf.RuntimeFilter
	// Hops is the full pushdown path: origin join -> every crossed
	// operator -> the scan.
	Hops []rf.Hop
}

// ScanBinding is the result of pushdown: everything a scan needs to apply
// the filters that reached it.
type ScanBinding struct {
	ScanID  int
	Table   string
	Items   []*Attachment
	Blocked []rf.Hop // filters whose travel stopped at an ancestor join
}

// PlannerContext carries the filter budget while walking the tree.
type PlannerContext struct {
	Cfg rf.Config
	// Handles maps join node id -> probe-side handle, created at planning.
	Handles  map[int]*rf.Handle
	Bindings map[int]*ScanBinding
	// BlockedHops records pushdown paths stopped by an outer-join rule,
	// including cases where no scan binding exists in the subtree.
	BlockedHops []rf.Hop
	nextID      int
}

// NewPlannerContext constructs the planning context.
func NewPlannerContext(cfg rf.Config) *PlannerContext {
	return &PlannerContext{
		Cfg:      cfg,
		Handles:  make(map[int]*rf.Handle),
		Bindings: make(map[int]*ScanBinding),
	}
}

func (pc *PlannerContext) newHandle() *rf.Handle { return rf.NewHandle(pc.Cfg) }

// PlanFilters walks the tree, creates one handle per hash join and pushes
// each join's (not-yet-materialized) filter down the allowed subtree.
func (pc *PlannerContext) PlanFilters(root plan.Node) {
	plan.Walk(root, func(n plan.Node) bool {
		j, ok := n.(*plan.JoinNode)
		if !ok || j.NestedLoop {
			return true
		}
		h := pc.newHandle()
		pc.Handles[j.ID()] = h
		probe := plan.ProbeSide(j)
		_, probeKeys := plan.JoinKeyOf(j)
		probeIdx := make([]int, len(probeKeys))
		for i, k := range probeKeys {
			probeIdx[i] = k.Index
		}
		hop := rf.Hop{
			FromNodeID: j.ID(), FromNode: j.Name(),
			ToNodeID: probe.ID(), ToNode: probe.Name(),
			Action: "PUSHED",
			Note:   fmt.Sprintf("filter generated after build of %s", plan.BuildSide(j).Name()),
		}
		// Enforce the origin join's own preservation rule: an outer join
		// keeps every row of its preserved side, so a filter may not reach
		// a scan on that side.
		leftAllowed, rightAllowed := joinChildrenPushable(j)
		probeAllowed := (j.BuildLeft && rightAllowed) || (!j.BuildLeft && leftAllowed)
		if !probeAllowed {
			hop.Action = "BLOCKED"
			hop.Note = fmt.Sprintf("%s preserves the probe side; runtime filter cannot be pushed", j.Type)
			pc.BlockedHops = append(pc.BlockedHops, hop)
			return true
		}
		pc.push(probe, h, probeIdx, j, []rf.Hop{hop})
		return true
	})
}

// push traverses a probe subtree, remapping key indexes through projection
// and enforcing outer-join preservation rules. hops accumulates the full
// lineage (origin join -> every crossed operator -> scan).
func (pc *PlannerContext) push(n plan.Node, h *rf.Handle, keyCols []int, origin *plan.JoinNode, hops []rf.Hop) {
	switch x := n.(type) {
	case *plan.ScanNode:
		pc.bindScan(x, h, keyCols, hops)

	case *plan.FilterNode:
		// Residual filters never remove the key columns; indexes unchanged.
		next := append(append([]rf.Hop{}, hops...), rf.Hop{
			FromNodeID: x.ID(), FromNode: x.Name(),
			ToNodeID: x.Child.ID(), ToNode: x.Child.Name(),
			Action: "PUSHED", Note: "passes through residual filter",
		})
		pc.push(x.Child, h, keyCols, origin, next)

	case *plan.ProjectNode:
		remapped, ok := remapThroughProject(x, keyCols)
		if !ok {
			pc.block(x, hops, "key column eliminated by projection")
			return
		}
		next := append(append([]rf.Hop{}, hops...), rf.Hop{
			FromNodeID: x.ID(), FromNode: x.Name(),
			ToNodeID: x.Child.ID(), ToNode: x.Child.Name(),
			Action: "PUSHED", Note: "remapped key indexes through projection",
		})
		pc.push(x.Child, h, remapped, origin, next)

	case *plan.JoinNode:
		// A filter may cross another join only into a child that is NOT
		// preserved by that join (outer joins keep all rows of their
		// preserved side, where filtering would change query semantics).
		// Join output layout: left columns first, then right columns.
		leftWidth := len(x.Left.OutTypes())
		inLeft, inRight := colsInRange(keyCols, 0, leftWidth)
		leftAllowed, rightAllowed := joinChildrenPushable(x)
		switch {
		case inLeft && leftAllowed:
			cols := shiftCols(keyCols, 0)
			next := append(append([]rf.Hop{}, hops...), rf.Hop{
				FromNodeID: x.ID(), FromNode: x.Name(),
				ToNodeID: x.Left.ID(), ToNode: x.Left.Name(),
				Action: "PUSHED", Note: fmt.Sprintf("crosses %s into non-preserved left side", x.Type),
			})
			pc.push(x.Left, h, cols, origin, next)
		case inRight && rightAllowed:
			cols := shiftCols(keyCols, leftWidth)
			next := append(append([]rf.Hop{}, hops...), rf.Hop{
				FromNodeID: x.ID(), FromNode: x.Name(),
				ToNodeID: x.Right.ID(), ToNode: x.Right.Name(),
				Action: "PUSHED", Note: fmt.Sprintf("crosses %s into non-preserved right side", x.Type),
			})
			pc.push(x.Right, h, cols, origin, next)
		default:
			pc.block(x, hops, "outer join preserves the side carrying the key; filter cannot cross")
		}
	}
}

func colsInRange(cols []int, lo, hi int) (allInLeft, allInRight bool) {
	allInLeft, allInRight = true, true
	for _, c := range cols {
		if c < lo || c >= hi {
			allInLeft = false
		}
		if c < hi {
			allInRight = false
		}
	}
	return allInLeft, allInRight
}

func shiftCols(cols []int, offset int) []int {
	out := make([]int, len(cols))
	for i, c := range cols {
		out[i] = c - offset
	}
	return out
}

// joinChildrenPushable reports which children may lose rows without
// changing join semantics.
func joinChildrenPushable(j *plan.JoinNode) (left, right bool) {
	switch j.Type {
	case plan.JoinInner:
		return true, true
	case plan.JoinLeft:
		return false, true // left rows preserved
	case plan.JoinRight:
		return true, false // right rows preserved
	default:
		return false, false // full outer preserves both
	}
}

func remapThroughProject(p *plan.ProjectNode, outCols []int) ([]int, bool) {
	in := make([]int, len(outCols))
	for i, c := range outCols {
		if c >= len(p.InputIdx) {
			return nil, false
		}
		in[i] = p.InputIdx[c]
	}
	return in, true
}

func (pc *PlannerContext) bindScan(s *plan.ScanNode, h *rf.Handle, keyCols []int, hops []rf.Hop) {
	b := pc.Bindings[s.ID()]
	if b == nil {
		b = &ScanBinding{ScanID: s.ID(), Table: s.Table.Name}
		pc.Bindings[s.ID()] = b
	}
	// Add the final delivery hop only when the path does not already end
	// on this scan (avoids a scan->scan self hop).
	final := append([]rf.Hop{}, hops...)
	if last := final[len(final)-1]; last.ToNodeID != s.ID() {
		final = append(final, rf.Hop{
			FromNodeID: last.ToNodeID, FromNode: last.ToNode,
			ToNodeID: s.ID(), ToNode: s.Name(),
			Action: "PUSHED", Note: "delivered to scan; applied before block/row reads",
		})
	} else {
		final[len(final)-1].Note = "delivered to scan; applied before block/row reads"
	}
	b.Items = append(b.Items, &Attachment{Handle: h, KeyCols: keyCols, Hops: final})
}

func (pc *PlannerContext) block(n plan.Node, hops []rf.Hop, reason string) {
	hop := hops[len(hops)-1]
	hop.Action = "BLOCKED"
	hop.ToNodeID = n.ID()
	hop.ToNode = n.Name()
	hop.Note = reason
	blocked := append(append([]rf.Hop{}, hops[:len(hops)-1]...), hop)
	// Attach the blocked path onto any scan binding that already exists in
	// this subtree so the dashboard can show where pushdown stopped.
	plan.Walk(n, func(x plan.Node) bool {
		if s, ok := x.(*plan.ScanNode); ok {
			if b := pc.Bindings[s.ID()]; b != nil {
				b.Blocked = append(b.Blocked, blocked...)
				return false
			}
		}
		return true
	})
	pc.BlockedHops = append(pc.BlockedHops, blocked...)
}
