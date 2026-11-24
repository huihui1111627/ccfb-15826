package exec

import (
	"io"

	"runtimefilter/plan"
	"runtimefilter/rel"
)

// nestedLoopExec is the block-nested-loop reference implementation used for
// correctness cross-checking. It materializes both sides and applies the
// same null-aware equality and outer-join semantics as the hash join.
type nestedLoopExec struct {
	node  *plan.JoinNode
	q     *QueryContext
	left  Executor
	right Executor
	stats *OpStats

	initialized bool
	out         []rel.Row
	idx         int

	leftRows  []rel.Row
	rightRows []rel.Row
}

func newNestedLoop(q *QueryContext, j *plan.JoinNode) *nestedLoopExec {
	return &nestedLoopExec{
		node:  j,
		q:     q,
		left:  Build(q, j.Left),
		right: Build(q, j.Right),
		stats: newStats(j, "NestedLoop"),
	}
}

func (n *nestedLoopExec) Children() []Executor { return []Executor{n.left, n.right} }
func (n *nestedLoopExec) Stats() *OpStats      { return n.stats }
func (n *nestedLoopExec) Close() {
	n.left.Close()
	n.right.Close()
	n.stats.end()
}

func (n *nestedLoopExec) init() error {
	n.initialized = true
	t0 := n.stats.buildStart()
	n.leftRows = drainAll(n.left)
	n.rightRows = drainAll(n.right)
	width := len(n.node.Left.OutTypes()) + len(n.node.Right.OutTypes())
	leftMatched := make([]bool, len(n.leftRows))
	rightMatched := make([]bool, len(n.rightRows))

	for li, lr := range n.leftRows {
		for ri, rr := range n.rightRows {
			if n.keysEqual(lr, rr) {
				out := make(rel.Row, width)
				copy(out, lr)
				copy(out[len(lr):], rr)
				n.out = append(n.out, out)
				leftMatched[li] = true
				rightMatched[ri] = true
			}
		}
	}
	switch n.node.Type {
	case plan.JoinLeft, plan.JoinFull:
		for li, lr := range n.leftRows {
			if leftMatched[li] {
				continue
			}
			out := make(rel.Row, width)
			for i := range out {
				out[i] = rel.Null()
			}
			copy(out, lr)
			n.out = append(n.out, out)
		}
	}
	switch n.node.Type {
	case plan.JoinRight, plan.JoinFull:
		for ri, rr := range n.rightRows {
			if rightMatched[ri] {
				continue
			}
			out := make(rel.Row, width)
			for i := range out {
				out[i] = rel.Null()
			}
			copy(out[len(n.node.Left.OutTypes()):], rr)
			n.out = append(n.out, out)
		}
	}
	n.stats.buildEnd(t0)
	return nil
}

func (n *nestedLoopExec) keysEqual(lr, rr rel.Row) bool {
	for i := range n.node.LeftKeys {
		lv := lr[n.node.LeftKeys[i].Index]
		rv := rr[n.node.RightKeys[i].Index]
		if lv.IsNull() || rv.IsNull() {
			return false
		}
		if rel.Compare(lv, rv) != 0 {
			return false
		}
	}
	return true
}

func (n *nestedLoopExec) Next() (rel.Row, error) {
	if !n.initialized {
		if err := n.init(); err != nil {
			return nil, err
		}
	}
	if n.idx >= len(n.out) {
		return nil, io.EOF
	}
	r := n.out[n.idx]
	n.idx++
	n.stats.Rows++
	return r, nil
}
