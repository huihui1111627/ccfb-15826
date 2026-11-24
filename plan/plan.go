// Package plan defines the logical/physical query plan tree used by the
// runtime-filter demo. Plans are constructed programmatically in the test
// generator; there is no SQL parser.
package plan

import (
	"fmt"
	"strings"

	"runtimefilter/rel"
)

// JoinType enumerates the implemented hash/nested-loop join variants.
type JoinType uint8

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
)

func (j JoinType) String() string {
	switch j {
	case JoinLeft:
		return "LEFT OUTER"
	case JoinRight:
		return "RIGHT OUTER"
	case JoinFull:
		return "FULL OUTER"
	default:
		return "INNER"
	}
}

// Node is any plan-tree node.
type Node interface {
	ID() int
	Name() string
	Children() []Node
	OutTypes() []rel.Kind
	OutNames() []string
	describe() string
}

type BaseInfo struct {
	id    int
	name  string
	types []rel.Kind
	cols  []string
}

// NewBase constructs the common node header. Exported for the test
// generator package.
func NewBase(id int, name string, types []rel.Kind, cols []string) BaseInfo {
	return BaseInfo{id: id, name: name, types: types, cols: cols}
}

// Base is the exported alias used by external packages.
type Base = BaseInfo

func (b BaseInfo) ID() int              { return b.id }
func (b BaseInfo) Name() string         { return b.name }
func (b BaseInfo) OutTypes() []rel.Kind { return b.types }
func (b BaseInfo) OutNames() []string   { return b.cols }

// ScanNode reads a base table.
type ScanNode struct {
	Base
	Table *rel.Table
}

func (s *ScanNode) Children() []Node { return nil }
func (s *ScanNode) describe() string {
	return fmt.Sprintf("Scan %s (%d rows, %d blocks)", s.Table.Name, s.Table.Rows, len(s.Table.Blocks))
}

// Predicate is one residual (non-join) comparison: Col OP Literal.
type Predicate struct {
	Col     string
	ColIdx  int
	Op      string // =, !=, <, <=, >, >=
	Literal rel.Value
}

func (p Predicate) Eval(r rel.Row) bool {
	v := r[p.ColIdx]
	if v.IsNull() || p.Literal.IsNull() {
		return false
	}
	c := rel.Compare(v, p.Literal)
	switch p.Op {
	case "=":
		return c == 0
	case "!=", "<>":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	}
	return false
}

// FilterNode applies residual predicates.
type FilterNode struct {
	Base
	Child Node
	Preds []Predicate
}

func (f *FilterNode) Children() []Node { return []Node{f.Child} }
func (f *FilterNode) describe() string {
	parts := make([]string, 0, len(f.Preds))
	for _, p := range f.Preds {
		parts = append(parts, fmt.Sprintf("%s %s %s", p.Col, p.Op, p.Literal.StringN()))
	}
	return "Filter " + strings.Join(parts, " AND ")
}

// ProjectNode keeps a subset of columns (and renames nothing in this demo).
type ProjectNode struct {
	Base
	Child Node
	// InputIdx maps output column i to the child's column index.
	InputIdx []int
}

func (p *ProjectNode) Children() []Node { return []Node{p.Child} }
func (p *ProjectNode) describe() string {
	return "Project " + strings.Join(p.cols, ", ")
}

// KeyCol identifies one join key column on one side.
type KeyCol struct {
	Col   string
	Index int
	Kind  rel.Kind
}

// JoinNode is a hash join (or nested loop in the reference plan).
type JoinNode struct {
	Base
	Left       Node
	Right      Node
	Type       JoinType
	LeftKeys   []KeyCol
	RightKeys  []KeyCol
	BuildLeft  bool // build side is the left child when true
	BuildRows  int  // estimate used for build-side selection
	Shards     int  // parallel build partitions (1 = serial)
	BuildDelay int  // simulated build latency in milliseconds (async tests)
	NestedLoop bool // reference implementation marker
}

func (j *JoinNode) Children() []Node { return []Node{j.Left, j.Right} }

func (j *JoinNode) describe() string {
	kind := "Hash Join"
	if j.NestedLoop {
		kind = "Nested Loop"
	}
	side := "build=right"
	if j.BuildLeft {
		side = "build=left"
	}
	return fmt.Sprintf("%s %s [%s, shards=%d]", kind, j.Type, side, j.Shards)
}

// Print renders an indented plan tree for logs and the dashboard.
func Print(n Node) string {
	var sb strings.Builder
	printNode(&sb, n, "", true)
	return sb.String()
}

func printNode(sb *strings.Builder, n Node, prefix string, last bool) {
	connector := "├── "
	if last {
		connector = "└── "
	}
	sb.WriteString(prefix + connector + n.Name() + " :: " + n.describe() + "\n")
	kids := n.Children()
	next := prefix
	if !last {
		next += "│   "
	} else {
		next += "    "
	}
	for i, c := range kids {
		printNode(sb, c, next, i == len(kids)-1)
	}
}

// Walk applies fn to every node in pre-order.
func Walk(n Node, fn func(Node) bool) {
	if !fn(n) {
		return
	}
	for _, c := range n.Children() {
		Walk(c, fn)
	}
}

// FindScan returns the scan node whose table name matches.
func FindScan(n Node, table string) *ScanNode {
	var found *ScanNode
	Walk(n, func(x Node) bool {
		if s, ok := x.(*ScanNode); ok && s.Table.Name == table {
			found = s
			return false
		}
		return true
	})
	return found
}

// JoinKeyOf returns the build- and probe-side key descriptors of a join.
func JoinKeyOf(j *JoinNode) (build, probe []KeyCol) {
	if j.BuildLeft {
		return j.LeftKeys, j.RightKeys
	}
	return j.RightKeys, j.LeftKeys
}

// ProbeSide returns the probe child of a join.
func ProbeSide(j *JoinNode) Node {
	if j.BuildLeft {
		return j.Right
	}
	return j.Left
}

// BuildSide returns the build child of a join.
func BuildSide(j *JoinNode) Node {
	if j.BuildLeft {
		return j.Left
	}
	return j.Right
}
