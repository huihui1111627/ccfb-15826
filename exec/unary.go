package exec

import (
	"io"

	"runtimefilter/plan"
	"runtimefilter/rel"
)

type filterExec struct {
	node  *plan.FilterNode
	child Executor
	stats *OpStats
}

func (f *filterExec) Children() []Executor { return []Executor{f.child} }
func (f *filterExec) Stats() *OpStats      { return f.stats }
func (f *filterExec) Close()               { f.child.Close(); f.stats.end() }

func (f *filterExec) Next() (rel.Row, error) {
	f.stats.begin()
	defer f.stats.end()
	for {
		r, err := f.child.Next()
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		ok := true
		for _, p := range f.node.Preds {
			if !p.Eval(r) {
				ok = false
				break
			}
		}
		if ok {
			f.stats.Rows++
			return r, nil
		}
	}
}

type projectExec struct {
	node  *plan.ProjectNode
	child Executor
	stats *OpStats
}

func (p *projectExec) Children() []Executor { return []Executor{p.child} }
func (p *projectExec) Stats() *OpStats      { return p.stats }
func (p *projectExec) Close()               { p.child.Close(); p.stats.end() }

func (p *projectExec) Next() (rel.Row, error) {
	p.stats.begin()
	defer p.stats.end()
	r, err := p.child.Next()
	if err != nil {
		return nil, err
	}
	out := make(rel.Row, len(p.node.InputIdx))
	for i, idx := range p.node.InputIdx {
		out[i] = r[idx]
	}
	p.stats.Rows++
	return out, nil
}
