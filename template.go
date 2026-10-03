package processforge

import "fmt"

// MaxNodes is the maximum number of nodes a template may contain.
const MaxNodes = 50

// NodeKind distinguishes ordinary tasks from two-person review gates.
type NodeKind int

const (
	// TaskNode is an ordinary task completed by a single Act.
	TaskNode NodeKind = iota
	// GateNode is a two-person review gate completed by votes of both
	// designated approvers.
	GateNode
)

// NodeSpec describes one node of a template.
type NodeSpec struct {
	ID   string
	Kind NodeKind
	// Approvers must list exactly two distinct approver identities for
	// GateNode and must be empty for TaskNode.
	Approvers []string
	// Deps lists IDs of predecessor nodes.
	Deps []string
}

// Template is a directed acyclic graph of nodes.
type Template struct {
	Nodes []NodeSpec
}

// staticTemplate is the validated, immutable form of a Template.
type staticTemplate struct {
	ids      []string    // node IDs in topological order
	kind     []NodeKind  // node kind by index
	approver [][2]string // gate approvers by index (gates only)
	preds    [][]int     // predecessor indexes
	affected [][]int     // per gate: indexes of gate + transitive successors
	index    map[string]int
}

// compileTemplate validates t and derives the static execution data.
func compileTemplate(t Template) (*staticTemplate, error) {
	if len(t.Nodes) == 0 {
		return nil, fmt.Errorf("%w: template has no nodes", ErrInvalidTemplate)
	}
	if len(t.Nodes) > MaxNodes {
		return nil, fmt.Errorf("%w: template has %d nodes, max is %d", ErrInvalidTemplate, len(t.Nodes), MaxNodes)
	}

	index := make(map[string]int, len(t.Nodes))
	for pos, n := range t.Nodes {
		if n.ID == "" {
			return nil, fmt.Errorf("%w: node %d has empty ID", ErrInvalidTemplate, pos)
		}
		if _, dup := index[n.ID]; dup {
			return nil, fmt.Errorf("%w: duplicate node ID %q", ErrInvalidTemplate, n.ID)
		}
		index[n.ID] = pos
	}

	st := &staticTemplate{
		ids:      make([]string, len(t.Nodes)),
		kind:     make([]NodeKind, len(t.Nodes)),
		approver: make([][2]string, len(t.Nodes)),
		preds:    make([][]int, len(t.Nodes)),
		index:    make(map[string]int, len(t.Nodes)),
	}

	// Kahn's algorithm over the original positions; the topological order
	// becomes the canonical node order of the instance.
	indegree := make([]int, len(t.Nodes))
	children := make([][]int, len(t.Nodes))
	for pos, n := range t.Nodes {
		switch n.Kind {
		case TaskNode:
			if len(n.Approvers) != 0 {
				return nil, fmt.Errorf("%w: task node %q must not declare approvers", ErrInvalidTemplate, n.ID)
			}
		case GateNode:
			if len(n.Approvers) != 2 || n.Approvers[0] == "" || n.Approvers[1] == "" {
				return nil, fmt.Errorf("%w: gate node %q needs exactly two approvers", ErrInvalidTemplate, n.ID)
			}
			if n.Approvers[0] == n.Approvers[1] {
				return nil, fmt.Errorf("%w: gate node %q has duplicate approvers", ErrInvalidTemplate, n.ID)
			}
		default:
			return nil, fmt.Errorf("%w: node %q has unknown kind", ErrInvalidTemplate, n.ID)
		}
		seen := make(map[int]bool, len(n.Deps))
		for _, dep := range n.Deps {
			p, ok := index[dep]
			if !ok {
				return nil, fmt.Errorf("%w: node %q depends on unknown node %q", ErrInvalidTemplate, n.ID, dep)
			}
			if p == pos {
				return nil, fmt.Errorf("%w: node %q depends on itself", ErrInvalidTemplate, n.ID)
			}
			if seen[p] {
				continue
			}
			seen[p] = true
			indegree[pos]++
			children[p] = append(children[p], pos)
		}
	}

	queue := make([]int, 0, len(t.Nodes))
	for pos := range t.Nodes {
		if indegree[pos] == 0 {
			queue = append(queue, pos)
		}
	}
	order := make([]int, 0, len(t.Nodes))
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		order = append(order, p)
		for _, c := range children[p] {
			indegree[c]--
			if indegree[c] == 0 {
				queue = append(queue, c)
			}
		}
	}
	if len(order) != len(t.Nodes) {
		return nil, fmt.Errorf("%w: template contains a dependency cycle", ErrInvalidTemplate)
	}

	// Remap everything into topological order.
	remap := make([]int, len(t.Nodes)) // old position -> new index
	for newIdx, oldPos := range order {
		remap[oldPos] = newIdx
	}
	for newIdx, oldPos := range order {
		n := t.Nodes[oldPos]
		st.ids[newIdx] = n.ID
		st.kind[newIdx] = n.Kind
		if n.Kind == GateNode {
			st.approver[newIdx] = [2]string{n.Approvers[0], n.Approvers[1]}
		}
		st.index[n.ID] = newIdx
		seen := make(map[int]bool, len(n.Deps))
		for _, dep := range n.Deps {
			p := remap[index[dep]]
			if seen[p] {
				continue
			}
			seen[p] = true
			st.preds[newIdx] = append(st.preds[newIdx], p)
		}
	}
	// Rebuild children in the new indexing for descendant computation.
	topoChildren := make([][]int, len(t.Nodes))
	for newIdx := range st.preds {
		for _, p := range st.preds[newIdx] {
			topoChildren[p] = append(topoChildren[p], newIdx)
		}
	}

	// For every gate compute gate + transitive successors, in topological
	// (canonical) order so concurrent rechecks lock in the same order.
	st.affected = make([][]int, len(t.Nodes))
	for g := range st.ids {
		if st.kind[g] != GateNode {
			continue
		}
		mark := make([]bool, len(st.ids))
		mark[g] = true
		stack := append([]int(nil), topoChildren[g]...)
		for len(stack) > 0 {
			c := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if mark[c] {
				continue
			}
			mark[c] = true
			stack = append(stack, topoChildren[c]...)
		}
		set := make([]int, 0, 4)
		for idx, m := range mark {
			if m {
				set = append(set, idx)
			}
		}
		st.affected[g] = set
	}
	return st, nil
}
