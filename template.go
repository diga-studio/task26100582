package processforge

import (
	"fmt"
	"sort"
	"sync"
)

const maxNodes = 50

// node is the mutable runtime state of one template node. Its lock only ever
// guards this node; multi-node operations lock several nodes in ascending
// topological index order, so no instance-wide lock exists and no lock-order
// cycle can form.
type node struct {
	id        string
	kind      NodeKind
	approvers [2]string
	index     int   // topological position; every predecessor has a smaller index
	preds     []int // predecessor indices, ascending
	succs     []int // successor indices, ascending
	affected  []int // self plus all transitive successors, ascending

	mu      sync.Mutex
	round   int
	done    bool
	tickets []string // approver identities collected in the current round
}

// Instance is a running execution of a Template.
type Instance struct {
	nodes []*node // ordered by topological index
	byID  map[string]*node
	hooks *Hooks
}

// Create validates the template and starts an instance with all rounds at 1.
func Create(tpl Template) (*Instance, error) {
	return CreateWithHooks(tpl, nil)
}

// CreateWithHooks is Create plus test pause points.
func CreateWithHooks(tpl Template, hooks *Hooks) (*Instance, error) {
	if len(tpl.Nodes) > maxNodes {
		return nil, fmt.Errorf("%w: more than %d nodes", ErrInvalidTemplate, maxNodes)
	}
	byID := make(map[string]*node, len(tpl.Nodes))
	for _, spec := range tpl.Nodes {
		if spec.ID == "" {
			return nil, fmt.Errorf("%w: empty node ID", ErrInvalidTemplate)
		}
		if _, dup := byID[spec.ID]; dup {
			return nil, fmt.Errorf("%w: duplicate node ID %q", ErrInvalidTemplate, spec.ID)
		}
		if spec.Kind != KindTask && spec.Kind != KindGate {
			return nil, fmt.Errorf("%w: node %q has unknown kind", ErrInvalidTemplate, spec.ID)
		}
		if spec.Kind == KindGate {
			if spec.Approvers[0] == "" || spec.Approvers[1] == "" {
				return nil, fmt.Errorf("%w: gate %q needs two approvers", ErrInvalidTemplate, spec.ID)
			}
			if spec.Approvers[0] == spec.Approvers[1] {
				return nil, fmt.Errorf("%w: gate %q approvers must differ", ErrInvalidTemplate, spec.ID)
			}
		}
		byID[spec.ID] = &node{
			id:        spec.ID,
			kind:      spec.Kind,
			approvers: spec.Approvers,
			round:     1,
		}
	}

	// Wire edges and run Kahn's algorithm for a topological order.
	indegree := make(map[string]int, len(byID))
	adj := make(map[string][]string, len(byID))
	for id := range byID {
		indegree[id] = 0
	}
	for _, spec := range tpl.Nodes {
		for _, dep := range spec.Deps {
			if _, ok := byID[dep]; !ok {
				return nil, fmt.Errorf("%w: node %q depends on unknown %q", ErrInvalidTemplate, spec.ID, dep)
			}
			adj[dep] = append(adj[dep], spec.ID)
			indegree[spec.ID]++
		}
	}
	queue := make([]string, 0, len(byID))
	for id, d := range indegree {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)
	var order []string
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)
		next := append([]string(nil), adj[id]...)
		sort.Strings(next)
		for _, m := range next {
			indegree[m]--
			if indegree[m] == 0 {
				queue = append(queue, m)
			}
		}
	}
	if len(order) != len(byID) {
		return nil, fmt.Errorf("%w: template has a cycle", ErrInvalidTemplate)
	}

	nodes := make([]*node, len(order))
	for i, id := range order {
		n := byID[id]
		n.index = i
		nodes[i] = n
	}
	for _, spec := range tpl.Nodes {
		n := byID[spec.ID]
		for _, dep := range spec.Deps {
			n.preds = append(n.preds, byID[dep].index)
		}
		sort.Ints(n.preds)
	}
	for _, spec := range tpl.Nodes {
		for _, dep := range spec.Deps {
			pred := byID[dep]
			pred.succs = append(pred.succs, byID[spec.ID].index)
		}
	}
	for _, n := range nodes {
		sort.Ints(n.succs)
	}

	// Precompute each node's affected set (self plus transitive successors)
	// in reverse topological order; Recheck uses the gate's set.
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		set := map[int]struct{}{n.index: {}}
		for _, s := range n.succs {
			for _, x := range nodes[s].affected {
				set[x] = struct{}{}
			}
		}
		n.affected = n.affected[:0]
		for x := range set {
			n.affected = append(n.affected, x)
		}
		sort.Ints(n.affected)
	}

	return &Instance{nodes: nodes, byID: byID, hooks: hooks}, nil
}
