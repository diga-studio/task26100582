package processforge

import "runtime"

// Status is the lifecycle state of a node in a snapshot.
type Status int

const (
	// StatusPending means at least one predecessor is not complete.
	StatusPending Status = iota
	// StatusReady means all predecessors are complete; the node accepts acts.
	StatusReady
	// StatusDone means the node is complete.
	StatusDone
)

// NodeView is one node's snapshot entry.
type NodeView struct {
	ID      string
	Round   uint64
	Status  Status
	Tickets []string
}

// Snapshot is a consistent, immutable view of all nodes. Nodes appear in
// topological order. A Snapshot is never affected by later operations.
type Snapshot struct {
	Nodes []NodeView
}

// Get returns the snapshot entry for the given node ID.
func (s Snapshot) Get(nodeID string) (NodeView, bool) {
	for _, n := range s.Nodes {
		if n.ID == nodeID {
			return n, true
		}
	}
	return NodeView{}, false
}

// View returns a consistent snapshot of every node's round, status and
// collected tickets. The snapshot always corresponds to some serial
// execution of the completed operations: in particular it never observes
// only part of a Recheck.
func (i *Instance) View() Snapshot {
	for {
		started := i.recheckStarted.Load()
		done := i.recheckDone.Load()
		if started != done {
			// A recheck is publishing right now; wait it out. Publishes
			// never pause mid-way, so this terminates promptly.
			runtime.Gosched()
			continue
		}
		states := make([]*nodeState, len(i.cells))
		for idx := range i.cells {
			states[idx] = i.cells[idx].state.Load()
		}
		if i.recheckStarted.Load() != started || i.recheckDone.Load() != done {
			// A recheck published while we were reading; retry.
			runtime.Gosched()
			continue
		}
		return i.buildSnapshot(states)
	}
}

func (i *Instance) buildSnapshot(states []*nodeState) Snapshot {
	snap := Snapshot{Nodes: make([]NodeView, len(states))}
	for idx, st := range states {
		nv := NodeView{ID: i.ids[idx], Round: st.round}
		switch {
		case st.done:
			nv.Status = StatusDone
		case i.predsDoneIn(idx, states):
			nv.Status = StatusReady
		default:
			nv.Status = StatusPending
		}
		if len(st.tickets) > 0 {
			nv.Tickets = append([]string(nil), st.tickets...)
		}
		snap.Nodes[idx] = nv
	}
	return snap
}

func (i *Instance) predsDoneIn(idx int, states []*nodeState) bool {
	for _, p := range i.preds[idx] {
		if !states[p].done {
			return false
		}
	}
	return true
}
