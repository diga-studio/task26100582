package processforge

// lockAll locks the given node indices (ascending) and returns an unlock
// func. Every multi-node critical section in this package acquires locks in
// ascending topological index order, which keeps locking deadlock-free
// without any instance-wide mutex.
func (in *Instance) lockAll(idxs []int) func() {
	for _, i := range idxs {
		in.nodes[i].mu.Lock()
	}
	return func() {
		for j := len(idxs) - 1; j >= 0; j-- {
			in.nodes[idxs[j]].mu.Unlock()
		}
	}
}

// withPreds returns the node's predecessor indices plus itself, ascending.
func withPreds(n *node) []int {
	idxs := make([]int, 0, len(n.preds)+1)
	idxs = append(idxs, n.preds...)
	idxs = append(idxs, n.index)
	return idxs
}

// readyLocked reports whether all predecessors are done. Callers must hold
// the locks of every predecessor (View holds all locks, so it qualifies).
func (in *Instance) readyLocked(n *node) bool {
	for _, p := range n.preds {
		if !in.nodes[p].done {
			return false
		}
	}
	return true
}

func isApprover(n *node, actor string) bool {
	return n.approvers[0] == actor || n.approvers[1] == actor
}

func hasVoted(n *node, actor string) bool {
	for _, t := range n.tickets {
		if t == actor {
			return true
		}
	}
	return false
}

// Act completes a task or casts one approval vote on a gate.
//
// It runs in two phases: validate (locks predecessors + self, then releases
// them), then publish (locks self only and revalidates). Between the phases
// the optional test hook runs without any lock held, so a paused Act never
// blocks other operations on the instance. Revalidation at publish only
// needs the node's own round: any Recheck that would invalidate this node's
// readiness also bumps this node's round, because the node is a transitive
// successor of the rechecked gate.
func (in *Instance) Act(nodeID string, round int, actor string) error {
	n, ok := in.byID[nodeID]
	if !ok {
		return ErrUnknownNode
	}

	unlock := in.lockAll(withPreds(n))
	err := validateAct(in, n, round, actor)
	unlock()
	if err != nil {
		return err
	}

	if in.hooks != nil && in.hooks.ActValidated != nil {
		in.hooks.ActValidated(nodeID, round, actor)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	return publishAct(n, round, actor)
}

func validateAct(in *Instance, n *node, round int, actor string) error {
	if n.round != round {
		return ErrStaleRound
	}
	if n.done {
		return ErrAlreadyDone
	}
	if !in.readyLocked(n) {
		return ErrNotReady
	}
	if n.kind == KindGate {
		if !isApprover(n, actor) {
			return ErrNotApprover
		}
		if hasVoted(n, actor) {
			return ErrDuplicateVote
		}
	}
	return nil
}

func publishAct(n *node, round int, actor string) error {
	if n.round != round {
		return ErrStaleRound
	}
	if n.done {
		return ErrAlreadyDone
	}
	if n.kind == KindGate {
		if !isApprover(n, actor) {
			return ErrNotApprover
		}
		if hasVoted(n, actor) {
			return ErrDuplicateVote
		}
		n.tickets = append(n.tickets, actor)
		if len(n.tickets) == len(n.approvers) {
			n.done = true
		}
		return nil
	}
	n.done = true
	return nil
}

// Recheck sends a ready or passed gate back for re-review: the gate and all
// its transitive successors advance one round and lose every completion
// result and ticket. Nodes outside the affected set keep their state.
//
// Like Act it validates first, then publishes under the locks of exactly the
// affected nodes in ascending index order, so the whole recheck takes effect
// atomically with respect to View and other operations.
func (in *Instance) Recheck(gateID string, round int) error {
	g, ok := in.byID[gateID]
	if !ok {
		return ErrUnknownNode
	}
	if g.kind != KindGate {
		return ErrNotGate
	}

	unlock := in.lockAll(withPreds(g))
	err := validateRecheck(in, g, round)
	unlock()
	if err != nil {
		return err
	}

	if in.hooks != nil && in.hooks.RecheckValidated != nil {
		in.hooks.RecheckValidated(gateID, round)
	}

	unlock = in.lockAll(g.affected)
	defer unlock()
	if g.round != round {
		return ErrStaleRound
	}
	for _, i := range g.affected {
		n := in.nodes[i]
		n.round++
		n.done = false
		n.tickets = nil
	}
	return nil
}

func validateRecheck(in *Instance, g *node, round int) error {
	if g.round != round {
		return ErrStaleRound
	}
	if !g.done && !in.readyLocked(g) {
		return ErrGateNotRecheckable
	}
	return nil
}

// View returns a consistent snapshot of every node. It briefly locks all
// nodes in ascending index order and copies their state, so the returned
// snapshot reflects one serializable point and can never be mutated by later
// operations.
func (in *Instance) View() Snapshot {
	idxs := make([]int, len(in.nodes))
	for i := range in.nodes {
		idxs[i] = i
	}
	unlock := in.lockAll(idxs)
	defer unlock()

	snap := Snapshot{Nodes: make([]NodeView, len(in.nodes))}
	for i, n := range in.nodes {
		status := StatusPending
		switch {
		case n.done:
			status = StatusDone
		case in.readyLocked(n):
			status = StatusReady
		}
		snap.Nodes[i] = NodeView{
			ID:      n.id,
			Round:   n.round,
			Status:  status,
			Tickets: append([]string(nil), n.tickets...),
		}
	}
	return snap
}
