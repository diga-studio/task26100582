package processforge

// Recheck sends a ready or passed gate back for re-review. The gate and
// all its transitive successors advance one round; their completion
// results and tickets are invalidated. Every other node keeps its state
// and round. The given round must still be the gate's current round,
// otherwise the call fails with ErrStaleRound and has no effect.
//
// The publish phase is atomic with respect to View: a snapshot either
// reflects the whole recheck or none of it.
func (i *Instance) Recheck(gateID string, round uint64) error {
	g, ok := i.index[gateID]
	if !ok {
		return ErrUnknownNode
	}
	if i.kind[g] != GateNode {
		return ErrNotGate
	}

	// Validation phase: read state without any lock. The authoritative
	// check is repeated under the node locks below.
	s := i.cells[g].state.Load()
	if s.round != round {
		return ErrStaleRound
	}
	if !i.predsDone(g) {
		return ErrNotReady
	}

	// Test pause point: validated, nothing published yet. No lock is
	// held here, so a paused Recheck never blocks other operations.
	i.fireBeforePublish(HookEvent{Op: OpRecheck, Node: gateID, Round: round})

	// Publish phase. recheckStarted/recheckDone let View detect and avoid
	// reading a partially published recheck.
	i.recheckStarted.Add(1)
	affected := i.affected[g]
	for _, idx := range affected {
		i.cells[idx].mu.Lock()
	}

	// Re-validate now that every affected node is frozen. Any recheck
	// that could have changed the gate's round or its predecessors'
	// results also covers the gate itself and therefore serialized with
	// us on these locks.
	gs := i.cells[g].state.Load()
	if gs.round != round {
		i.unlockAffected(affected)
		i.recheckDone.Add(1)
		return ErrStaleRound
	}
	if !i.predsDone(g) {
		i.unlockAffected(affected)
		i.recheckDone.Add(1)
		return ErrNotReady
	}

	for _, idx := range affected {
		cur := i.cells[idx].state.Load()
		i.cells[idx].state.Store(&nodeState{round: cur.round + 1})
	}
	i.unlockAffected(affected)
	i.recheckDone.Add(1)
	return nil
}

func (i *Instance) unlockAffected(affected []int) {
	for k := len(affected) - 1; k >= 0; k-- {
		i.cells[affected[k]].mu.Unlock()
	}
}
