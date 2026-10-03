package processforge

// Act completes a task node, or casts one approval vote on a review gate.
// The given round must still be the node's current round, otherwise the
// call fails with ErrStaleRound and has no effect. Act only ever touches
// the target node's own state, so it never disturbs concurrent operations
// on other nodes.
func (i *Instance) Act(nodeID string, round uint64, actor string) error {
	idx, ok := i.index[nodeID]
	if !ok {
		return ErrUnknownNode
	}
	cell := &i.cells[idx]
	isGate := i.kind[idx] == GateNode

	hooked := false
	for {
		s := cell.state.Load()
		if s.round != round {
			return ErrStaleRound
		}
		if s.done {
			return ErrAlreadyDone
		}
		if !i.predsDone(idx) {
			return ErrNotReady
		}

		var newTickets []string
		if isGate {
			ap := i.approver[idx]
			if actor != ap[0] && actor != ap[1] {
				return ErrNotApprover
			}
			for _, t := range s.tickets {
				if t == actor {
					return ErrDuplicateVote
				}
			}
			newTickets = make([]string, len(s.tickets)+1)
			copy(newTickets, s.tickets)
			newTickets[len(s.tickets)] = actor
		}

		// Test pause point: state has been read and validated, nothing
		// published yet. Fired at most once per call and outside any lock,
		// so a paused Act never blocks other operations.
		if !hooked {
			hooked = true
			i.fireBeforePublish(HookEvent{Op: OpAct, Node: nodeID, Round: round, Actor: actor})
		}

		ns := &nodeState{round: s.round, tickets: newTickets}
		if !isGate || len(newTickets) == 2 {
			ns.done = true
		}
		if cell.state.CompareAndSwap(s, ns) {
			return nil
		}
		// Lost a race against another publish on this same node (a
		// concurrent Act, or a Recheck that bumped the round). Re-read
		// and re-validate; a round change surfaces as ErrStaleRound.
	}
}
