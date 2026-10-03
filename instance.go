package processforge

import (
	"sync"
	"sync/atomic"
)

// nodeState is the immutable state of a single node. It is only ever
// replaced wholesale, never mutated in place, so readers always observe a
// consistent per-node value.
type nodeState struct {
	round   uint64
	done    bool
	tickets []string // approvers who voted this round (gates only)
}

// nodeCell holds the mutable cell of one node.
type nodeCell struct {
	// mu serializes Recheck publishes touching this node. Act and View
	// never take it.
	mu    sync.Mutex
	state atomic.Pointer[nodeState]
}

// Instance is a running review process created from a Template. Act,
// Recheck and View may be called concurrently from any goroutines.
type Instance struct {
	staticTemplate

	cells []nodeCell

	// recheckStarted / recheckDone bracket the publish phase of Recheck.
	// View uses them to avoid reading a partially published recheck.
	recheckStarted atomic.Uint64
	recheckDone    atomic.Uint64

	beforePublish func(HookEvent)
}

// Create validates the template and returns a new instance. Every node
// starts at round 1 with no results and no tickets.
func Create(t Template, opts ...Option) (*Instance, error) {
	st, err := compileTemplate(t)
	if err != nil {
		return nil, err
	}
	i := &Instance{staticTemplate: *st}
	i.cells = make([]nodeCell, len(st.ids))
	for idx := range i.cells {
		i.cells[idx].state.Store(&nodeState{round: 1})
	}
	for _, opt := range opts {
		opt(i)
	}
	return i, nil
}

// predsDone reports whether every predecessor of node idx is complete.
func (i *Instance) predsDone(idx int) bool {
	for _, p := range i.preds[idx] {
		if !i.cells[p].state.Load().done {
			return false
		}
	}
	return true
}

// fireBeforePublish invokes the test pause point, if one is registered.
func (i *Instance) fireBeforePublish(ev HookEvent) {
	if i.beforePublish != nil {
		i.beforePublish(ev)
	}
}
