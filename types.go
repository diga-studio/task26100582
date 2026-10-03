package processforge

import "errors"

// NodeKind distinguishes ordinary tasks from dual-approver review gates.
type NodeKind int

const (
	KindTask NodeKind = iota
	KindGate
)

// Status is the derived lifecycle state of a node in a snapshot.
type Status int

const (
	StatusPending Status = iota // waiting on predecessors
	StatusReady                 // all predecessors done, accepts actions
	StatusDone                  // completed
)

// NodeSpec describes one template node.
type NodeSpec struct {
	ID        string
	Kind      NodeKind
	Approvers [2]string // gates only: two distinct approver identities
	Deps      []string  // predecessor node IDs
}

// Template is a validated-on-create DAG of at most 50 nodes.
type Template struct {
	Nodes []NodeSpec
}

// NodeView is one node's consistent snapshot entry.
type NodeView struct {
	ID      string
	Round   int
	Status  Status
	Tickets []string
}

// Snapshot is an immutable, consistent view of all nodes.
type Snapshot struct {
	Nodes []NodeView
}

// ByID returns the view for a node ID and whether it exists.
func (s Snapshot) ByID(id string) (NodeView, bool) {
	for _, n := range s.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeView{}, false
}

var (
	ErrInvalidTemplate    = errors.New("processforge: invalid template")
	ErrUnknownNode        = errors.New("processforge: unknown node")
	ErrNotGate            = errors.New("processforge: node is not a review gate")
	ErrStaleRound         = errors.New("processforge: stale round")
	ErrNotReady           = errors.New("processforge: node not ready")
	ErrAlreadyDone        = errors.New("processforge: node already done")
	ErrNotApprover        = errors.New("processforge: actor is not an approver")
	ErrDuplicateVote      = errors.New("processforge: actor already voted this round")
	ErrGateNotRecheckable = errors.New("processforge: gate is neither ready nor passed")
)
