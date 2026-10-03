package processforge

import "errors"

var (
	ErrUnknownNode     = errors.New("processforge: unknown node")
	ErrNotGate         = errors.New("processforge: node is not a review gate")
	ErrStaleRound      = errors.New("processforge: stale round")
	ErrNotReady        = errors.New("processforge: node is not ready")
	ErrAlreadyDone     = errors.New("processforge: node already completed")
	ErrNotApprover     = errors.New("processforge: actor is not a designated approver")
	ErrDuplicateVote   = errors.New("processforge: actor already voted this round")
	ErrInvalidTemplate = errors.New("processforge: invalid template")
)
