package processforge

// Hooks are optional test pause points. A hook runs after the operation has
// read state and finished validation, but before its result is published.
// Hooks run without holding any node lock, so other operations on the same
// instance can proceed while a hook blocks.
type Hooks struct {
	// ActValidated fires inside Act after validation, before publishing.
	ActValidated func(nodeID string, round int, actor string)
	// RecheckValidated fires inside Recheck after validation, before publishing.
	RecheckValidated func(gateID string, round int)
}
