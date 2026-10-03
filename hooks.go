package processforge

// Op identifies the operation kind reported to hooks.
type Op int

const (
	// OpAct is an Act operation.
	OpAct Op = iota
	// OpRecheck is a Recheck operation.
	OpRecheck
)

// HookEvent describes the operation reaching a hook point.
type HookEvent struct {
	Op    Op
	Node  string
	Round uint64
	// Actor is set for OpAct events.
	Actor string
}

// Option configures an Instance at creation time.
type Option func(*Instance)

// WithBeforePublishHook registers a hook invoked by Act and Recheck after
// they have read state and completed validation, but before they publish
// any result. The hook runs outside every lock, so other operations on the
// same instance keep making progress while a hook blocks. It is a test
// pause point and must be set before any concurrent use.
func WithBeforePublishHook(h func(HookEvent)) Option {
	return func(i *Instance) {
		i.beforePublish = h
	}
}
