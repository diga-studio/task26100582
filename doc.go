// Package processforge executes in-process review workflows described by
// directed acyclic templates of at most MaxNodes nodes. A node is either
// an ordinary task, completed by a single Act, or a two-person review
// gate, completed once both designated approvers have voted in the same
// round.
//
// An Instance is created with Create; every node starts at round 1. Act
// completes tasks and casts gate votes. Recheck sends a ready or passed
// gate back for re-review: the gate and all its transitive successors
// advance one round and lose their results and tickets, while every
// other node is untouched. View returns a consistent, immutable snapshot
// of all nodes.
//
// Act, Recheck and View are safe for concurrent use. Their results can
// always be explained by some serial execution respecting the order of
// the calls: Act publishes with a per-node compare-and-swap, Recheck
// publishes atomically under per-node locks taken in a canonical order,
// and View never observes a partially published Recheck. A call carrying
// a round that is no longer current fails with ErrStaleRound and has no
// side effects.
//
// Caller identities are trusted; the package keeps no state outside the
// process and performs no persistence or crash recovery.
package processforge
