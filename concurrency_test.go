package processforge_test

import (
	"errors"
	"sync"
	"testing"

	"processforge"
)

// pausePoint pairs an arrival signal with a release gate for one paused
// operation.
type pausePoint struct {
	arrived chan struct{}
	release chan struct{}
}

func newPausePoint() *pausePoint {
	return &pausePoint{arrived: make(chan struct{}), release: make(chan struct{})}
}

// hook returns the hook function pausing on this point.
func (p *pausePoint) hook() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			close(p.arrived)
			<-p.release
		})
	}
}

// checkConsistent verifies the invariants any legal serial snapshot must
// satisfy for the given template.
func checkConsistent(t *testing.T, tmpl processforge.Template, snap processforge.Snapshot) {
	t.Helper()
	if len(snap.Nodes) != len(tmpl.Nodes) {
		t.Errorf("snapshot has %d nodes, template has %d", len(snap.Nodes), len(tmpl.Nodes))
		return
	}
	spec := make(map[string]processforge.NodeSpec, len(tmpl.Nodes))
	for _, n := range tmpl.Nodes {
		spec[n.ID] = n
	}
	view := make(map[string]processforge.NodeView, len(snap.Nodes))
	for _, nv := range snap.Nodes {
		view[nv.ID] = nv
	}
	for _, nv := range snap.Nodes {
		sp, ok := spec[nv.ID]
		if !ok {
			t.Errorf("snapshot has unknown node %q", nv.ID)
			continue
		}
		if nv.Round < 1 {
			t.Errorf("node %s: round %d < 1", nv.ID, nv.Round)
		}
		// Ready or Done implies every predecessor is Done.
		if nv.Status != processforge.StatusPending {
			for _, dep := range sp.Deps {
				if view[dep].Status != processforge.StatusDone {
					t.Errorf("node %s is %v but predecessor %s is %v", nv.ID, nv.Status, dep, view[dep].Status)
				}
			}
		}
		switch sp.Kind {
		case processforge.TaskNode:
			if len(nv.Tickets) != 0 {
				t.Errorf("task %s carries tickets %v", nv.ID, nv.Tickets)
			}
		case processforge.GateNode:
			if len(nv.Tickets) > 2 {
				t.Errorf("gate %s: %d tickets, max 2", nv.ID, len(nv.Tickets))
			}
			seen := map[string]bool{}
			for _, tk := range nv.Tickets {
				if tk != sp.Approvers[0] && tk != sp.Approvers[1] {
					t.Errorf("gate %s: ticket from non-approver %q", nv.ID, tk)
				}
				if seen[tk] {
					t.Errorf("gate %s: duplicate ticket from %q", nv.ID, tk)
				}
				seen[tk] = true
			}
			if nv.Status == processforge.StatusDone && len(nv.Tickets) != 2 {
				t.Errorf("gate %s done with %d tickets", nv.ID, len(nv.Tickets))
			}
		}
	}
}

// Scenario 1: a stale-round vote paused mid-flight loses against a full
// Recheck; the new round starts clean and all successors are invalidated.
func TestStaleVoteLosesToRecheck(t *testing.T) {
	votePause := newPausePoint()
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{
		gate("G", "a", "b"),
		task("T", "G"),
		task("U", "T"),
	}}, processforge.WithBeforePublishHook(func(ev processforge.HookEvent) {
		if ev.Op == processforge.OpAct && ev.Node == "G" && ev.Actor == "b" && ev.Round == 2 {
			votePause.hook()()
		}
	}))

	// Complete the gate and T once so the recheck has results to wipe.
	if err := inst.Act("G", 1, "a"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Act("G", 1, "b"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Act("T", 1, "x"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Recheck("G", 1); err != nil {
		t.Fatal(err)
	}
	// Now G is at round 2 with one fresh vote; pause a second vote.
	if err := inst.Act("G", 2, "a"); err != nil {
		t.Fatal(err)
	}

	actErr := make(chan error, 1)
	go func() { actErr <- inst.Act("G", 2, "b") }()
	<-votePause.arrived // vote b validated against round 2, not published

	// While the vote is paused, a recheck of the same instance runs to
	// completion and a snapshot is taken.
	if err := inst.Recheck("G", 2); err != nil {
		t.Fatalf("Recheck while vote paused: %v", err)
	}
	snap := inst.View()
	if nv := node(t, snap, "G"); nv.Round != 3 || len(nv.Tickets) != 0 {
		t.Fatalf("G in snapshot: %+v, want round 3 no tickets", nv)
	}
	for _, id := range []string{"T", "U"} {
		if nv := node(t, snap, id); nv.Round != 3 || nv.Status != processforge.StatusPending {
			t.Fatalf("%s in snapshot: %+v, want round 3 Pending", id, nv)
		}
	}

	// Release the stale vote: it must fail without any side effect.
	close(votePause.release)
	if err := <-actErr; !errors.Is(err, processforge.ErrStaleRound) {
		t.Fatalf("stale vote: err = %v, want ErrStaleRound", err)
	}
	snap = inst.View()
	if nv := node(t, snap, "G"); nv.Round != 3 || len(nv.Tickets) != 0 {
		t.Fatalf("G after stale vote: %+v, want round 3 no tickets", nv)
	}
	for _, id := range []string{"T", "U"} {
		if nv := node(t, snap, id); nv.Round != 3 || nv.Status != processforge.StatusPending {
			t.Fatalf("%s after stale vote: %+v, want round 3 Pending", id, nv)
		}
	}
}

// Scenario 2: two independent gates recheck concurrently over a shared
// completed join; both succeed and the join advances two rounds. Views
// interleaved with the commits only observe legal serial snapshots. A
// side-branch Act validated before both rechecks still succeeds after.
func TestConcurrentRechecksOverJoin(t *testing.T) {
	recheckPause := map[string]*pausePoint{"G1": newPausePoint(), "G2": newPausePoint()}
	sidePause := newPausePoint()
	tmpl := processforge.Template{Nodes: []processforge.NodeSpec{
		task("A"),
		task("B"),
		gate("G1", "a1", "b1", "A"),
		gate("G2", "a2", "b2", "B"),
		task("J", "G1", "G2"),
		task("S"),
	}}
	inst := mustCreate(t, tmpl, processforge.WithBeforePublishHook(func(ev processforge.HookEvent) {
		switch {
		case ev.Op == processforge.OpRecheck:
			recheckPause[ev.Node].hook()()
		case ev.Op == processforge.OpAct && ev.Node == "S":
			sidePause.hook()()
		}
	}))

	// Drive everything to completion.
	for _, step := range []struct{ node, actor string }{
		{"A", "x"}, {"B", "x"},
		{"G1", "a1"}, {"G1", "b1"},
		{"G2", "a2"}, {"G2", "b2"},
		{"J", "x"},
	} {
		if err := inst.Act(step.node, 1, step.actor); err != nil {
			t.Fatalf("setup Act %s: %v", step.node, err)
		}
	}

	// Side-branch act validates first, then pauses.
	sideErr := make(chan error, 1)
	go func() { sideErr <- inst.Act("S", 1, "side") }()
	<-sidePause.arrived

	// Both rechecks validate and pause before publishing.
	r1 := make(chan error, 1)
	r2 := make(chan error, 1)
	go func() { r1 <- inst.Recheck("G1", 1) }()
	go func() { r2 <- inst.Recheck("G2", 1) }()
	<-recheckPause["G1"].arrived
	<-recheckPause["G2"].arrived

	// View before any commit: everything still at round 1.
	snap := inst.View()
	checkConsistent(t, tmpl, snap)
	if nv := node(t, snap, "J"); nv.Round != 1 || nv.Status != processforge.StatusDone {
		t.Fatalf("J before commits: %+v, want Done round 1", nv)
	}

	// Commit the first recheck only.
	close(recheckPause["G1"].release)
	if err := <-r1; err != nil {
		t.Fatalf("recheck G1: %v", err)
	}
	snap = inst.View()
	checkConsistent(t, tmpl, snap)
	if nv := node(t, snap, "G1"); nv.Round != 2 || nv.Status != processforge.StatusReady || len(nv.Tickets) != 0 {
		t.Fatalf("G1 between commits: %+v, want Ready round 2 no tickets", nv)
	}
	if nv := node(t, snap, "G2"); nv.Round != 1 || nv.Status != processforge.StatusDone {
		t.Fatalf("G2 between commits: %+v, want Done round 1", nv)
	}
	if nv := node(t, snap, "J"); nv.Round != 2 || nv.Status != processforge.StatusPending {
		t.Fatalf("J between commits: %+v, want Pending round 2", nv)
	}

	// Commit the second recheck.
	close(recheckPause["G2"].release)
	if err := <-r2; err != nil {
		t.Fatalf("recheck G2: %v", err)
	}
	snap = inst.View()
	checkConsistent(t, tmpl, snap)
	if nv := node(t, snap, "G2"); nv.Round != 2 || nv.Status != processforge.StatusReady || len(nv.Tickets) != 0 {
		t.Fatalf("G2 after commits: %+v, want Ready round 2 no tickets", nv)
	}
	if nv := node(t, snap, "J"); nv.Round != 3 || nv.Status != processforge.StatusPending {
		t.Fatalf("J after commits: %+v, want Pending round 3", nv)
	}

	// Release the side act: unaffected by the rechecks, it must succeed
	// and must not clobber their results.
	close(sidePause.release)
	if err := <-sideErr; err != nil {
		t.Fatalf("side act: %v", err)
	}
	snap = inst.View()
	checkConsistent(t, tmpl, snap)
	if nv := node(t, snap, "S"); nv.Round != 1 || nv.Status != processforge.StatusDone {
		t.Fatalf("S: %+v, want Done round 1", nv)
	}
	if nv := node(t, snap, "J"); nv.Round != 3 || nv.Status != processforge.StatusPending {
		t.Fatalf("J after side act: %+v, want Pending round 3", nv)
	}
	if nv := node(t, snap, "G1"); nv.Round != 2 || nv.Status != processforge.StatusReady {
		t.Fatalf("G1 after side act: %+v, want Ready round 2", nv)
	}
}

// Scenario 3: both approvers validate before either publishes; both votes
// survive, the downstream node stays pending until the second vote, and a
// snapshot taken before the votes never changes.
func TestBothVotesPreserved(t *testing.T) {
	arrived := make(chan string, 2)
	release := map[string]chan struct{}{"a": make(chan struct{}), "b": make(chan struct{})}
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{
		gate("G", "a", "b"),
		task("D", "G"),
	}}, processforge.WithBeforePublishHook(func(ev processforge.HookEvent) {
		if ev.Op == processforge.OpAct && ev.Node == "G" {
			arrived <- ev.Actor
			<-release[ev.Actor]
		}
	}))

	// Snapshot taken before any vote; must never change afterwards.
	before := inst.View()

	errs := make(chan error, 2)
	go func() { errs <- inst.Act("G", 1, "a") }()
	go func() { errs <- inst.Act("G", 1, "b") }()
	seen := map[string]bool{<-arrived: true, <-arrived: true}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("expected both approvers paused, got %v", seen)
	}

	// Nothing published yet: the gate still shows no tickets.
	if nv := node(t, inst.View(), "G"); len(nv.Tickets) != 0 {
		t.Fatalf("G while both votes paused: tickets = %v, want none", nv.Tickets)
	}

	// Publish the first vote only: the gate must not complete and the
	// downstream node must stay pending.
	close(release["a"])
	if err := <-errs; err != nil {
		t.Fatalf("first vote: %v", err)
	}
	snap := inst.View()
	if nv := node(t, snap, "G"); nv.Status != processforge.StatusReady || len(nv.Tickets) != 1 {
		t.Fatalf("G after one vote: %+v, want Ready with 1 ticket", nv)
	}
	if nv := node(t, snap, "D"); nv.Status != processforge.StatusPending {
		t.Fatalf("D after one vote: status = %v, want Pending", nv.Status)
	}

	// Publish the second vote: both tickets survive, the gate completes.
	close(release["b"])
	if err := <-errs; err != nil {
		t.Fatalf("second vote: %v", err)
	}
	snap = inst.View()
	nv := node(t, snap, "G")
	if nv.Status != processforge.StatusDone || len(nv.Tickets) != 2 {
		t.Fatalf("G after both votes: %+v, want Done with 2 tickets", nv)
	}
	got := map[string]bool{nv.Tickets[0]: true, nv.Tickets[1]: true}
	if !got["a"] || !got["b"] {
		t.Fatalf("tickets = %v, want both a and b", nv.Tickets)
	}
	if nv := node(t, snap, "D"); nv.Status != processforge.StatusReady {
		t.Fatalf("D after gate done: status = %v, want Ready", nv.Status)
	}

	// The pre-vote snapshot is unchanged.
	if nv := node(t, before, "G"); nv.Round != 1 || nv.Status != processforge.StatusReady || len(nv.Tickets) != 0 {
		t.Fatalf("pre-vote snapshot mutated: %+v, want Ready round 1 no tickets", nv)
	}
}
