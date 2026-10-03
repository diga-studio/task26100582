package processforge

import (
	"errors"
	"sync"
	"testing"
)

// pausePoint lets a hook park an operation between validation and publish.
type pausePoint struct {
	entered chan struct{}
	release chan struct{}
}

func newPause() *pausePoint {
	return &pausePoint{entered: make(chan struct{}), release: make(chan struct{})}
}

func (p *pausePoint) park() { close(p.entered); <-p.release }

// Scenario 1: an Act paused after validation with the old round must fail
// once a Recheck commits, while the Recheck and Views proceed freely.
func TestPausedStaleVoteFailsAfterRecheck(t *testing.T) {
	pause := newPause()
	var once sync.Once
	hooks := &Hooks{ActValidated: func(nodeID string, round int, actor string) {
		if nodeID == "g" && round == 1 {
			once.Do(pause.park)
		}
	}}
	in, err := CreateWithHooks(Template{Nodes: []NodeSpec{
		gate("g", "alice", "bob"),
		task("down", "g"),
	}}, hooks)
	if err != nil {
		t.Fatal(err)
	}

	actErr := make(chan error, 1)
	go func() { actErr <- in.Act("g", 1, "alice") }()
	<-pause.entered // Act validated with round 1 and now parks, holding no locks

	// The paused Act must not block Recheck or View on the same instance.
	if err := in.Recheck("g", 1); err != nil {
		t.Fatalf("recheck while act paused: %v", err)
	}
	snap := in.View()
	if v, _ := snap.ByID("g"); v.Round != 2 || len(v.Tickets) != 0 || v.Status != StatusReady {
		t.Fatalf("g after recheck: %+v", v)
	}
	if v, _ := snap.ByID("down"); v.Round != 2 || v.Status != StatusPending {
		t.Fatalf("down after recheck: %+v", v)
	}

	close(pause.release)
	if err := <-actErr; !errors.Is(err, ErrStaleRound) {
		t.Fatalf("stale vote must fail, got %v", err)
	}

	// The failed stale vote left no trace; the new round starts clean.
	snap = in.View()
	if v, _ := snap.ByID("g"); v.Round != 2 || len(v.Tickets) != 0 {
		t.Fatalf("new round must have no tickets: %+v", v)
	}
	if v, _ := snap.ByID("down"); v.Round != 2 || v.Status != StatusPending {
		t.Fatalf("successor stays invalidated: %+v", v)
	}

	// The new round is fully usable.
	for _, a := range []string{"alice", "bob"} {
		if err := in.Act("g", 2, a); err != nil {
			t.Fatalf("new round vote: %v", err)
		}
	}
	if err := in.Act("down", 2, "u"); err != nil {
		t.Fatalf("successor re-executes in round 2: %v", err)
	}
}

// Scenario 2: two independent gates rechecked concurrently share one
// completed join successor. Both rechecks succeed, the join advances two
// rounds, Views only ever see serializable snapshots, and an unrelated Act
// validated before the rechecks still succeeds afterwards.
func TestConcurrentRechecksOnSharedJoin(t *testing.T) {
	p1, p2, pSide := newPause(), newPause(), newPause()
	var once1, once2, onceSide sync.Once
	hooks := &Hooks{
		RecheckValidated: func(gateID string, round int) {
			switch gateID {
			case "g1":
				once1.Do(p1.park)
			case "g2":
				once2.Do(p2.park)
			}
		},
		ActValidated: func(nodeID string, round int, actor string) {
			if nodeID == "side" {
				onceSide.Do(pSide.park)
			}
		},
	}
	in, err := CreateWithHooks(Template{Nodes: []NodeSpec{
		gate("g1", "a", "b"),
		gate("g2", "c", "d"),
		task("join", "g1", "g2"),
		task("side"),
	}}, hooks)
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(in.Act("g1", 1, "a"))
	must(in.Act("g1", 1, "b"))
	must(in.Act("g2", 1, "c"))
	must(in.Act("g2", 1, "d"))
	must(in.Act("join", 1, "u"))

	// checkJoinInvariant verifies the snapshot is explainable by some serial
	// order of the two rechecks: join's round equals 1 plus the number of
	// committed rechecks, and a rechecked join is never done.
	checkJoinInvariant := func(s Snapshot) {
		t.Helper()
		g1, _ := s.ByID("g1")
		g2, _ := s.ByID("g2")
		join, _ := s.ByID("join")
		if want := g1.Round + g2.Round - 1; join.Round != want {
			t.Fatalf("non-serializable snapshot: g1=%d g2=%d join=%d", g1.Round, g2.Round, join.Round)
		}
		if join.Round > 1 && join.Status == StatusDone {
			t.Fatalf("rechecked join must lose its result: %+v", join)
		}
	}

	// An unaffected side Act validates first and parks before publishing.
	sideErr := make(chan error, 1)
	go func() { sideErr <- in.Act("side", 1, "u") }()
	<-pSide.entered

	r1Err := make(chan error, 1)
	r2Err := make(chan error, 1)
	go func() { r1Err <- in.Recheck("g1", 1) }()
	<-p1.entered
	go func() { r2Err <- in.Recheck("g2", 1) }()
	<-p2.entered

	// Both rechecks validated, neither committed: everything still round 1.
	snap := in.View()
	checkJoinInvariant(snap)
	if v, _ := snap.ByID("join"); v.Round != 1 || v.Status != StatusDone {
		t.Fatalf("before commits: %+v", v)
	}

	close(p1.release)
	must(<-r1Err)
	snap = in.View()
	checkJoinInvariant(snap)
	if v, _ := snap.ByID("join"); v.Round != 2 || v.Status != StatusPending {
		t.Fatalf("after first commit: %+v", v)
	}
	if v, _ := snap.ByID("g2"); v.Round != 1 || v.Status != StatusDone {
		t.Fatalf("g2 untouched by first recheck: %+v", v)
	}

	close(p2.release)
	must(<-r2Err)
	snap = in.View()
	checkJoinInvariant(snap)
	if v, _ := snap.ByID("join"); v.Round != 3 || v.Status != StatusPending {
		t.Fatalf("join advanced by both rechecks: %+v", v)
	}
	for _, id := range []string{"g1", "g2"} {
		if v, _ := snap.ByID(id); v.Round != 2 || v.Status != StatusReady || len(v.Tickets) != 0 {
			t.Fatalf("%s rechecked once: %+v", id, v)
		}
	}

	// The side Act parked since before both commits still succeeds: its node
	// was never touched, so its round is still current.
	close(pSide.release)
	if err := <-sideErr; err != nil {
		t.Fatalf("unaffected act must survive rechecks: %v", err)
	}
	snap = in.View()
	if v, _ := snap.ByID("side"); v.Round != 1 || v.Status != StatusDone {
		t.Fatalf("side completed in round 1: %+v", v)
	}
	checkJoinInvariant(snap) // recheck results not overwritten by the side act
}

// Scenario 3: both approvers finish validation before either publishes; both
// votes must survive, and the downstream node must stay pending while only
// one vote is in. A snapshot taken before the votes never changes.
func TestBothApproversValidatedBeforePublish(t *testing.T) {
	entered := make(chan string, 2)
	release := map[string]chan struct{}{
		"alice": make(chan struct{}),
		"bob":   make(chan struct{}),
	}
	hooks := &Hooks{ActValidated: func(nodeID string, round int, actor string) {
		if nodeID == "g" && round == 1 {
			entered <- actor
			<-release[actor]
		}
	}}
	in, err := CreateWithHooks(Template{Nodes: []NodeSpec{
		gate("g", "alice", "bob"),
		task("down", "g"),
	}}, hooks)
	if err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 2)
	go func() { errs <- in.Act("g", 1, "alice") }()
	go func() { errs <- in.Act("g", 1, "bob") }()
	got := map[string]bool{<-entered: true, <-entered: true}
	if !got["alice"] || !got["bob"] {
		t.Fatalf("both approvers should have validated: %v", got)
	}

	// Snapshot taken while both votes are validated but unpublished.
	before := in.View()
	if v, _ := before.ByID("g"); len(v.Tickets) != 0 || v.Status != StatusReady {
		t.Fatalf("no votes visible yet: %+v", v)
	}

	close(release["alice"])
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	mid := in.View()
	if v, _ := mid.ByID("g"); v.Status != StatusReady || len(v.Tickets) != 1 {
		t.Fatalf("one vote kept, gate not done: %+v", v)
	}
	if v, _ := mid.ByID("down"); v.Status != StatusPending {
		t.Fatalf("downstream must not be ready on one vote: %+v", v)
	}

	close(release["bob"])
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	final := in.View()
	v, _ := final.ByID("g")
	if v.Status != StatusDone || len(v.Tickets) != 2 {
		t.Fatalf("both votes kept: %+v", v)
	}
	seen := map[string]bool{v.Tickets[0]: true, v.Tickets[1]: true}
	if !seen["alice"] || !seen["bob"] {
		t.Fatalf("tickets = %v", v.Tickets)
	}
	if v, _ := final.ByID("down"); v.Status != StatusReady {
		t.Fatalf("downstream ready after both votes: %+v", v)
	}

	// The pre-vote snapshot is immutable history.
	if v, _ := before.ByID("g"); len(v.Tickets) != 0 || v.Status != StatusReady || v.Round != 1 {
		t.Fatalf("earlier snapshot changed: %+v", v)
	}
}

// Concurrent duplicate votes by the same actor in the same round count once.
func TestConcurrentDuplicateVoteCountsOnce(t *testing.T) {
	pause := newPause()
	var once sync.Once
	hooks := &Hooks{ActValidated: func(nodeID string, round int, actor string) {
		if nodeID == "g" {
			once.Do(pause.park) // first voter parks; second validates meanwhile
		}
	}}
	in, err := CreateWithHooks(Template{Nodes: []NodeSpec{gate("g", "alice", "bob")}}, hooks)
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	go func() { errs <- in.Act("g", 1, "alice") }()
	<-pause.entered
	go func() { errs <- in.Act("g", 1, "alice") }()
	close(pause.release)

	var ok, dup int
	for i := 0; i < 2; i++ {
		switch err := <-errs; {
		case err == nil:
			ok++
		case errors.Is(err, ErrDuplicateVote):
			dup++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 || dup != 1 {
		t.Fatalf("ok=%d dup=%d", ok, dup)
	}
	if v := mustView(t, in, "g"); len(v.Tickets) != 1 {
		t.Fatalf("exactly one ticket: %+v", v)
	}
}
