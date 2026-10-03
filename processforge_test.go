package processforge

import (
	"errors"
	"fmt"
	"testing"
)

func gate(id string, a, b string, deps ...string) NodeSpec {
	return NodeSpec{ID: id, Kind: KindGate, Approvers: [2]string{a, b}, Deps: deps}
}

func task(id string, deps ...string) NodeSpec {
	return NodeSpec{ID: id, Kind: KindTask, Deps: deps}
}

func mustView(t *testing.T, in *Instance, id string) NodeView {
	t.Helper()
	v, ok := in.View().ByID(id)
	if !ok {
		t.Fatalf("node %q missing from snapshot", id)
	}
	return v
}

func TestCreateValidatesTemplates(t *testing.T) {
	cases := map[string]Template{
		"too many nodes": func() Template {
			var nodes []NodeSpec
			for i := 0; i < 51; i++ {
				nodes = append(nodes, task(fmt.Sprintf("n%d", i)))
			}
			return Template{Nodes: nodes}
		}(),
		"empty id":         {Nodes: []NodeSpec{task("")}},
		"duplicate id":     {Nodes: []NodeSpec{task("a"), task("a")}},
		"unknown dep":      {Nodes: []NodeSpec{task("a", "ghost")}},
		"cycle":            {Nodes: []NodeSpec{task("a", "b"), task("b", "a")}},
		"self cycle":       {Nodes: []NodeSpec{task("a", "a")}},
		"bad kind":         {Nodes: []NodeSpec{{ID: "a", Kind: NodeKind(7)}}},
		"same approvers":   {Nodes: []NodeSpec{gate("g", "x", "x")}},
		"missing approver": {Nodes: []NodeSpec{gate("g", "x", "")}},
	}
	for name, tpl := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Create(tpl); !errors.Is(err, ErrInvalidTemplate) {
				t.Fatalf("got %v, want ErrInvalidTemplate", err)
			}
		})
	}
}

func TestInitialView(t *testing.T) {
	in, err := Create(Template{Nodes: []NodeSpec{
		task("src"),
		gate("g", "a", "b", "src"),
		task("sink", "g"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if v := mustView(t, in, "src"); v.Round != 1 || v.Status != StatusReady {
		t.Fatalf("src: %+v", v)
	}
	for _, id := range []string{"g", "sink"} {
		if v := mustView(t, in, id); v.Round != 1 || v.Status != StatusPending {
			t.Fatalf("%s: %+v", id, v)
		}
	}
}

func TestTaskCompletionUnlocksDependents(t *testing.T) {
	in, _ := Create(Template{Nodes: []NodeSpec{task("a"), task("b", "a")}})
	if err := in.Act("b", 1, "anyone"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("act on pending: %v", err)
	}
	if err := in.Act("a", 1, "anyone"); err != nil {
		t.Fatal(err)
	}
	if v := mustView(t, in, "b"); v.Status != StatusReady {
		t.Fatalf("b should be ready: %+v", v)
	}
	if err := in.Act("a", 1, "again"); !errors.Is(err, ErrAlreadyDone) {
		t.Fatalf("re-act done task: %v", err)
	}
}

func TestGateVoting(t *testing.T) {
	in, _ := Create(Template{Nodes: []NodeSpec{
		gate("g", "alice", "bob"),
		task("down", "g"),
	}})
	if err := in.Act("g", 1, "mallory"); !errors.Is(err, ErrNotApprover) {
		t.Fatalf("non-approver: %v", err)
	}
	if err := in.Act("g", 1, "alice"); err != nil {
		t.Fatal(err)
	}
	v := mustView(t, in, "g")
	if v.Status != StatusReady || len(v.Tickets) != 1 || v.Tickets[0] != "alice" {
		t.Fatalf("after one vote: %+v", v)
	}
	if v := mustView(t, in, "down"); v.Status != StatusPending {
		t.Fatalf("downstream must wait for two votes: %+v", v)
	}
	if err := in.Act("g", 1, "alice"); !errors.Is(err, ErrDuplicateVote) {
		t.Fatalf("duplicate vote: %v", err)
	}
	if err := in.Act("g", 1, "bob"); err != nil {
		t.Fatal(err)
	}
	if v := mustView(t, in, "g"); v.Status != StatusDone || len(v.Tickets) != 2 {
		t.Fatalf("gate should pass: %+v", v)
	}
	if v := mustView(t, in, "down"); v.Status != StatusReady {
		t.Fatalf("downstream ready after pass: %+v", v)
	}
}

func TestStaleRoundRejected(t *testing.T) {
	in, _ := Create(Template{Nodes: []NodeSpec{task("a")}})
	if err := in.Act("a", 99, "x"); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("got %v", err)
	}
	if v := mustView(t, in, "a"); v.Status != StatusReady {
		t.Fatalf("failed act must not change state: %+v", v)
	}
	if err := in.Recheck("a", 1); !errors.Is(err, ErrNotGate) {
		t.Fatalf("recheck task: %v", err)
	}
	if _, err := Create(Template{Nodes: []NodeSpec{task("x")}}); err != nil {
		t.Fatal(err)
	}
	if err := in.Act("ghost", 1, "x"); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("unknown node: %v", err)
	}
}

func TestRecheckResetsGateAndSuccessors(t *testing.T) {
	in, _ := Create(Template{Nodes: []NodeSpec{
		task("src"),
		gate("g", "alice", "bob", "src"),
		task("mid", "g"),
		gate("g2", "carol", "dave", "mid"),
		task("side"),
	}})
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(in.Act("src", 1, "u"))
	must(in.Act("g", 1, "alice"))
	must(in.Act("g", 1, "bob"))
	must(in.Act("mid", 1, "u"))
	must(in.Act("g2", 1, "carol"))
	must(in.Act("side", 1, "u"))

	// Pending gate cannot be rechecked; stale round rejected.
	if err := in.Recheck("g2", 2); !errors.Is(err, ErrStaleRound) {
		t.Fatalf("stale recheck: %v", err)
	}

	must(in.Recheck("g", 1))

	if v := mustView(t, in, "g"); v.Round != 2 || v.Status != StatusReady || len(v.Tickets) != 0 {
		t.Fatalf("g after recheck: %+v", v)
	}
	if v := mustView(t, in, "mid"); v.Round != 2 || v.Status != StatusPending {
		t.Fatalf("mid after recheck: %+v", v)
	}
	if v := mustView(t, in, "g2"); v.Round != 2 || v.Status != StatusPending || len(v.Tickets) != 0 {
		t.Fatalf("g2 after recheck: %+v", v)
	}
	// Unaffected nodes keep state and round.
	if v := mustView(t, in, "src"); v.Round != 1 || v.Status != StatusDone {
		t.Fatalf("src untouched: %+v", v)
	}
	if v := mustView(t, in, "side"); v.Round != 1 || v.Status != StatusDone {
		t.Fatalf("side untouched: %+v", v)
	}

	// Re-execute the invalidated chain in the new round.
	must(in.Act("g", 2, "alice"))
	must(in.Act("g", 2, "bob"))
	must(in.Act("mid", 2, "u"))
	if v := mustView(t, in, "g2"); v.Status != StatusReady {
		t.Fatalf("g2 ready again: %+v", v)
	}
}

func TestRecheckPendingGateRejected(t *testing.T) {
	in, _ := Create(Template{Nodes: []NodeSpec{
		task("src"),
		gate("g", "a", "b", "src"),
	}})
	if err := in.Recheck("g", 1); !errors.Is(err, ErrGateNotRecheckable) {
		t.Fatalf("got %v", err)
	}
}

func TestRecheckReadyGateClearsPartialVotes(t *testing.T) {
	in, _ := Create(Template{Nodes: []NodeSpec{gate("g", "a", "b")}})
	if err := in.Act("g", 1, "a"); err != nil {
		t.Fatal(err)
	}
	if err := in.Recheck("g", 1); err != nil {
		t.Fatal(err)
	}
	v := mustView(t, in, "g")
	if v.Round != 2 || len(v.Tickets) != 0 || v.Status != StatusReady {
		t.Fatalf("partial votes cleared: %+v", v)
	}
}

func TestRecheckPreservesUnaffectedJoinBranch(t *testing.T) {
	in, _ := Create(Template{Nodes: []NodeSpec{
		gate("g1", "a", "b"),
		task("keep"),
		task("join", "g1", "keep"),
	}})
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(in.Act("g1", 1, "a"))
	must(in.Act("g1", 1, "b"))
	must(in.Act("keep", 1, "u"))
	must(in.Recheck("g1", 1))
	if v := mustView(t, in, "keep"); v.Status != StatusDone || v.Round != 1 {
		t.Fatalf("unaffected branch result preserved: %+v", v)
	}
	if v := mustView(t, in, "join"); v.Status != StatusPending || v.Round != 2 {
		t.Fatalf("join reset: %+v", v)
	}
	// Once g1 passes again, join becomes ready using the preserved result.
	must(in.Act("g1", 2, "a"))
	must(in.Act("g1", 2, "b"))
	if v := mustView(t, in, "join"); v.Status != StatusReady {
		t.Fatalf("join ready again: %+v", v)
	}
}

func TestSnapshotIsImmutable(t *testing.T) {
	in, _ := Create(Template{Nodes: []NodeSpec{gate("g", "a", "b")}})
	before := in.View()
	if err := in.Act("g", 1, "a"); err != nil {
		t.Fatal(err)
	}
	if v, _ := before.ByID("g"); len(v.Tickets) != 0 {
		t.Fatalf("earlier snapshot mutated by later act: %+v", v)
	}
	// Mutating the returned snapshot must not affect the instance.
	after := in.View()
	after.Nodes[0].Tickets[0] = "forged"
	after.Nodes[0].Round = 99
	if v := mustView(t, in, "g"); v.Round != 1 || v.Tickets[0] != "a" {
		t.Fatalf("instance state leaked into snapshot: %+v", v)
	}
}
