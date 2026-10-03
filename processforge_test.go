package processforge_test

import (
	"errors"
	"fmt"
	"testing"

	"processforge"
)

func mustCreate(t *testing.T, tmpl processforge.Template, opts ...processforge.Option) *processforge.Instance {
	t.Helper()
	inst, err := processforge.Create(tmpl, opts...)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return inst
}

func node(t *testing.T, snap processforge.Snapshot, id string) processforge.NodeView {
	t.Helper()
	nv, ok := snap.Get(id)
	if !ok {
		t.Fatalf("snapshot missing node %q", id)
	}
	return nv
}

func gate(id string, a, b string, deps ...string) processforge.NodeSpec {
	return processforge.NodeSpec{ID: id, Kind: processforge.GateNode, Approvers: []string{a, b}, Deps: deps}
}

func task(id string, deps ...string) processforge.NodeSpec {
	return processforge.NodeSpec{ID: id, Kind: processforge.TaskNode, Deps: deps}
}

func TestCreateInitialState(t *testing.T) {
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{
		task("A"),
		gate("G", "a", "b", "A"),
		task("C", "G"),
	}})
	snap := inst.View()
	for _, id := range []string{"A", "G", "C"} {
		if nv := node(t, snap, id); nv.Round != 1 {
			t.Fatalf("node %s: round = %d, want 1", id, nv.Round)
		}
	}
	if nv := node(t, snap, "A"); nv.Status != processforge.StatusReady {
		t.Fatalf("source node A: status = %v, want Ready", nv.Status)
	}
	if nv := node(t, snap, "G"); nv.Status != processforge.StatusPending {
		t.Fatalf("G: status = %v, want Pending", nv.Status)
	}
	if nv := node(t, snap, "C"); nv.Status != processforge.StatusPending {
		t.Fatalf("C: status = %v, want Pending", nv.Status)
	}
}

func TestTemplateValidation(t *testing.T) {
	cases := map[string]processforge.Template{
		"empty": {},
		"too many nodes": func() processforge.Template {
			var nodes []processforge.NodeSpec
			for k := 0; k < processforge.MaxNodes+1; k++ {
				nodes = append(nodes, task(fmt.Sprintf("n%d", k)))
			}
			return processforge.Template{Nodes: nodes}
		}(),
		"duplicate ID": {Nodes: []processforge.NodeSpec{task("A"), task("A")}},
		"empty ID":     {Nodes: []processforge.NodeSpec{task("")}},
		"unknown dep":  {Nodes: []processforge.NodeSpec{task("A", "B")}},
		"self dep":     {Nodes: []processforge.NodeSpec{task("A", "A")}},
		"cycle": {Nodes: []processforge.NodeSpec{
			task("A", "B"),
			task("B", "A"),
		}},
		"gate one approver":    {Nodes: []processforge.NodeSpec{gate("G", "a", "")}},
		"gate three approvers": {Nodes: []processforge.NodeSpec{{ID: "G", Kind: processforge.GateNode, Approvers: []string{"a", "b", "c"}}}},
		"gate same approver":   {Nodes: []processforge.NodeSpec{gate("G", "a", "a")}},
		"task with approvers":  {Nodes: []processforge.NodeSpec{{ID: "A", Kind: processforge.TaskNode, Approvers: []string{"a", "b"}}}},
	}
	for name, tmpl := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := processforge.Create(tmpl); !errors.Is(err, processforge.ErrInvalidTemplate) {
				t.Fatalf("Create: err = %v, want ErrInvalidTemplate", err)
			}
		})
	}
}

func TestActTaskFlow(t *testing.T) {
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{
		task("A"),
		task("B", "A"),
	}})
	if err := inst.Act("B", 1, "x"); !errors.Is(err, processforge.ErrNotReady) {
		t.Fatalf("Act on pending B: err = %v, want ErrNotReady", err)
	}
	if err := inst.Act("A", 1, "x"); err != nil {
		t.Fatalf("Act A: %v", err)
	}
	if nv := node(t, inst.View(), "B"); nv.Status != processforge.StatusReady {
		t.Fatalf("B after A done: status = %v, want Ready", nv.Status)
	}
	if err := inst.Act("B", 1, "x"); err != nil {
		t.Fatalf("Act B: %v", err)
	}
	if err := inst.Act("B", 1, "x"); !errors.Is(err, processforge.ErrAlreadyDone) {
		t.Fatalf("Act done B: err = %v, want ErrAlreadyDone", err)
	}
}

func TestActErrors(t *testing.T) {
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{task("A")}})
	if err := inst.Act("ZZ", 1, "x"); !errors.Is(err, processforge.ErrUnknownNode) {
		t.Fatalf("Act unknown: err = %v, want ErrUnknownNode", err)
	}
	if err := inst.Act("A", 99, "x"); !errors.Is(err, processforge.ErrStaleRound) {
		t.Fatalf("Act stale round: err = %v, want ErrStaleRound", err)
	}
	if err := inst.Act("A", 1, "x"); err != nil {
		t.Fatalf("Act A: %v", err)
	}
}

func TestGateVoting(t *testing.T) {
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{
		gate("G", "a", "b"),
		task("D", "G"),
	}})
	if err := inst.Act("G", 1, "stranger"); !errors.Is(err, processforge.ErrNotApprover) {
		t.Fatalf("Act by stranger: err = %v, want ErrNotApprover", err)
	}
	if err := inst.Act("G", 1, "a"); err != nil {
		t.Fatalf("vote a: %v", err)
	}
	if err := inst.Act("G", 1, "a"); !errors.Is(err, processforge.ErrDuplicateVote) {
		t.Fatalf("duplicate vote: err = %v, want ErrDuplicateVote", err)
	}
	snap := inst.View()
	if nv := node(t, snap, "G"); nv.Status != processforge.StatusReady || len(nv.Tickets) != 1 {
		t.Fatalf("G after one vote: %+v, want Ready with 1 ticket", nv)
	}
	if nv := node(t, snap, "D"); nv.Status != processforge.StatusPending {
		t.Fatalf("D after one vote: status = %v, want Pending", nv.Status)
	}
	if err := inst.Act("G", 1, "b"); err != nil {
		t.Fatalf("vote b: %v", err)
	}
	snap = inst.View()
	if nv := node(t, snap, "G"); nv.Status != processforge.StatusDone || len(nv.Tickets) != 2 {
		t.Fatalf("G after two votes: %+v, want Done with 2 tickets", nv)
	}
	if nv := node(t, snap, "D"); nv.Status != processforge.StatusReady {
		t.Fatalf("D after gate done: status = %v, want Ready", nv.Status)
	}
}

func TestRecheckBasic(t *testing.T) {
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{
		gate("G", "a", "b"),
		task("T", "G"),
		task("U", "T"),
		task("S"),
	}})
	if err := inst.Act("G", 1, "a"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Act("G", 1, "b"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Act("T", 1, "x"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Act("S", 1, "x"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Recheck("G", 2); !errors.Is(err, processforge.ErrStaleRound) {
		t.Fatalf("Recheck stale round: err = %v, want ErrStaleRound", err)
	}
	if err := inst.Recheck("G", 1); err != nil {
		t.Fatalf("Recheck: %v", err)
	}
	snap := inst.View()
	for _, id := range []string{"G", "T", "U"} {
		if nv := node(t, snap, id); nv.Round != 2 {
			t.Fatalf("%s after recheck: round = %d, want 2", id, nv.Round)
		}
	}
	if nv := node(t, snap, "G"); nv.Status != processforge.StatusReady || len(nv.Tickets) != 0 {
		t.Fatalf("G after recheck: %+v, want Ready with no tickets", nv)
	}
	if nv := node(t, snap, "T"); nv.Status != processforge.StatusPending {
		t.Fatalf("T after recheck: status = %v, want Pending", nv.Status)
	}
	// Unaffected node keeps state and round.
	if nv := node(t, snap, "S"); nv.Status != processforge.StatusDone || nv.Round != 1 {
		t.Fatalf("S after recheck: %+v, want Done round 1", nv)
	}
	// Old-round acts now fail; new-round votes work.
	if err := inst.Act("G", 1, "a"); !errors.Is(err, processforge.ErrStaleRound) {
		t.Fatalf("old round vote: err = %v, want ErrStaleRound", err)
	}
	if err := inst.Act("G", 2, "a"); err != nil {
		t.Fatalf("new round vote: %v", err)
	}
}

func TestRecheckErrors(t *testing.T) {
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{
		task("A"),
		gate("G", "a", "b", "A"),
	}})
	if err := inst.Recheck("ZZ", 1); !errors.Is(err, processforge.ErrUnknownNode) {
		t.Fatalf("Recheck unknown: err = %v, want ErrUnknownNode", err)
	}
	if err := inst.Recheck("A", 1); !errors.Is(err, processforge.ErrNotGate) {
		t.Fatalf("Recheck task: err = %v, want ErrNotGate", err)
	}
	if err := inst.Recheck("G", 1); !errors.Is(err, processforge.ErrNotReady) {
		t.Fatalf("Recheck pending gate: err = %v, want ErrNotReady", err)
	}
}

// Rechecking one branch of a join must keep the other branch's results.
func TestRecheckJoinKeepsOtherBranch(t *testing.T) {
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{
		gate("G", "a", "b"),
		task("C"),
		task("J", "G", "C"),
	}})
	if err := inst.Act("G", 1, "a"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Act("G", 1, "b"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Act("C", 1, "x"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Act("J", 1, "x"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Recheck("G", 1); err != nil {
		t.Fatalf("Recheck: %v", err)
	}
	snap := inst.View()
	if nv := node(t, snap, "C"); nv.Status != processforge.StatusDone || nv.Round != 1 {
		t.Fatalf("C: %+v, want Done round 1 (untouched)", nv)
	}
	if nv := node(t, snap, "J"); nv.Round != 2 || nv.Status != processforge.StatusPending {
		t.Fatalf("J: %+v, want Pending round 2", nv)
	}
	// Re-complete the gate; J becomes ready again without touching C.
	if err := inst.Act("G", 2, "a"); err != nil {
		t.Fatal(err)
	}
	if err := inst.Act("G", 2, "b"); err != nil {
		t.Fatal(err)
	}
	if nv := node(t, inst.View(), "J"); nv.Status != processforge.StatusReady {
		t.Fatalf("J after gate re-done: status = %v, want Ready", nv.Status)
	}
}

func TestSnapshotImmutability(t *testing.T) {
	inst := mustCreate(t, processforge.Template{Nodes: []processforge.NodeSpec{
		gate("G", "a", "b"),
	}})
	if err := inst.Act("G", 1, "a"); err != nil {
		t.Fatal(err)
	}
	snap := inst.View()
	nv := node(t, snap, "G")
	if len(nv.Tickets) != 1 || nv.Tickets[0] != "a" {
		t.Fatalf("tickets = %v, want [a]", nv.Tickets)
	}
	// Mutating the returned snapshot must not affect the instance.
	nv.Tickets[0] = "hacked"
	snap.Nodes[0].Round = 999
	fresh := node(t, inst.View(), "G")
	if fresh.Tickets[0] != "a" || fresh.Round != 1 {
		t.Fatalf("instance state changed via snapshot: %+v", fresh)
	}
}
