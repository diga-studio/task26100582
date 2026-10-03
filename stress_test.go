package processforge_test

import (
	"math/rand"
	"sync"
	"testing"

	"processforge"
)

// TestConcurrentStress hammers one instance with concurrent Acts,
// Rechecks and Views. Every observed snapshot must be a legal serial
// snapshot and per-node rounds must never go backwards.
func TestConcurrentStress(t *testing.T) {
	tmpl := processforge.Template{Nodes: []processforge.NodeSpec{
		task("A"),
		task("B"),
		gate("G1", "a1", "b1", "A"),
		gate("G2", "a2", "b2", "B"),
		task("M", "G1"),
		task("J", "M", "G2"),
		task("S"),
	}}
	inst := mustCreate(t, tmpl)

	var wg sync.WaitGroup

	// Viewers: every snapshot must be consistent; rounds never decrease.
	for v := 0; v < 4; v++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lastRound := map[string]uint64{}
			for k := 0; k < 200; k++ {
				snap := inst.View()
				checkConsistent(t, tmpl, snap)
				for _, nv := range snap.Nodes {
					if nv.Round < lastRound[nv.ID] {
						t.Errorf("node %s round went backwards: %d -> %d", nv.ID, lastRound[nv.ID], nv.Round)
					}
					lastRound[nv.ID] = nv.Round
				}
			}
		}()
	}

	// Actors: attempt plausible acts; any error is acceptable, panics and
	// races are not.
	actors := []string{"a1", "b1", "a2", "b2", "x"}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for k := 0; k < 200; k++ {
				snap := inst.View()
				nv := snap.Nodes[rng.Intn(len(snap.Nodes))]
				_ = inst.Act(nv.ID, nv.Round, actors[rng.Intn(len(actors))])
			}
		}(int64(1000 + w))
	}

	// Recheckers: alternate between the two gates with fresh rounds.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			gates := []string{"G1", "G2"}
			for k := 0; k < 50; k++ {
				g := gates[rng.Intn(len(gates))]
				nv, ok := inst.View().Get(g)
				if !ok {
					t.Errorf("gate %s missing from snapshot", g)
					return
				}
				_ = inst.Recheck(g, nv.Round)
			}
		}(int64(2000 + w))
	}

	wg.Wait()
	checkConsistent(t, tmpl, inst.View())
}
