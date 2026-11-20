package sim

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/forgehk/quorum/kv"
	"github.com/forgehk/quorum/raft"
)

// quiet is a cluster with a perfect network and no clients, for scripted tests.
func quiet(o Options) *Cluster {
	o.Clients = 1
	c := New(o)
	c.lossy = false
	return c
}

func put(key, value string) kv.Command { return kv.Command{Op: kv.Put, Key: key, Value: value} }

func mustDo(t *testing.T, c *Cluster, seq uint64, cmd kv.Command) kv.Result {
	t.Helper()
	result, ok := c.Do(seq, cmd, 2000)
	if !ok {
		t.Fatalf("command %d did not commit", seq)
	}
	return result
}

func settle(t *testing.T, c *Cluster) {
	t.Helper()
	for i := 0; i < 3000 && !c.settled(); i++ {
		c.Tick()
	}
	if !c.settled() {
		t.Fatal("the cluster did not settle")
	}
	if v := c.Violation(); v != "" {
		t.Fatal(v)
	}
}

func TestClusterElectsOneLeaderEveryoneAgreesOn(t *testing.T) {
	c := quiet(Options{Seed: 1})
	lead := c.WaitLeader(500)
	if lead == raft.None {
		t.Fatal("no leader after 500 ticks")
	}
	c.Run(50)
	for id := 0; id < 5; id++ {
		if got := c.Node(id).Leader(); got != lead {
			t.Errorf("node %d thinks the leader is %d, not %d", id, got, lead)
		}
	}
}

func TestCommandsReachEveryStore(t *testing.T) {
	c := quiet(Options{Seed: 2})
	for i := uint64(1); i <= 20; i++ {
		mustDo(t, c, i, put("k"+strconv.Itoa(int(i%4)), strconv.Itoa(int(i))))
	}
	settle(t, c)
	want := c.Store(0).Snapshot()
	for id := 1; id < 5; id++ {
		if !bytes.Equal(want, c.Store(id).Snapshot()) {
			t.Fatalf("node %d has a different store", id)
		}
	}
	if got := c.Store(3).Read("k0"); got.Value != "20" {
		t.Fatalf("k0=%q, want the last write", got.Value)
	}
}

func TestANewLeaderTakesOverAndTheOldOneCatchesUp(t *testing.T) {
	c := quiet(Options{Seed: 3})
	mustDo(t, c, 1, put("a", "1"))
	old := c.Leader()
	oldTerm := c.Node(old).Term()
	c.Crash(old)

	mustDo(t, c, 2, put("a", "2"))
	next := c.Leader()
	if next == old || c.Node(next).Term() <= oldTerm {
		t.Fatalf("expected a new leader in a later term, got node %d", next)
	}

	c.Restart(old)
	mustDo(t, c, 3, put("b", "3"))
	settle(t, c)
	if got := c.Store(old).Read("a"); got.Value != "2" {
		t.Fatalf("the restarted node has a=%q", got.Value)
	}
	if c.Node(old).State() == raft.Leader && c.Node(next).State() == raft.Leader {
		t.Fatal("two leaders")
	}
}

func TestOnlyTheMajoritySideCommits(t *testing.T) {
	c := quiet(Options{Seed: 4})
	mustDo(t, c, 1, put("a", "1"))
	lead := c.Leader()
	minority := []int{lead, (lead + 1) % 5}
	var majority []int
	for id := 0; id < 5; id++ {
		if id != minority[0] && id != minority[1] {
			majority = append(majority, id)
		}
	}
	c.Partition(minority, majority)

	// The old leader still accepts the proposal but can never commit it.
	stranded := kv.Command{Client: 77, Seq: 1, Op: kv.Put, Key: "a", Value: "stranded"}
	c.Node(lead).Propose(kv.Encode(stranded))
	before := c.Node(lead).Commit()
	c.Run(400)
	if c.Node(lead).Commit() != before {
		t.Fatal("the minority side committed something")
	}
	if c.Node(lead).State() == raft.Leader {
		t.Fatal("a leader cut off from the majority kept its role (CheckQuorum is on)")
	}
	if c.Leader() == raft.None {
		t.Fatal("the majority side did not elect a leader")
	}
	mustDo(t, c, 2, put("a", "2"))

	c.Heal()
	mustDo(t, c, 3, put("b", "3"))
	settle(t, c)
	for id := 0; id < 5; id++ {
		if got := c.Store(id).Read("a"); got.Value != "2" {
			t.Fatalf("node %d has a=%q after healing", id, got.Value)
		}
	}
}

func TestAReturningNodeDoesNotDisruptTheClusterWithPreVote(t *testing.T) {
	for _, noPreVote := range []bool{false, true} {
		c := quiet(Options{Seed: 5, NoPreVote: noPreVote, NoCheckQuorum: true})
		mustDo(t, c, 1, put("a", "1"))
		lead := c.Leader()
		term := c.Node(lead).Term()
		lonely := (lead + 1) % 5
		c.Isolate(lonely)
		c.Run(600)
		c.Heal()
		c.Run(200)
		mustDo(t, c, 2, put("a", "2"))

		kept := c.Leader() == lead && c.Node(lead).Term() == term
		if !noPreVote && !kept {
			t.Errorf("with Pre-Vote the leader should survive a node rejoining, but term went %d -> %d", term, c.Node(c.Leader()).Term())
		}
		if noPreVote && kept {
			t.Errorf("without Pre-Vote the rejoining node should have forced a new term")
		}
	}
}

func TestACrashedNodeComesBackFromItsSnapshot(t *testing.T) {
	c := quiet(Options{Seed: 6, Nodes: 3, SnapshotEvery: 10})
	mustDo(t, c, 1, put("a", "1"))
	victim := (c.Leader() + 1) % 3
	c.Crash(victim)
	for i := uint64(2); i <= 60; i++ {
		mustDo(t, c, i, put("k"+strconv.Itoa(int(i%5)), strconv.Itoa(int(i))))
	}
	if c.Stats().Snapshots == 0 {
		t.Fatal("no compaction happened")
	}
	if first := c.Node(c.Leader()).FirstIndex(); first < 40 {
		t.Fatalf("the leader's log still starts at %d", first)
	}
	c.Restart(victim)
	mustDo(t, c, 61, put("z", "done"))
	settle(t, c)
	if !bytes.Equal(c.Store(victim).Snapshot(), c.Store(c.Leader()).Snapshot()) {
		t.Fatal("the node that was down does not match the leader")
	}
	if c.Node(victim).FirstIndex() < 40 {
		t.Fatal("the node that was down replayed the whole log, so it was not sent a snapshot")
	}
}

func TestARetriedCommandIsAppliedOnce(t *testing.T) {
	c := quiet(Options{Seed: 7, Nodes: 3})
	mustDo(t, c, 1, put("n", "1"))
	swap := kv.Command{Op: kv.CAS, Key: "n", Expect: "1", Value: "2"}
	if !mustDo(t, c, 2, swap).OK {
		t.Fatal("the swap failed")
	}
	mustDo(t, c, 3, put("n", "1"))

	// The client never saw the answer to command 2 and sends it again.
	retry := swap
	retry.Client, retry.Seq = scriptedClient, 2
	lead := c.Leader()
	c.Node(lead).Propose(kv.Encode(retry))
	mustDo(t, c, 4, put("other", "x"))
	settle(t, c)
	if got := c.Store(lead).Read("n"); got.Value != "1" {
		t.Fatalf("the retry swapped again: n=%q", got.Value)
	}
}

func faulty(seed int64) Options {
	return Options{Seed: seed, Ticks: 4000, FaultEvery: 150, SnapshotEvery: 40, DropRate: 0.05, DupRate: 0.02}
}

func seeds(t *testing.T, fallback int) int {
	if raw := os.Getenv("QUORUM_SEEDS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("QUORUM_SEEDS=%q is not a number", raw)
		}
		return n
	}
	if testing.Short() {
		return fallback / 4
	}
	return fallback
}

func TestRunIsDeterministic(t *testing.T) {
	first, second := Run(faulty(42)), Run(faulty(42))
	if first != second {
		t.Fatalf("the same seed gave two different runs:\n%+v\n%+v", first, second)
	}
	if other := Run(faulty(43)); other.Stats == first.Stats {
		t.Fatal("a different seed gave an identical run")
	}
	if first.Operations < 500 || first.Crashes == 0 || first.Partitions == 0 || first.Snapshots == 0 {
		t.Fatalf("the faulty run did not exercise much: %+v", first.Stats)
	}
}

func TestRandomFaultsPreserveEveryProperty(t *testing.T) {
	for seed := int64(1); seed <= int64(seeds(t, 120)); seed++ {
		if result := Run(faulty(seed)); result.Violation != "" {
			t.Fatalf("seed %d: %s", seed, result.Violation)
		}
	}
}

func TestThreeNodesOnALossyNetwork(t *testing.T) {
	for seed := int64(1); seed <= int64(seeds(t, 80)); seed++ {
		o := faulty(seed)
		o.Nodes, o.DropRate, o.FaultEvery = 3, 0.2, 60
		if result := Run(o); result.Violation != "" {
			t.Fatalf("seed %d: %s", seed, result.Violation)
		}
	}
}

func TestSevenNodesWithoutPreVoteOrCheckQuorum(t *testing.T) {
	for seed := int64(1); seed <= int64(seeds(t, 40)); seed++ {
		o := faulty(seed)
		o.Nodes, o.NoPreVote, o.NoCheckQuorum, o.FaultEvery = 7, true, true, 80
		if result := Run(o); result.Violation != "" {
			t.Fatalf("seed %d: %s", seed, result.Violation)
		}
	}
}

// The checker has to be able to fail. Serving reads from a leader's local state
// is the classic way to break linearizability: a leader that has been deposed
// but has not noticed yet returns data that newer writes have replaced.
func TestTheCheckerCatchesStaleReads(t *testing.T) {
	caught := 0
	for seed := int64(1); seed <= 60; seed++ {
		o := faulty(seed)
		o.UnsafeLocalReads = true
		result := Run(o)
		if result.Violation == "" {
			continue
		}
		if !strings.Contains(result.Violation, "linearizability") {
			t.Fatalf("seed %d: expected a linearizability violation, got: %s", seed, result.Violation)
		}
		caught++
	}
	if caught == 0 {
		t.Fatal("60 seeds of stale reads and the checker accepted every history")
	}
	t.Logf("stale reads caught in %d of 60 seeds", caught)
}
