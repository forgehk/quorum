package raft

import (
	"math/rand"
	"testing"
)

func cluster(t *testing.T, size int, edit func(*Config)) map[int]*Node {
	t.Helper()
	peers := make([]int, size)
	for i := range peers {
		peers[i] = i
	}
	nodes := map[int]*Node{}
	for _, id := range peers {
		cfg := Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 2, Rand: rand.New(rand.NewSource(int64(id) + 1))}
		if edit != nil {
			edit(&cfg)
		}
		nodes[id] = New(cfg, NewPersistent())
	}
	return nodes
}

// exchange delivers messages between the given nodes until none are left.
func exchange(nodes map[int]*Node) {
	for round := 0; round < 100; round++ {
		var pending []Message
		for id := 0; id < len(nodes)+8; id++ {
			if n, ok := nodes[id]; ok {
				pending = append(pending, n.Messages()...)
			}
		}
		if len(pending) == 0 {
			return
		}
		for _, m := range pending {
			if to, ok := nodes[m.To]; ok {
				to.Step(m)
			}
		}
	}
}

func elect(t *testing.T, nodes map[int]*Node, id int) {
	t.Helper()
	nodes[id].campaign(false)
	exchange(nodes)
	if nodes[id].State() != Leader {
		t.Fatalf("node %d did not become leader, state %v", id, nodes[id].State())
	}
}

func TestSingleNodeElectsItselfAndCommits(t *testing.T) {
	n := cluster(t, 1, nil)[0]
	for i := 0; i < 30 && n.State() != Leader; i++ {
		n.Tick()
	}
	if n.State() != Leader {
		t.Fatal("a one-node cluster never elected itself")
	}
	index, _, ok := n.Propose([]byte("x"))
	if !ok || n.Commit() != index {
		t.Fatalf("proposal not committed: ok=%v commit=%d index=%d", ok, n.Commit(), index)
	}
	applies := n.Applies()
	if len(applies) != 2 || applies[0].Entry.Data != nil || string(applies[1].Entry.Data) != "x" {
		t.Fatalf("expected the no-op then x, got %d applies", len(applies))
	}
}

func TestThreeNodesElectExactlyOneLeader(t *testing.T) {
	nodes := cluster(t, 3, nil)
	for tick := 0; tick < 200; tick++ {
		for id := 0; id < 3; id++ {
			nodes[id].Tick()
		}
		exchange(nodes)
	}
	leaders := 0
	for _, n := range nodes {
		if n.State() == Leader {
			leaders++
		}
		if n.Leader() == None {
			t.Errorf("node %d does not know the leader", n.ID())
		}
	}
	if leaders != 1 {
		t.Fatalf("expected one leader, found %d", leaders)
	}
}

func TestVoteIsDeniedToACandidateWithAStaleLog(t *testing.T) {
	voter := New(Config{ID: 1, Peers: []int{0, 1, 2}, ElectionTick: 10, HeartbeatTick: 2},
		&Persistent{Term: 2, Vote: None, Log: []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}})

	voter.Step(Message{Type: MsgVote, From: 0, To: 1, Term: 3, LogIndex: 5, LogTerm: 1})
	if reply := voter.Messages()[0]; !reply.Reject {
		t.Fatal("granted a vote to a candidate whose last term is older")
	}
	voter.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, LogIndex: 1, LogTerm: 2})
	if reply := voter.Messages()[0]; !reply.Reject {
		t.Fatal("granted a vote to a candidate with a shorter log in the same term")
	}
	voter.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, LogIndex: 2, LogTerm: 2})
	if reply := voter.Messages()[0]; reply.Reject {
		t.Fatal("refused a candidate whose log is exactly as up to date")
	}
	voter.Step(Message{Type: MsgVote, From: 0, To: 1, Term: 3, LogIndex: 9, LogTerm: 3})
	if reply := voter.Messages()[0]; !reply.Reject {
		t.Fatal("voted twice in one term")
	}
}

func TestFollowerDropsAConflictingSuffixAndHintsWhereToRetry(t *testing.T) {
	follower := New(Config{ID: 1, Peers: []int{0, 1, 2}, ElectionTick: 10, HeartbeatTick: 2},
		&Persistent{Term: 3, Vote: None, Log: []Entry{{1, 1, nil}, {2, 2, nil}, {3, 2, nil}, {4, 2, nil}}})

	// The leader thinks index 4 is from term 3. We have term 2 there, and all of term 2 starts at index 2.
	follower.Step(Message{Type: MsgApp, From: 0, To: 1, Term: 3, LogIndex: 4, LogTerm: 3})
	reply := follower.Messages()[0]
	if !reply.Reject || reply.Hint != 2 {
		t.Fatalf("expected a rejection hinting index 2, got reject=%v hint=%d", reply.Reject, reply.Hint)
	}

	follower.Step(Message{Type: MsgApp, From: 0, To: 1, Term: 3, LogIndex: 1, LogTerm: 1,
		Entries: []Entry{{2, 3, []byte("a")}, {3, 3, []byte("b")}}, Commit: 3})
	reply = follower.Messages()[0]
	if reply.Reject || reply.Match != 3 {
		t.Fatalf("expected success matching 3, got reject=%v match=%d", reply.Reject, reply.Match)
	}
	if follower.LastIndex() != 3 || follower.Commit() != 3 {
		t.Fatalf("log should be cut to 3 and committed, last=%d commit=%d", follower.LastIndex(), follower.Commit())
	}
	if term, _ := follower.termAt(3); term != 3 {
		t.Fatalf("index 3 should now be from term 3, is %d", term)
	}
}

func TestCommitNeverPassesThePrefixAMessageVerified(t *testing.T) {
	follower := New(Config{ID: 1, Peers: []int{0, 1, 2}, ElectionTick: 10, HeartbeatTick: 2},
		&Persistent{Term: 1, Vote: None, Log: []Entry{{1, 1, nil}, {2, 1, nil}, {3, 1, nil}}})
	// A heartbeat at index 1 says the leader has committed 3. We may hold other entries at 2 and 3.
	follower.Step(Message{Type: MsgApp, From: 0, To: 1, Term: 1, LogIndex: 1, LogTerm: 1, Commit: 3})
	if follower.Commit() != 1 {
		t.Fatalf("committed %d on the strength of a message that only verified index 1", follower.Commit())
	}
}

func TestDuplicateAndReorderedAppendsAreHarmless(t *testing.T) {
	follower := New(Config{ID: 1, Peers: []int{0, 1, 2}, ElectionTick: 10, HeartbeatTick: 2}, NewPersistent())
	first := Message{Type: MsgApp, From: 0, To: 1, Term: 1, Entries: []Entry{{1, 1, []byte("a")}, {2, 1, []byte("b")}}}
	second := Message{Type: MsgApp, From: 0, To: 1, Term: 1, LogIndex: 2, LogTerm: 1, Entries: []Entry{{3, 1, []byte("c")}}, Commit: 3}
	for _, m := range []Message{first, second, first, first, second} {
		follower.Step(m)
	}
	if follower.LastIndex() != 3 || follower.Commit() != 3 {
		t.Fatalf("replays changed the log: last=%d commit=%d", follower.LastIndex(), follower.Commit())
	}
	if got := len(follower.Applies()); got != 3 {
		t.Fatalf("applied %d entries, want 3", got)
	}
}

// Raft section 5.4.2 and figure 8: an entry from an earlier term is not
// committed just because a quorum stores it. Only an entry of the leader's own
// term can be counted, and committing that one commits everything before it.
func TestLeaderDoesNotCommitEarlierTermsByCounting(t *testing.T) {
	peers := []int{0, 1, 2}
	leader := New(Config{ID: 0, Peers: peers, ElectionTick: 10, HeartbeatTick: 2},
		&Persistent{Term: 2, Vote: None, Log: []Entry{{1, 1, nil}, {2, 2, []byte("old")}}})
	leader.campaign(false)
	leader.Step(Message{Type: MsgVoteResp, From: 1, To: 0, Term: 3})
	if leader.State() != Leader || leader.LastIndex() != 3 {
		t.Fatalf("setup failed: state=%v last=%d", leader.State(), leader.LastIndex())
	}
	leader.Messages()

	leader.Step(Message{Type: MsgAppResp, From: 1, To: 0, Term: 3, Match: 2})
	if leader.Commit() != 0 {
		t.Fatalf("committed %d: a term 2 entry was counted by a term 3 leader", leader.Commit())
	}
	leader.Step(Message{Type: MsgAppResp, From: 1, To: 0, Term: 3, Match: 3})
	if leader.Commit() != 3 {
		t.Fatalf("commit should reach 3 once the leader's own entry is on a quorum, is %d", leader.Commit())
	}
}

func TestLaggingFollowerIsCaughtUpWithASnapshot(t *testing.T) {
	nodes := cluster(t, 3, nil)
	elect(t, nodes, 0)
	behind := nodes[2]
	delete(nodes, 2) // node 2 hears nothing for a while
	for i := 0; i < 10; i++ {
		nodes[0].Propose([]byte{byte('a' + i)})
		exchange(nodes)
	}
	leader := nodes[0]
	leader.Applies()
	leader.Compact(leader.Applied(), []byte("state@11"))
	if leader.FirstIndex() != 12 {
		t.Fatalf("log should start at 12 after compaction, starts at %d", leader.FirstIndex())
	}

	nodes[2] = behind
	leader.Propose([]byte("z"))
	exchange(nodes)

	applies := behind.Applies()
	if len(applies) == 0 || applies[0].Snapshot == nil || string(applies[0].Snapshot.Data) != "state@11" {
		t.Fatal("the lagging follower was not handed the snapshot first")
	}
	if last := applies[len(applies)-1]; last.Entry == nil || string(last.Entry.Data) != "z" {
		t.Fatal("the entry after the snapshot was not applied")
	}
	if behind.Commit() != leader.Commit() {
		t.Fatalf("follower commit %d, leader commit %d", behind.Commit(), leader.Commit())
	}
}

func TestPreVoteKeepsAnIsolatedNodeFromRaisingItsTerm(t *testing.T) {
	for _, preVote := range []bool{true, false} {
		lonely := cluster(t, 3, func(c *Config) { c.PreVote = preVote })[1]
		for i := 0; i < 500; i++ {
			lonely.Tick()
			lonely.Messages()
		}
		if preVote && lonely.Term() != 0 {
			t.Errorf("with Pre-Vote the term rose to %d while isolated", lonely.Term())
		}
		if !preVote && lonely.Term() < 5 {
			t.Errorf("without Pre-Vote the term should climb while isolated, is %d", lonely.Term())
		}
	}
}

func TestCheckQuorumMakesACutOffLeaderStepDown(t *testing.T) {
	nodes := cluster(t, 3, func(c *Config) { c.CheckQuorum = true })
	elect(t, nodes, 0)
	leader := nodes[0]
	for i := 0; i < 25; i++ {
		leader.Tick()
		leader.Messages() // nothing is delivered either way
	}
	if leader.State() == Leader {
		t.Fatal("the leader kept its role without hearing from a quorum for over two election timeouts")
	}
	if _, _, ok := leader.Propose([]byte("x")); ok {
		t.Fatal("a deposed leader accepted a proposal")
	}
}

func TestStaleLeaderStepsDownWhenAnsweredWithAHigherTerm(t *testing.T) {
	nodes := cluster(t, 3, nil)
	elect(t, nodes, 0)
	stale := nodes[0]
	delete(nodes, 0)
	nodes[1].campaign(false)
	exchange(nodes) // node 1 wins term 2 with node 2's vote
	if nodes[1].State() != Leader || nodes[1].Term() != 2 {
		t.Fatalf("setup failed: node 1 is %v in term %d", nodes[1].State(), nodes[1].Term())
	}
	nodes[0] = stale
	stale.Propose([]byte("lost"))
	exchange(nodes)
	if stale.State() != Follower || stale.Term() != 2 {
		t.Fatalf("the stale leader is %v in term %d", stale.State(), stale.Term())
	}
	for _, n := range nodes {
		for _, apply := range n.Applies() {
			if apply.Entry != nil && string(apply.Entry.Data) == "lost" {
				t.Fatal("an entry proposed to a deposed leader was applied")
			}
		}
	}
}

func TestRestartKeepsTermVoteAndLog(t *testing.T) {
	nodes := cluster(t, 3, nil)
	elect(t, nodes, 0)
	nodes[0].Propose([]byte("a"))
	exchange(nodes)
	nodes[0].Tick()
	exchange(nodes)

	old := nodes[1]
	reborn := New(old.cfg, old.p)
	if reborn.Term() != old.Term() || reborn.p.Vote != 0 || reborn.LastIndex() != old.LastIndex() {
		t.Fatalf("persistent state lost: term=%d vote=%d last=%d", reborn.Term(), reborn.p.Vote, reborn.LastIndex())
	}
	if reborn.State() != Follower || reborn.Commit() != 0 {
		t.Fatalf("volatile state should reset: state=%v commit=%d", reborn.State(), reborn.Commit())
	}
	// It must not vote for anyone else in the term it already voted in.
	reborn.Step(Message{Type: MsgVote, From: 2, To: 1, Term: reborn.Term(), LogIndex: 99, LogTerm: 99})
	if reply := reborn.Messages()[0]; !reply.Reject {
		t.Fatal("voted a second time in the same term after a restart")
	}
}
