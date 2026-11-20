// Package raft is the Raft consensus algorithm as a deterministic state machine.
//
// A Node owns no goroutines, clocks or sockets. The caller drives it:
//
//	node.Tick()            // one unit of logical time has passed
//	node.Step(msg)         // a message arrived from a peer
//	node.Propose(data)     // a client wants this appended (leaders only)
//	node.Messages()        // what to send
//	node.Applies()         // what the state machine must apply, in order
//
// Because every input is explicit, a whole cluster can be run inside one
// goroutine with a seeded random source and replayed exactly. The sim package
// does that.
//
// Implemented: leader election with randomized timeouts, Pre-Vote, CheckQuorum,
// log replication with fast conflict back-off, the current-term commit rule,
// log compaction, and snapshot transfer to followers that have fallen behind.
// Not implemented: membership changes and leadership transfer.
package raft

import (
	"math/rand"
	"sort"
)

// None means "no node": no leader known, or no vote cast.
const None = -1

type StateType int

const (
	Follower StateType = iota
	PreCandidate
	Candidate
	Leader
)

func (s StateType) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[s]
}

type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte // nil for the no-op a new leader appends
}

// Snapshot stands in for every entry up to and including Index.
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type MsgType int

const (
	MsgPreVote MsgType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgSnap
)

type Message struct {
	Type     MsgType
	From, To int
	Term     uint64
	LogIndex uint64 // votes: the candidate's last index. Appends: the index just before Entries.
	LogTerm  uint64 // the term of LogIndex
	Entries  []Entry
	Commit   uint64
	Reject   bool
	Match    uint64 // append responses: the highest index known to match the leader
	Hint     uint64 // rejected appends: where the leader should retry from
	Snapshot *Snapshot
}

// Persistent is the state Raft requires on stable storage. A Node writes to it
// before it sends any message that depends on the write, so a caller that keeps
// the same Persistent across a crash gets a correct restart.
type Persistent struct {
	Term     uint64
	Vote     int
	Snapshot Snapshot
	Log      []Entry // entries after Snapshot.Index
}

func NewPersistent() *Persistent { return &Persistent{Vote: None} }

type Config struct {
	ID            int
	Peers         []int // every member, including ID
	ElectionTick  int   // a follower waits between ElectionTick and 2*ElectionTick ticks
	HeartbeatTick int
	PreVote       bool // ask before disrupting: do not bump the term until a quorum would vote
	CheckQuorum   bool // a leader that cannot reach a quorum steps down
	MaxBatch      int  // most entries in one append
	Rand          *rand.Rand
}

// Apply is one step for the state machine: either an entry or a snapshot to restore from.
type Apply struct {
	Entry    *Entry
	Snapshot *Snapshot
}

type Node struct {
	cfg   Config
	p     *Persistent
	state StateType
	lead  int

	commit  uint64
	applied uint64

	electionElapsed  int
	heartbeatElapsed int
	timeout          int

	votes  map[int]bool
	next   map[int]uint64
	match  map[int]uint64
	active map[int]bool

	msgs        []Message
	pendingSnap *Snapshot
}

// New starts a node from its persistent state. A node that restarts with a
// snapshot hands that snapshot to the state machine first.
func New(cfg Config, p *Persistent) *Node {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 64
	}
	n := &Node{cfg: cfg, p: p, lead: None, commit: p.Snapshot.Index, applied: p.Snapshot.Index}
	if p.Snapshot.Index > 0 {
		snap := p.Snapshot
		n.pendingSnap = &snap
	}
	n.resetTimer()
	return n
}

func (n *Node) ID() int            { return n.cfg.ID }
func (n *Node) State() StateType   { return n.state }
func (n *Node) Term() uint64       { return n.p.Term }
func (n *Node) Leader() int        { return n.lead }
func (n *Node) Commit() uint64     { return n.commit }
func (n *Node) Applied() uint64    { return n.applied }
func (n *Node) LastIndex() uint64  { return n.lastIndex() }
func (n *Node) FirstIndex() uint64 { return n.p.Snapshot.Index + 1 }

// Tick advances logical time by one unit.
func (n *Node) Tick() {
	n.electionElapsed++
	if n.state != Leader {
		if n.electionElapsed >= n.timeout {
			n.campaign(n.cfg.PreVote)
		}
		return
	}
	if n.cfg.CheckQuorum && n.electionElapsed >= n.cfg.ElectionTick {
		n.electionElapsed = 0
		alive := 1
		for _, id := range n.cfg.Peers {
			if id != n.cfg.ID && n.active[id] {
				alive++
			}
			n.active[id] = false
		}
		if alive < n.quorum() {
			n.becomeFollower(n.p.Term, None)
			return
		}
	}
	n.heartbeatElapsed++
	if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
		n.heartbeatElapsed = 0
		n.broadcast()
	}
}

// Propose appends data to the log if this node is the leader. The entry is
// committed only when it later comes out of Applies.
func (n *Node) Propose(data []byte) (index, term uint64, ok bool) {
	if n.state != Leader {
		return 0, 0, false
	}
	n.appendEntry(data)
	n.broadcast()
	return n.lastIndex(), n.p.Term, true
}

// Messages returns the messages to send and clears the outbox.
func (n *Node) Messages() []Message {
	out := n.msgs
	n.msgs = nil
	return out
}

// Applies returns what the state machine must apply next, in order.
func (n *Node) Applies() []Apply {
	var out []Apply
	if n.pendingSnap != nil {
		out = append(out, Apply{Snapshot: n.pendingSnap})
		n.pendingSnap = nil
	}
	for n.applied < n.commit {
		n.applied++
		entry := n.p.Log[n.applied-n.p.Snapshot.Index-1]
		out = append(out, Apply{Entry: &entry})
	}
	return out
}

// Compact discards the log through index, which the state machine has already
// applied, and keeps data as the snapshot that replaces it.
func (n *Node) Compact(index uint64, data []byte) {
	if index <= n.p.Snapshot.Index || index > n.applied {
		return
	}
	term, _ := n.termAt(index)
	n.p.Log = append([]Entry(nil), n.p.Log[index-n.p.Snapshot.Index:]...)
	n.p.Snapshot = Snapshot{Index: index, Term: term, Data: data}
}

// Step handles one message from a peer.
func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.p.Term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) && n.inLease() {
			// We heard from a leader recently, so this candidate does not speak for a quorum.
			return
		}
		switch {
		case m.Type == MsgPreVote:
			// A pre-vote asks about a future term. It never changes ours.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// A grant carries the term we asked about, which is one ahead of ours.
		default:
			lead := None
			if m.Type == MsgApp || m.Type == MsgSnap {
				lead = m.From
			}
			n.becomeFollower(m.Term, lead)
		}
	case m.Term < n.p.Term:
		// Answer with our term so a stale sender catches up, then drop the message.
		switch m.Type {
		case MsgPreVote:
			n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: n.p.Term, Reject: true})
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.p.Term, Reject: true})
		case MsgApp, MsgSnap:
			n.send(Message{Type: MsgAppResp, To: m.From, Term: n.p.Term, Reject: true})
		}
		return
	}

	switch m.Type {
	case MsgPreVote, MsgVote:
		n.handleVote(m)
	case MsgPreVoteResp:
		if n.state != PreCandidate || (!m.Reject && m.Term != n.p.Term+1) {
			return
		}
		if win, lose := n.poll(m.From, !m.Reject); win {
			n.campaign(false)
		} else if lose {
			n.becomeFollower(n.p.Term, None)
		}
	case MsgVoteResp:
		if n.state != Candidate {
			return
		}
		if win, lose := n.poll(m.From, !m.Reject); win {
			n.becomeLeader()
		} else if lose {
			n.becomeFollower(n.p.Term, None)
		}
	case MsgApp:
		if n.state == Leader {
			return // two leaders in one term cannot happen; ignore rather than corrupt state
		}
		if n.state != Follower {
			n.becomeFollower(n.p.Term, m.From)
		}
		n.handleAppend(m)
	case MsgSnap:
		if n.state == Leader {
			return
		}
		if n.state != Follower {
			n.becomeFollower(n.p.Term, m.From)
		}
		n.handleSnapshot(m)
	case MsgAppResp:
		if n.state == Leader {
			n.handleAppendResp(m)
		}
	}
}

func (n *Node) handleVote(m Message) {
	canVote := n.p.Vote == m.From ||
		(n.p.Vote == None && n.lead == None) ||
		(m.Type == MsgPreVote && m.Term > n.p.Term)
	upToDate := m.LogTerm > n.lastTerm() || (m.LogTerm == n.lastTerm() && m.LogIndex >= n.lastIndex())
	grant := canVote && upToDate

	if m.Type == MsgPreVote {
		term := n.p.Term
		if grant {
			term = m.Term
		}
		n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: term, Reject: !grant})
		return
	}
	if grant {
		n.p.Vote = m.From
		n.electionElapsed = 0
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.p.Term, Reject: !grant})
}

func (n *Node) handleAppend(m Message) {
	n.lead = m.From
	n.electionElapsed = 0

	prev, prevTerm, entries := m.LogIndex, m.LogTerm, m.Entries
	lastNew := prev + uint64(len(entries))
	snap := n.p.Snapshot

	if prev < snap.Index {
		// Everything through our snapshot is committed and identical on the leader. Skip it.
		skip := snap.Index - prev
		if skip >= uint64(len(entries)) {
			n.send(Message{Type: MsgAppResp, To: m.From, Term: n.p.Term, Match: snap.Index})
			return
		}
		entries, prev, prevTerm = entries[skip:], snap.Index, snap.Term
	}
	if prev > n.lastIndex() {
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.p.Term, Reject: true, Hint: n.lastIndex() + 1})
		return
	}
	if ours, _ := n.termAt(prev); ours != prevTerm {
		// Skip back over the whole conflicting term in one round trip.
		hint := prev
		for hint > snap.Index+1 {
			if t, _ := n.termAt(hint - 1); t != ours {
				break
			}
			hint--
		}
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.p.Term, Reject: true, Hint: hint})
		return
	}
	for i, entry := range entries {
		if entry.Index <= n.lastIndex() {
			if t, _ := n.termAt(entry.Index); t == entry.Term {
				continue
			}
			// Our suffix disagrees with the leader. It was never committed; drop it.
			n.p.Log = n.p.Log[:entry.Index-snap.Index-1]
		}
		n.p.Log = append(n.p.Log, entries[i:]...)
		break
	}
	// Only the prefix this message verified is known to match, so commit no further than that.
	if c := min(m.Commit, lastNew); c > n.commit {
		n.commit = c
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Term: n.p.Term, Match: lastNew})
}

func (n *Node) handleSnapshot(m Message) {
	n.lead = m.From
	n.electionElapsed = 0
	snap := *m.Snapshot
	if snap.Index <= n.commit {
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.p.Term, Match: n.commit})
		return
	}
	if t, ok := n.termAt(snap.Index); ok && t == snap.Term {
		n.p.Log = append([]Entry(nil), n.p.Log[snap.Index-n.p.Snapshot.Index:]...)
	} else {
		n.p.Log = nil
	}
	n.p.Snapshot = snap
	n.commit = snap.Index
	n.applied = snap.Index
	n.pendingSnap = &snap
	n.send(Message{Type: MsgAppResp, To: m.From, Term: n.p.Term, Match: snap.Index})
}

func (n *Node) handleAppendResp(m Message) {
	n.active[m.From] = true
	if m.Reject {
		if m.Hint > 0 && m.Hint < n.next[m.From] {
			n.next[m.From] = m.Hint
		} else if n.next[m.From] > 1 {
			n.next[m.From]--
		}
		if n.next[m.From] <= n.match[m.From] {
			n.next[m.From] = n.match[m.From] + 1
		}
		n.sendAppend(m.From)
		return
	}
	if m.Match > n.match[m.From] {
		n.match[m.From] = m.Match
		if n.next[m.From] <= m.Match {
			n.next[m.From] = m.Match + 1
		}
		if n.maybeCommit() {
			n.broadcast()
			return
		}
	}
	if n.next[m.From] <= n.lastIndex() {
		n.sendAppend(m.From)
	}
}

func (n *Node) campaign(pre bool) {
	n.resetTimer()
	n.lead = None
	n.votes = map[int]bool{n.cfg.ID: true}
	term := n.p.Term + 1
	kind := MsgPreVote
	if pre {
		n.state = PreCandidate
	} else {
		n.state = Candidate
		n.p.Term = term
		n.p.Vote = n.cfg.ID
		kind = MsgVote
	}
	if n.quorum() == 1 {
		if pre {
			n.campaign(false)
		} else {
			n.becomeLeader()
		}
		return
	}
	for _, id := range n.cfg.Peers {
		if id != n.cfg.ID {
			n.send(Message{Type: kind, To: id, Term: term, LogIndex: n.lastIndex(), LogTerm: n.lastTerm()})
		}
	}
}

func (n *Node) poll(from int, granted bool) (win, lose bool) {
	n.votes[from] = granted
	yes, no := 0, 0
	for _, id := range n.cfg.Peers {
		if v, voted := n.votes[id]; voted {
			if v {
				yes++
			} else {
				no++
			}
		}
	}
	return yes >= n.quorum(), len(n.cfg.Peers)-no < n.quorum()
}

func (n *Node) becomeFollower(term uint64, lead int) {
	if term > n.p.Term {
		n.p.Term = term
		n.p.Vote = None
	}
	n.state = Follower
	n.lead = lead
	n.resetTimer()
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.lead = n.cfg.ID
	n.resetTimer()
	n.next, n.match, n.active = map[int]uint64{}, map[int]uint64{}, map[int]bool{}
	for _, id := range n.cfg.Peers {
		n.next[id] = n.lastIndex() + 1
	}
	n.match[n.cfg.ID] = n.lastIndex()
	// A leader may only count replicas for entries of its own term, so it starts one.
	n.appendEntry(nil)
	n.broadcast()
}

func (n *Node) appendEntry(data []byte) {
	n.p.Log = append(n.p.Log, Entry{Index: n.lastIndex() + 1, Term: n.p.Term, Data: data})
	n.match[n.cfg.ID] = n.lastIndex()
	n.maybeCommit()
}

// maybeCommit advances the commit index to the highest index stored on a quorum,
// but only if that entry is from the current term (Raft section 5.4.2).
func (n *Node) maybeCommit() bool {
	matched := make([]uint64, 0, len(n.cfg.Peers))
	for _, id := range n.cfg.Peers {
		matched = append(matched, n.match[id])
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i] > matched[j] })
	index := matched[n.quorum()-1]
	if index <= n.commit {
		return false
	}
	if term, ok := n.termAt(index); !ok || term != n.p.Term {
		return false
	}
	n.commit = index
	return true
}

func (n *Node) broadcast() {
	for _, id := range n.cfg.Peers {
		if id != n.cfg.ID {
			n.sendAppend(id)
		}
	}
}

func (n *Node) sendAppend(to int) {
	next := n.next[to]
	if next <= n.p.Snapshot.Index {
		// The entries this follower needs are gone. Send the snapshot that replaced them.
		snap := n.p.Snapshot
		n.send(Message{Type: MsgSnap, To: to, Term: n.p.Term, Snapshot: &snap})
		return
	}
	prevTerm, _ := n.termAt(next - 1)
	n.send(Message{
		Type: MsgApp, To: to, Term: n.p.Term,
		LogIndex: next - 1, LogTerm: prevTerm,
		Entries: n.entriesFrom(next), Commit: n.commit,
	})
}

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	n.msgs = append(n.msgs, m)
}

func (n *Node) inLease() bool {
	return n.cfg.CheckQuorum && n.lead != None && n.electionElapsed < n.cfg.ElectionTick
}

func (n *Node) resetTimer() {
	n.electionElapsed = 0
	n.heartbeatElapsed = 0
	n.timeout = n.cfg.ElectionTick
	if n.cfg.Rand != nil && n.cfg.ElectionTick > 0 {
		n.timeout += n.cfg.Rand.Intn(n.cfg.ElectionTick)
	}
}

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) lastIndex() uint64 { return n.p.Snapshot.Index + uint64(len(n.p.Log)) }

func (n *Node) lastTerm() uint64 {
	term, _ := n.termAt(n.lastIndex())
	return term
}

func (n *Node) termAt(index uint64) (uint64, bool) {
	first := n.p.Snapshot.Index
	switch {
	case index == first:
		return n.p.Snapshot.Term, true
	case index < first || index > n.lastIndex():
		return 0, false
	}
	return n.p.Log[index-first-1].Term, true
}

func (n *Node) entriesFrom(index uint64) []Entry {
	start := index - n.p.Snapshot.Index - 1
	end := uint64(len(n.p.Log))
	if end-start > uint64(n.cfg.MaxBatch) {
		end = start + uint64(n.cfg.MaxBatch)
	}
	return append([]Entry(nil), n.p.Log[start:end]...)
}
