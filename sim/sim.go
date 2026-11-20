// Package sim runs a whole Raft cluster, its network, its clients and its
// failures inside one goroutine, driven by one seeded random source.
//
// Nothing here reads a clock or starts a goroutine, so a run is a pure function
// of its Options: the same seed replays the same messages, crashes and
// partitions in the same order. A failing seed is a reproducible bug report.
//
// While it runs, the simulator checks the properties Raft promises:
//
//   - Election safety: at most one leader per term.
//   - State machine safety: no two nodes apply different entries at one index.
//   - Linearizability: the history the clients saw is explainable by one
//     sequential order of their operations (checked at the end, per key).
//   - Liveness after healing: once the network is whole and every node is up,
//     every outstanding operation completes.
//   - Convergence: every node ends with an identical store.
//
// Storage is modelled as synchronous and atomic: what a node wrote before a
// crash is there after it. Torn writes and lying disks are out of scope.
package sim

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"

	"github.com/forgehk/quorum/kv"
	"github.com/forgehk/quorum/lincheck"
	"github.com/forgehk/quorum/raft"
)

type Options struct {
	Seed          int64
	Nodes         int
	Clients       int
	Keys          int
	Ticks         int     // length of the phase in which faults are injected
	OpRate        float64 // chance per tick that an idle client starts an operation
	DropRate      float64 // chance a message is lost
	DupRate       float64 // chance a message is delivered twice
	MaxDelay      int     // a message takes 1 to MaxDelay ticks
	FaultEvery    int     // one fault roughly every this many ticks; 0 turns faults off
	SnapshotEvery int     // compact after this many applied entries; 0 turns compaction off
	RetryAfter    int     // ticks a client waits before it retries elsewhere
	ElectionTick  int
	HeartbeatTick int
	NoPreVote     bool
	NoCheckQuorum bool
	// UnsafeLocalReads answers gets from the local state of whichever node believes
	// it is the leader, without going through the log. It is wrong on purpose: a
	// deposed leader that has not yet noticed serves stale data. The tests use it
	// to prove the checker catches a real bug.
	UnsafeLocalReads bool
}

func (o Options) withDefaults() Options {
	set := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	set(&o.Nodes, 5)
	set(&o.Clients, 4)
	set(&o.Keys, 3)
	set(&o.MaxDelay, 4)
	set(&o.RetryAfter, 60)
	set(&o.ElectionTick, 20)
	set(&o.HeartbeatTick, 4)
	if o.OpRate == 0 {
		o.OpRate = 0.08
	}
	return o
}

type Stats struct {
	Ticks      int64
	Operations int
	Completed  int
	Terms      int // terms in which some node became leader
	Committed  uint64
	Snapshots  int
	Crashes    int
	Partitions int
	Messages   int
	Dropped    int
}

type Result struct {
	Stats
	Violation string // empty when every check passed
}

type packet struct {
	at  int64
	seq uint64
	msg raft.Message
}

type member struct {
	id          int
	p           *raft.Persistent
	node        *raft.Node // nil while crashed
	store       *kv.Store
	incarnation int
}

type client struct {
	id          uint64
	seq         uint64
	cmd         *kv.Command
	op          int
	target      int
	incarnation int
	deadline    int64
	waiting     bool
	seen        map[string]string
}

type Cluster struct {
	o        Options
	rng      *rand.Rand
	now      int64
	clock    int64
	seq      uint64
	members  []*member
	queue    []packet
	cut      map[[2]int]bool
	clients  []*client
	issuing  bool
	lossy    bool
	history  []lincheck.Op
	leaders  map[uint64]int
	applied  map[uint64]uint64
	scripted map[uint64]kv.Result
	stats    Stats
	bad      string
}

const scriptedClient = 1 << 32

func New(o Options) *Cluster {
	o = o.withDefaults()
	c := &Cluster{
		o: o, rng: rand.New(rand.NewSource(o.Seed)), cut: map[[2]int]bool{}, lossy: true,
		leaders: map[uint64]int{}, applied: map[uint64]uint64{}, scripted: map[uint64]kv.Result{},
	}
	for id := 0; id < o.Nodes; id++ {
		m := &member{id: id, p: raft.NewPersistent()}
		c.members = append(c.members, m)
		c.start(m)
	}
	for id := 0; id < o.Clients; id++ {
		c.clients = append(c.clients, &client{id: uint64(id + 1), seen: map[string]string{}})
	}
	return c
}

func (c *Cluster) start(m *member) {
	peers := make([]int, c.o.Nodes)
	for i := range peers {
		peers[i] = i
	}
	m.incarnation++
	m.store = kv.New()
	m.node = raft.New(raft.Config{
		ID: m.id, Peers: peers,
		ElectionTick: c.o.ElectionTick, HeartbeatTick: c.o.HeartbeatTick,
		PreVote: !c.o.NoPreVote, CheckQuorum: !c.o.NoCheckQuorum,
		Rand: rand.New(rand.NewSource(c.o.Seed*1000003 + int64(m.id)*7919 + int64(m.incarnation))),
	}, m.p)
}

// Tick advances the whole cluster by one unit of time.
func (c *Cluster) Tick() {
	c.now++
	c.stats.Ticks = c.now
	c.deliver()
	for _, m := range c.members {
		if m.node != nil {
			m.node.Tick()
		}
	}
	c.runClients()
	for _, m := range c.members {
		c.drain(m)
	}
}

func (c *Cluster) Run(ticks int) {
	for i := 0; i < ticks && c.bad == ""; i++ {
		c.Tick()
	}
}

func (c *Cluster) deliver() {
	var due []packet
	rest := c.queue[:0]
	for _, p := range c.queue {
		if p.at <= c.now {
			due = append(due, p)
		} else {
			rest = append(rest, p)
		}
	}
	c.queue = rest
	sort.Slice(due, func(i, j int) bool {
		if due[i].at != due[j].at {
			return due[i].at < due[j].at
		}
		return due[i].seq < due[j].seq
	})
	for _, p := range due {
		to := c.members[p.msg.To]
		if to.node == nil || c.cut[[2]int{p.msg.From, p.msg.To}] {
			c.stats.Dropped++
			continue
		}
		to.node.Step(p.msg)
	}
}

func (c *Cluster) send(m raft.Message) {
	c.stats.Messages++
	if c.lossy && c.rng.Float64() < c.o.DropRate {
		c.stats.Dropped++
		return
	}
	c.enqueue(m)
	if c.lossy && c.rng.Float64() < c.o.DupRate {
		c.enqueue(m)
	}
}

func (c *Cluster) enqueue(m raft.Message) {
	c.seq++
	c.queue = append(c.queue, packet{at: c.now + 1 + int64(c.rng.Intn(c.o.MaxDelay)), seq: c.seq, msg: m})
}

func (c *Cluster) drain(m *member) {
	if m.node == nil {
		return
	}
	for _, msg := range m.node.Messages() {
		c.send(msg)
	}
	for _, apply := range m.node.Applies() {
		if apply.Snapshot != nil {
			m.store = kv.New()
			if err := m.store.Restore(apply.Snapshot.Data); err != nil {
				c.fail("node %d could not restore a snapshot: %v", m.id, err)
			}
			continue
		}
		entry := apply.Entry
		digest := fnv.New64a()
		fmt.Fprintf(digest, "%d|", entry.Term)
		digest.Write(entry.Data)
		if prior, ok := c.applied[entry.Index]; ok && prior != digest.Sum64() {
			c.fail("state machine safety: node %d applied a different entry at index %d", m.id, entry.Index)
		}
		c.applied[entry.Index] = digest.Sum64()
		if entry.Index > c.stats.Committed {
			c.stats.Committed = entry.Index
		}
		if entry.Data == nil {
			continue
		}
		cmd, err := kv.Decode(entry.Data)
		if err != nil {
			c.fail("node %d found an undecodable entry at index %d", m.id, entry.Index)
			continue
		}
		c.respond(m, cmd, m.store.Apply(cmd))
	}
	if c.o.SnapshotEvery > 0 && m.node.Applied()-m.p.Snapshot.Index >= uint64(c.o.SnapshotEvery) {
		m.node.Compact(m.node.Applied(), m.store.Snapshot())
		c.stats.Snapshots++
	}
	if m.node.State() == raft.Leader {
		term := m.node.Term()
		if other, ok := c.leaders[term]; ok && other != m.id {
			c.fail("election safety: nodes %d and %d were both leader in term %d", other, m.id, term)
		}
		c.leaders[term] = m.id
		c.stats.Terms = len(c.leaders)
	}
}

func (c *Cluster) respond(m *member, cmd kv.Command, result kv.Result) {
	if cmd.Client == scriptedClient {
		c.scripted[cmd.Seq] = result
		return
	}
	cl := c.clients[cmd.Client-1]
	if cl.cmd == nil || cl.seq != cmd.Seq || !cl.waiting || cl.target != m.id || cl.incarnation != m.incarnation {
		return
	}
	c.complete(cl, result)
}

func (c *Cluster) complete(cl *client, result kv.Result) {
	op := &c.history[cl.op]
	op.Pending, op.Return, op.Output = false, c.event(), result
	if cl.cmd.Op == kv.Get && result.Found {
		cl.seen[cl.cmd.Key] = result.Value
	}
	cl.cmd, cl.waiting = nil, false
	c.stats.Completed++
}

func (c *Cluster) event() int64 {
	c.clock++
	return c.clock
}

func (c *Cluster) runClients() {
	n := len(c.members)
	for _, cl := range c.clients {
		if cl.cmd == nil {
			if !c.issuing || c.rng.Float64() >= c.o.OpRate {
				continue
			}
			cl.seq++
			cmd := c.randomCommand(cl)
			cl.cmd, cl.waiting, cl.target = &cmd, false, c.rng.Intn(n)
			cl.op = len(c.history)
			c.history = append(c.history, lincheck.Op{Client: int(cl.id), Call: c.event(), Pending: true, Input: cmd})
			c.stats.Operations++
		}
		target := c.members[cl.target]
		if cl.waiting {
			if c.now < cl.deadline && target.node != nil && target.incarnation == cl.incarnation {
				continue
			}
			// No answer in time, or the node died. Retry the same command somewhere else.
			cl.waiting = false
			cl.target = (cl.target + 1 + c.rng.Intn(n-1)) % n
			continue
		}
		if target.node == nil {
			cl.target = c.rng.Intn(n)
			continue
		}
		if target.node.State() != raft.Leader {
			if lead := target.node.Leader(); lead != raft.None && lead != cl.target {
				cl.target = lead
			} else {
				cl.target = c.rng.Intn(n)
			}
			continue
		}
		if c.o.UnsafeLocalReads && cl.cmd.Op == kv.Get {
			c.complete(cl, target.store.Read(cl.cmd.Key))
			continue
		}
		target.node.Propose(kv.Encode(*cl.cmd))
		cl.waiting, cl.incarnation, cl.deadline = true, target.incarnation, c.now+int64(c.o.RetryAfter)
	}
}

func (c *Cluster) randomCommand(cl *client) kv.Command {
	cmd := kv.Command{Client: cl.id, Seq: cl.seq, Key: fmt.Sprintf("k%d", c.rng.Intn(c.o.Keys))}
	fresh := fmt.Sprintf("%d.%d", cl.id, cl.seq)
	switch r := c.rng.Intn(100); {
	case r < 35:
		cmd.Op = kv.Get
	case r < 70:
		cmd.Op, cmd.Value = kv.Put, fresh
	case r < 80:
		cmd.Op = kv.Del
	default:
		cmd.Op, cmd.Value, cmd.Expect = kv.CAS, fresh, cl.seen[cmd.Key]
	}
	return cmd
}

func (c *Cluster) injectFault() {
	n := len(c.members)
	switch r := c.rng.Intn(10); {
	case r < 3:
		order := c.rng.Perm(n)
		split := 1 + c.rng.Intn(n-1)
		c.Partition(order[:split], order[split:])
	case r < 4:
		if lead := c.Leader(); lead != raft.None {
			c.Isolate(lead)
		}
	case r < 6:
		c.Heal()
	case r < 8:
		var up []int
		for _, m := range c.members {
			if m.node != nil {
				up = append(up, m.id)
			}
		}
		if n-len(up) < (n-1)/2 {
			c.Crash(up[c.rng.Intn(len(up))])
		}
	default:
		var down []int
		for _, m := range c.members {
			if m.node == nil {
				down = append(down, m.id)
			}
		}
		if len(down) > 0 {
			c.Restart(down[c.rng.Intn(len(down))])
		}
	}
}

// Partition cuts every link between nodes in different groups.
func (c *Cluster) Partition(groups ...[]int) {
	c.Heal()
	side := map[int]int{}
	for g, ids := range groups {
		for _, id := range ids {
			side[id] = g + 1
		}
	}
	for a := range c.members {
		for b := range c.members {
			if a != b && side[a] != side[b] {
				c.cut[[2]int{a, b}] = true
			}
		}
	}
	c.stats.Partitions++
}

// Isolate cuts one node off from all the others.
func (c *Cluster) Isolate(id int) {
	var rest []int
	for _, m := range c.members {
		if m.id != id {
			rest = append(rest, m.id)
		}
	}
	c.Partition([]int{id}, rest)
}

func (c *Cluster) Heal() { c.cut = map[[2]int]bool{} }

// Crash stops a node and discards everything it held in memory.
func (c *Cluster) Crash(id int) {
	m := c.members[id]
	if m.node != nil {
		m.node, m.store = nil, nil
		c.stats.Crashes++
	}
}

// Restart brings a crashed node back from its persistent state.
func (c *Cluster) Restart(id int) {
	if m := c.members[id]; m.node == nil {
		c.start(m)
	}
}

// Leader returns the running leader with the highest term, or raft.None.
func (c *Cluster) Leader() int {
	lead, term := raft.None, uint64(0)
	for _, m := range c.members {
		if m.node != nil && m.node.State() == raft.Leader && m.node.Term() >= term {
			lead, term = m.id, m.node.Term()
		}
	}
	return lead
}

// WaitLeader ticks until some node is leader, for at most limit ticks.
func (c *Cluster) WaitLeader(limit int) int {
	for i := 0; i < limit; i++ {
		if lead := c.Leader(); lead != raft.None {
			return lead
		}
		c.Tick()
	}
	return c.Leader()
}

// Do submits one command and ticks until it is applied, retrying through whoever
// is leader. It reports false if the command did not commit within limit ticks.
func (c *Cluster) Do(seq uint64, cmd kv.Command, limit int) (kv.Result, bool) {
	cmd.Client, cmd.Seq = scriptedClient, seq
	proposedTo, proposedAt := raft.None, int64(0)
	for i := 0; i < limit; i++ {
		if result, ok := c.scripted[seq]; ok {
			return result, true
		}
		lead := c.Leader()
		if lead != raft.None && (lead != proposedTo || c.now-proposedAt > int64(c.o.RetryAfter)) {
			c.members[lead].node.Propose(kv.Encode(cmd))
			proposedTo, proposedAt = lead, c.now
		}
		c.Tick()
	}
	result, ok := c.scripted[seq]
	return result, ok
}

func (c *Cluster) Node(id int) *raft.Node { return c.members[id].node }
func (c *Cluster) Store(id int) *kv.Store { return c.members[id].store }
func (c *Cluster) Violation() string      { return c.bad }
func (c *Cluster) Stats() Stats           { return c.stats }

func (c *Cluster) fail(format string, args ...any) {
	if c.bad == "" {
		c.bad = fmt.Sprintf("tick %d: ", c.now) + fmt.Sprintf(format, args...)
	}
}

func (c *Cluster) busy() bool {
	for _, cl := range c.clients {
		if cl.cmd != nil {
			return true
		}
	}
	return false
}

func (c *Cluster) settled() bool {
	for _, m := range c.members {
		if m.node == nil || m.node.Applied() != c.stats.Committed || m.node.Commit() != c.stats.Committed {
			return false
		}
	}
	return true
}

// Run executes one complete experiment: a faulty phase with clients, then a
// healed phase in which everything must finish, then the end-of-run checks.
func Run(o Options) Result {
	c := New(o)
	c.issuing = true
	for i := 0; i < c.o.Ticks && c.bad == ""; i++ {
		if c.o.FaultEvery > 0 && c.rng.Intn(c.o.FaultEvery) == 0 {
			c.injectFault()
		}
		c.Tick()
	}

	// Heal everything. From here on the cluster owes us progress.
	c.issuing, c.lossy = false, false
	c.Heal()
	for _, m := range c.members {
		c.Restart(m.id)
	}
	const patience = 4000
	for i := 0; i < patience && c.bad == "" && (c.busy() || !c.settled()); i++ {
		c.Tick()
	}
	switch {
	case c.bad != "":
	case c.busy():
		c.fail("liveness: operations were still outstanding %d ticks after the network healed", patience)
	case !c.settled():
		c.fail("liveness: nodes had not caught up %d ticks after the network healed", patience)
	default:
		want := c.members[0].store.Snapshot()
		for _, m := range c.members[1:] {
			if !bytes.Equal(want, m.store.Snapshot()) {
				c.fail("convergence: node %d ended with a different store than node 0", m.id)
			}
		}
	}
	if c.bad == "" {
		if ok, key := lincheck.CheckKV(c.history); !ok {
			c.fail("linearizability: no sequential order explains the history of key %q", key)
		}
	}
	return Result{Stats: c.stats, Violation: c.bad}
}
