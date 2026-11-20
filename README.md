# quorum

> Raft consensus in Go, written as a deterministic state machine, with a simulator that runs a whole cluster, its network and its failures from one seed, and a linearizability checker that judges what the clients saw.

[![CI](https://github.com/forgehk/quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/forgehk/quorum/actions/workflows/ci.yml)
[![Go 1.22+](https://img.shields.io/badge/go-1.22+-00ADD8.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Consensus code is easy to write and hard to trust. The bugs live in interleavings: a message that arrives after a crash, a leader that was deposed and has not noticed, a retry that lands twice. You cannot reach those with unit tests, and you cannot reproduce them on a real network.

So the Raft node here owns no goroutines, clocks or sockets. Every input is an explicit call. That lets the simulator run five nodes, four clients, a lossy network and a fault injector inside one goroutine, driven by one seeded random source. A run is a pure function of its seed: a failing seed is a bug report you can replay.

No dependencies outside the Go standard library.

**Status:** a study implementation. The node is the algorithm and nothing else: it has no network transport and no on-disk log, and the simulator is its only host. It is not a substitute for a production Raft library.

```text
                 one seed
                    │
   ┌────────────────▼─────────────────────────────────────────┐
   │ simulator: one goroutine, one random source, no clocks    │
   │                                                           │
   │   clients ──▶ kv store ──▶ raft node x5 ◀──▶ network      │
   │      │        sessions     Tick / Step       drop, delay, │
   │      │                                       partition    │
   │      │        fault injector: crash, restart,             │
   │      │        isolate the leader                          │
   │      ▼                                                    │
   │   history of every call and return                        │
   └──────┬────────────────────────────────────────────────────┘
          ▼
   linearizability checker ──▶ "no property violated", or the seed that broke it
```

## Quick start

Needs Go 1.22 or newer and nothing else.

```bash
go test ./...                                    # 34 tests, about 10 seconds
go run ./cmd/quorum-sim -seeds 500               # 500 simulated clusters, about 10 seconds
go run ./cmd/quorum-sim -seed 1337 -seeds 1 -v   # replay one seed, tick by tick
```

## What is implemented

| | |
|---|---|
| Leader election | Randomized timeouts, one vote per term, the log up-to-date check |
| Pre-Vote | A node asks whether it could win before it raises its term, so a node returning from a partition does not depose a healthy leader |
| CheckQuorum | A leader that stops hearing from a quorum steps down, and nodes that hear from a leader ignore candidates |
| Log replication | Conflict back-off that skips a whole term per round trip, batching, duplicate and reordered messages tolerated |
| Commit rule | Only entries of the leader's own term are counted (Raft section 5.4.2) |
| Compaction | The log is cut at a snapshot of the state machine |
| Snapshot transfer | A follower too far behind to be caught up from the log is sent the snapshot |
| Exactly-once commands | Client id and sequence number on every command, a session table in the state machine and in its snapshots |

Not implemented: membership changes, leadership transfer, read leases, a network transport and an on-disk log. The node is the algorithm; the simulator is its only host so far.

## What the simulator checks

Every tick:

- **Election safety.** At most one leader per term.
- **State machine safety.** No two nodes apply different entries at the same index, across crashes and restarts.

At the end of every run, after the network is healed and every node restarted:

- **Liveness.** Every operation a client started completes.
- **Convergence.** Every node holds a byte-identical store.
- **Linearizability.** The history the clients observed (calls, returns, results) can be explained by one sequential order of their operations that respects real time. Checked per key with the Wing and Gong search and Lowe's memoization.

The first two and linearizability are safety properties: they must hold at every moment, whatever the network does. Liveness is weaker than its name. Raft cannot promise progress while the network keeps failing, and nothing here claims it does. The check is that the cluster recovers once the faults stop.

The checker's assumptions: each key's history is judged on its own, each client has one operation in flight at a time, and every operation has returned by the time the history is checked.

The fault injector partitions the cluster at random, isolates the current leader, crashes nodes (never more than a minority at once) and restarts them. Messages are delayed by a random number of ticks, which reorders them, and are dropped and duplicated at configurable rates. Clients retry timed-out commands against other nodes, which is what exercises the exactly-once path.

```console
$ go run ./cmd/quorum-sim -seeds 1500
1500 seeds in 24.166s: 6020413 ticks, 989607 operations (989607 completed), 9765 leader terms, 1012587 entries committed,
91317 snapshots, 6014 crashes, 15189 partitions, 38790922 messages (6527499 lost)
no property violated
```

Those are seeds 0 through 1499 on Go 1.24 and two cores of a 2.1 GHz Xeon. A run is a function of its seed, so the counts above repeat exactly on any machine; only the time changes.

## Does the checker actually catch bugs?

A checker that never fails proves nothing, so I broke things on purpose.

**Stale reads.** `-unsafe-reads` answers a get from the local state of whichever node believes it is the leader, without going through the log. A deposed leader that has not noticed yet serves data that newer writes have replaced. The linearizability check catches it, and a test keeps it honest:

```console
$ go run ./cmd/quorum-sim -seeds 40 -unsafe-reads
seed 17: VIOLATION at tick 4013: linearizability: no sequential order explains the history of key "k1"
seed 21: VIOLATION at tick 4005: linearizability: no sequential order explains the history of key "k1"
seed 26: VIOLATION at tick 4009: linearizability: no sequential order explains the history of key "k2"
```

Any failing seed replays on its own:

```console
$ go run ./cmd/quorum-sim -seed 17 -seeds 1 -unsafe-reads
seed 17: VIOLATION at tick 4013: linearizability: no sequential order explains the history of key "k1"
1 seeds in 19ms: 4013 ticks, 725 operations (725 completed), 9 leader terms, 496 entries committed,
46 snapshots, 6 crashes, 11 partitions, 19664 messages (3708 lost)
1 seed(s) violated a property
```

**Votes without the up-to-date check.** With that one condition removed from vote granting, a node with a stale log can win an election and overwrite committed entries. The simulator reports it as a state machine safety violation, though rarely: one seed in 300 on a three node cluster.

**Counting replicas of an old term's entry.** Removing the current-term condition from the commit rule was *not* caught in 600 seeds. The schedule that exposes it (figure 8 in the Raft paper) is too specific for random faults to hit at this scale. That rule is covered by a unit test that builds the exact situation instead. Random simulation finds a lot, and it is not a proof.

## Use

```bash
go test ./...                          # 34 tests, about 10 seconds
go test -race ./...                    # the same under the race detector
QUORUM_SEEDS=2000 go test ./sim        # more seeds per property test

go run ./cmd/quorum-sim -seeds 500
go run ./cmd/quorum-sim -seed 1337 -seeds 1 -v          # replay one seed
go run ./cmd/quorum-sim -nodes 3 -drop 0.2 -fault-every 60
```

Driving a node yourself:

```go
node := raft.New(raft.Config{ID: 0, Peers: []int{0, 1, 2}, ElectionTick: 20, HeartbeatTick: 4,
	PreVote: true, CheckQuorum: true, Rand: rand.New(rand.NewSource(1))}, raft.NewPersistent())

node.Tick()                    // time passed
node.Step(msg)                 // a message arrived
node.Propose(data)             // leaders only
for _, m := range node.Messages() { send(m) }
for _, a := range node.Applies() { apply(a) }   // an entry, or a snapshot to restore from
```

## Layout

```text
raft/       the algorithm: one file, no goroutines, no I/O
kv/         the replicated state machine: get, put, delete, compare-and-swap, sessions
lincheck/   the linearizability checker and the key-value model it checks against
sim/        cluster, network, clients, fault injector and the property checks
cmd/quorum-sim/   run seeds from the command line
```

## Limits

- Storage is modelled as synchronous and atomic. What a node wrote before a crash is there afterwards. Torn writes and disks that lie about fsync are out of scope.
- The linearizability search can take exponential time to *reject* a history with many overlapping operations. The simulator keeps the overlap small: one operation in flight per client.
- Passing seeds are evidence, not proof. See the experiment above that random faults missed.

## License

MIT
