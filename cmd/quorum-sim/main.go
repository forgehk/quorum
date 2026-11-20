// quorum-sim runs the deterministic simulator over a range of seeds.
//
//	quorum-sim -seeds 200                 # 200 seeds with the default fault mix
//	quorum-sim -seed 1337 -seeds 1 -v     # replay one seed
//	quorum-sim -unsafe-reads              # watch the checker catch stale reads
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/forgehk/quorum/sim"
)

func main() {
	var o sim.Options
	first := flag.Int64("seed", 1, "first seed")
	seeds := flag.Int("seeds", 100, "how many seeds to run")
	verbose := flag.Bool("v", false, "print a line for every seed")
	flag.IntVar(&o.Nodes, "nodes", 5, "cluster size")
	flag.IntVar(&o.Clients, "clients", 4, "concurrent clients")
	flag.IntVar(&o.Ticks, "ticks", 4000, "length of the faulty phase")
	flag.IntVar(&o.FaultEvery, "fault-every", 150, "one fault about every N ticks (0 = none)")
	flag.IntVar(&o.SnapshotEvery, "snapshot-every", 40, "compact after N applied entries (0 = never)")
	flag.Float64Var(&o.DropRate, "drop", 0.05, "chance a message is lost")
	flag.Float64Var(&o.DupRate, "dup", 0.02, "chance a message is delivered twice")
	flag.BoolVar(&o.NoPreVote, "no-prevote", false, "turn Pre-Vote off")
	flag.BoolVar(&o.NoCheckQuorum, "no-check-quorum", false, "turn CheckQuorum off")
	flag.BoolVar(&o.UnsafeLocalReads, "unsafe-reads", false, "serve reads from local state (deliberately wrong)")
	flag.Parse()

	started := time.Now()
	var total sim.Stats
	failures := 0
	for i := 0; i < *seeds; i++ {
		o.Seed = *first + int64(i)
		result := sim.Run(o)
		total.Ticks += result.Ticks
		total.Operations += result.Operations
		total.Completed += result.Completed
		total.Terms += result.Terms
		total.Committed += result.Committed
		total.Snapshots += result.Snapshots
		total.Crashes += result.Crashes
		total.Partitions += result.Partitions
		total.Messages += result.Messages
		total.Dropped += result.Dropped
		if result.Violation != "" {
			failures++
			fmt.Printf("seed %d: VIOLATION at %s\n", o.Seed, result.Violation)
		} else if *verbose {
			fmt.Printf("seed %d: ok  ops=%d terms=%d committed=%d snapshots=%d crashes=%d partitions=%d\n",
				o.Seed, result.Operations, result.Terms, result.Committed, result.Snapshots, result.Crashes, result.Partitions)
		}
	}
	fmt.Printf("%d seeds in %s: %d ticks, %d operations (%d completed), %d leader terms, %d entries committed,\n",
		*seeds, time.Since(started).Round(time.Millisecond), total.Ticks, total.Operations, total.Completed, total.Terms, total.Committed)
	fmt.Printf("%d snapshots, %d crashes, %d partitions, %d messages (%d lost)\n",
		total.Snapshots, total.Crashes, total.Partitions, total.Messages, total.Dropped)
	if failures > 0 {
		fmt.Printf("%d seed(s) violated a property\n", failures)
		os.Exit(1)
	}
	fmt.Println("no property violated")
}
