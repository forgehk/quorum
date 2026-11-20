package lincheck

import (
	"math/rand"
	"testing"

	"github.com/forgehk/quorum/kv"
)

func get(client int, call, ret int64, value string, found bool) Op {
	return Op{Client: client, Call: call, Return: ret, Input: kv.Command{Op: kv.Get, Key: "k"}, Output: kv.Result{Value: value, Found: found}}
}

func put(client int, call, ret int64, value string) Op {
	return Op{Client: client, Call: call, Return: ret, Input: kv.Command{Op: kv.Put, Key: "k", Value: value}, Output: kv.Result{}}
}

func cas(client int, call, ret int64, expect, value string, ok bool) Op {
	return Op{Client: client, Call: call, Return: ret,
		Input: kv.Command{Op: kv.CAS, Key: "k", Expect: expect, Value: value}, Output: kv.Result{OK: ok}}
}

func pending(op Op) Op {
	op.Pending, op.Output = true, nil
	return op
}

func TestHistories(t *testing.T) {
	cases := []struct {
		name    string
		history []Op
		want    bool
	}{
		{"empty", nil, true},
		{"read of a missing key", []Op{get(1, 1, 2, "", false)}, true},
		{"read of a value nobody wrote", []Op{get(1, 1, 2, "x", true)}, false},
		{"sequential write then read", []Op{put(1, 1, 2, "a"), get(2, 3, 4, "a", true)}, true},
		{"read misses a completed write", []Op{put(1, 1, 2, "a"), get(2, 3, 4, "", false)}, false},
		{"read concurrent with a write sees the old value", []Op{put(1, 1, 4, "a"), get(2, 2, 3, "", false)}, true},
		{"read concurrent with a write sees the new value", []Op{put(1, 1, 4, "a"), get(2, 2, 3, "a", true)}, true},
		{"stale read after an overwrite completed", []Op{put(1, 1, 2, "a"), put(1, 3, 4, "b"), get(2, 5, 6, "a", true)}, false},
		{"two readers disagree on the order of concurrent writes", []Op{
			put(1, 1, 10, "a"), put(2, 2, 11, "b"),
			get(3, 12, 13, "a", true), get(4, 14, 15, "b", true),
		}, false},
		{"value goes back in time between two reads", []Op{
			put(1, 1, 2, "a"), put(2, 3, 9, "b"),
			get(3, 4, 5, "b", true), get(3, 6, 7, "a", true),
		}, false},
		{"cas succeeds against the current value", []Op{put(1, 1, 2, "a"), cas(2, 3, 4, "a", "b", true), get(1, 5, 6, "b", true)}, true},
		{"cas claims success against the wrong value", []Op{put(1, 1, 2, "a"), cas(2, 3, 4, "z", "b", true)}, false},
		{"cas claims failure against the right value", []Op{put(1, 1, 2, "a"), cas(2, 3, 4, "a", "b", false)}, false},
		{"lost update: two swaps from the same value both succeed", []Op{
			put(1, 1, 2, "a"), cas(2, 3, 6, "a", "b", true), cas(3, 4, 7, "a", "c", true),
		}, false},
		{"a write that never returned took effect", []Op{pending(put(1, 1, 0, "a")), get(2, 2, 3, "a", true)}, true},
		{"a write that never returned did not take effect", []Op{pending(put(1, 1, 0, "a")), get(2, 2, 3, "", false)}, true},
		{"a write is observed before it was called", []Op{get(2, 1, 2, "a", true), pending(put(1, 3, 0, "a"))}, false},
		{"an unfinished write takes effect between two reads", []Op{
			put(1, 1, 2, "a"), pending(put(2, 3, 0, "b")),
			get(3, 4, 5, "a", true), get(3, 6, 7, "b", true),
		}, true},
		{"an unfinished write cannot be undone", []Op{
			put(1, 1, 2, "a"), pending(put(2, 3, 0, "b")),
			get(3, 4, 5, "b", true), get(3, 6, 7, "a", true),
		}, false},
	}
	for _, tc := range cases {
		if got := Check(KV, tc.history); got != tc.want {
			t.Errorf("%s: linearizable=%v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestCheckKVSplitsByKeyAndNamesTheBadOne(t *testing.T) {
	on := func(key string, op Op) Op {
		cmd := op.Input.(kv.Command)
		cmd.Key = key
		op.Input = cmd
		return op
	}
	history := []Op{
		on("a", put(1, 1, 2, "x")), on("b", put(2, 3, 4, "y")),
		on("a", get(1, 5, 6, "x", true)), on("b", get(2, 7, 8, "x", true)),
	}
	if ok, key := CheckKV(history); ok || key != "b" {
		t.Fatalf("ok=%v key=%q, want the violation on b", ok, key)
	}
	history[3] = on("b", get(2, 7, 8, "y", true))
	if ok, _ := CheckKV(history); !ok {
		t.Fatal("a correct two-key history was rejected")
	}
}

// Build histories from a real sequential store: pick a linearization order, then
// give every operation a window that contains its place in that order. Such a
// history is linearizable by construction. Corrupting one read must be caught
// whenever the corrupted value was never the value at any point in its window.
func TestGeneratedHistories(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for round := 0; round < 300; round++ {
		store := kv.New()
		var history []Op
		clock := int64(0)
		for i := 0; i < 24; i++ {
			cmd := kv.Command{Client: uint64(i + 1), Seq: 1, Key: "k"}
			switch rng.Intn(4) {
			case 0:
				cmd.Op = kv.Get
			case 1:
				cmd.Op, cmd.Value = kv.Put, string(rune('a'+rng.Intn(4)))
			case 2:
				cmd.Op = kv.Del
			default:
				cmd.Op, cmd.Expect, cmd.Value = kv.CAS, string(rune('a'+rng.Intn(4))), string(rune('a'+rng.Intn(4)))
			}
			point := clock + 10
			clock = point
			history = append(history, Op{
				Client: i, Call: point*100 - int64(rng.Intn(1500)) - 1 - int64(i), Return: point*100 + int64(rng.Intn(1500)) + 1 + int64(i),
				Input: cmd, Output: store.Apply(cmd),
			})
		}
		if !Check(KV, history) {
			t.Fatalf("round %d: a history built from a sequential run was rejected", round)
		}
		for i := range history {
			if history[i].Input.(kv.Command).Op == kv.Get {
				history[i].Output = kv.Result{Value: "never-written", Found: true}
				if Check(KV, history) {
					t.Fatalf("round %d: a read of a value nobody wrote was accepted", round)
				}
				break
			}
		}
	}
}

func TestOverlappingWritersAreSearchedExhaustively(t *testing.T) {
	// Twelve puts that all overlap, then a read. Rejecting the bad read means ruling out
	// every order, which is every subset of writers: the memo keeps that to 2^12 states.
	var history []Op
	for i := 0; i < 12; i++ {
		history = append(history, put(i, int64(i+1), int64(1000+i), string(rune('A'+i))))
	}
	history = append(history, get(99, 2000, 2001, "nobody", true))
	if Check(KV, history) {
		t.Fatal("accepted a read of a value none of the writers wrote")
	}
	history[len(history)-1] = get(99, 2000, 2001, "H", true)
	if !Check(KV, history) {
		t.Fatal("rejected a read of one of the concurrent writes")
	}
}
