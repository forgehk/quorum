package kv

import (
	"bytes"
	"testing"
)

func TestOperations(t *testing.T) {
	s := New()
	if r := s.Apply(Command{Client: 1, Seq: 1, Op: Get, Key: "a"}); r.Found {
		t.Fatal("found a key in an empty store")
	}
	s.Apply(Command{Client: 1, Seq: 2, Op: Put, Key: "a", Value: "1"})
	if r := s.Apply(Command{Client: 1, Seq: 3, Op: Get, Key: "a"}); !r.Found || r.Value != "1" {
		t.Fatalf("get after put: %+v", r)
	}
	if r := s.Apply(Command{Client: 1, Seq: 4, Op: CAS, Key: "a", Expect: "0", Value: "2"}); r.OK {
		t.Fatal("cas succeeded against the wrong value")
	}
	if r := s.Apply(Command{Client: 1, Seq: 5, Op: CAS, Key: "a", Expect: "1", Value: "2"}); !r.OK {
		t.Fatal("cas failed against the right value")
	}
	if r := s.Apply(Command{Client: 1, Seq: 6, Op: CAS, Key: "missing", Expect: "", Value: "x"}); r.OK {
		t.Fatal("cas created a key that did not exist")
	}
	if r := s.Apply(Command{Client: 1, Seq: 7, Op: Del, Key: "a"}); !r.Found {
		t.Fatal("delete did not report the key existed")
	}
	if r := s.Apply(Command{Client: 1, Seq: 8, Op: Del, Key: "a"}); r.Found {
		t.Fatal("second delete reported the key existed")
	}
}

func TestARepeatedCommandReturnsItsFirstResultAndChangesNothing(t *testing.T) {
	s := New()
	s.Apply(Command{Client: 1, Seq: 1, Op: Put, Key: "n", Value: "1"})
	swap := Command{Client: 2, Seq: 1, Op: CAS, Key: "n", Expect: "1", Value: "2"}
	if !s.Apply(swap).OK {
		t.Fatal("first cas failed")
	}
	s.Apply(Command{Client: 1, Seq: 2, Op: Put, Key: "n", Value: "1"}) // someone puts the old value back
	if !s.Apply(swap).OK {
		t.Fatal("the retry should report the original success")
	}
	if got := s.Read("n"); got.Value != "1" {
		t.Fatalf("the retry ran a second time: n=%q", got.Value)
	}
	stale := Command{Client: 1, Seq: 1, Op: Put, Key: "n", Value: "ancient"}
	s.Apply(stale)
	if got := s.Read("n"); got.Value != "1" {
		t.Fatalf("a command older than the client's last one was applied: n=%q", got.Value)
	}
}

func TestSnapshotRoundTripKeepsDataAndSessions(t *testing.T) {
	s := New()
	s.Apply(Command{Client: 7, Seq: 3, Op: Put, Key: "a", Value: "1"})
	s.Apply(Command{Client: 9, Seq: 1, Op: Put, Key: "b", Value: "2"})

	restored := New()
	if err := restored.Restore(s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored.Snapshot(), s.Snapshot()) {
		t.Fatal("snapshot of the restored store differs")
	}
	restored.Apply(Command{Client: 7, Seq: 3, Op: Put, Key: "a", Value: "again"})
	if got := restored.Read("a"); got.Value != "1" {
		t.Fatalf("the session table was not restored: a=%q", got.Value)
	}
}

func TestSnapshotsOfEqualStoresAreIdenticalBytes(t *testing.T) {
	a, b := New(), New()
	keys := []string{"x", "m", "a", "q", "c"}
	for i, k := range keys {
		a.Apply(Command{Client: uint64(i + 1), Seq: 1, Op: Put, Key: k, Value: k})
	}
	for i := len(keys) - 1; i >= 0; i-- {
		b.Apply(Command{Client: uint64(i + 1), Seq: 1, Op: Put, Key: keys[i], Value: keys[i]})
	}
	if !bytes.Equal(a.Snapshot(), b.Snapshot()) {
		t.Fatal("insertion order leaked into the snapshot")
	}
}

func TestRestoreRejectsGarbage(t *testing.T) {
	if err := New().Restore([]byte("not json")); err == nil {
		t.Fatal("restored from garbage without an error")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	want := Command{Client: 4, Seq: 9, Op: CAS, Key: "k", Value: "new", Expect: "old"}
	got, err := Decode(Encode(want))
	if err != nil || got != want {
		t.Fatalf("round trip: %+v, %v", got, err)
	}
	if _, err := Decode([]byte("{")); err == nil {
		t.Fatal("decoded a truncated command")
	}
}
