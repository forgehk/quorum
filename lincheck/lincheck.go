// Package lincheck decides whether a concurrent history is linearizable.
//
// A history is a set of operations, each with the moment it was called and the
// moment it returned. It is linearizable if the operations can be put in one
// sequential order that respects real time (an operation that returned before
// another was called comes first) and in which every operation's output is what
// a correct sequential object would have produced.
//
// The search is the Wing and Gong algorithm with Lowe's memoization: try each
// operation that could go next, remember every (set of operations done, state)
// pair already explored, and backtrack when stuck.
//
// An operation that never returned may have taken effect at any point after its
// call, or not at all. It is modelled as returning after everything else with an
// unknown output, which covers both cases.
//
// Cost: accepting a history is usually fast, but rejecting one can take time
// exponential in the number of operations that overlap each other. The simulator
// keeps that number small (one operation in flight per client, and every
// operation finishes before the check runs).
package lincheck

import (
	"sort"
	"strings"
)

// Op is one operation in a history. Call and Return are positions in a single
// total order of events, so no two are equal. Return is ignored when Pending is set.
type Op struct {
	Client  int
	Call    int64
	Return  int64
	Pending bool
	Input   any
	Output  any
}

// Model is the sequential specification the history is checked against.
type Model struct {
	// Init returns the initial state. States are strings so they can be memoized.
	Init func() string
	// Step applies input to state. If known is true it also checks that output is
	// what the object would have returned, and reports false if it is not.
	Step func(state string, input, output any, known bool) (ok bool, next string)
}

type event struct {
	op     int
	isCall bool
	time   int64
}

type node struct {
	op         int
	match      *node // on a call: its return. nil on a return.
	prev, next *node
}

// Check reports whether history is linearizable with respect to model.
func Check(model Model, history []Op) bool {
	if len(history) == 0 {
		return true
	}
	const never = int64(1) << 62
	events := make([]event, 0, 2*len(history))
	for i, op := range history {
		ret := op.Return
		if op.Pending {
			ret = never + int64(i)
		}
		events = append(events, event{i, true, op.Call}, event{i, false, ret})
	}
	sort.Slice(events, func(a, b int) bool { return events[a].time < events[b].time })

	head := &node{op: -1}
	tail := head
	calls := make([]*node, len(history))
	for _, e := range events {
		n := &node{op: e.op, prev: tail}
		tail.next = n
		tail = n
		if e.isCall {
			calls[e.op] = n
		} else {
			calls[e.op].match = n
		}
	}

	type frame struct {
		entry *node
		state string
	}
	var stack []frame
	done := make([]uint64, (len(history)+63)/64)
	seen := map[string]struct{}{}
	state := model.Init()
	entry := head.next

	for head.next != nil {
		if entry.match != nil {
			op := history[entry.op]
			ok, next := model.Step(state, op.Input, op.Output, !op.Pending)
			if ok {
				done[entry.op/64] |= 1 << (entry.op % 64)
				key := memoKey(done, next)
				if _, visited := seen[key]; !visited {
					seen[key] = struct{}{}
					stack = append(stack, frame{entry, state})
					state = next
					lift(entry)
					entry = head.next
					continue
				}
				done[entry.op/64] &^= 1 << (entry.op % 64)
			}
			entry = entry.next
			continue
		}
		// Reached a return whose call has not been placed: the current prefix is a dead end.
		if len(stack) == 0 {
			return false
		}
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = top.state
		done[top.entry.op/64] &^= 1 << (top.entry.op % 64)
		unlift(top.entry)
		entry = top.entry.next
	}
	return true
}

func lift(call *node) {
	call.prev.next = call.next
	call.next.prev = call.prev
	ret := call.match
	ret.prev.next = ret.next
	if ret.next != nil {
		ret.next.prev = ret.prev
	}
}

func unlift(call *node) {
	ret := call.match
	ret.prev.next = ret
	if ret.next != nil {
		ret.next.prev = ret
	}
	call.prev.next = call
	call.next.prev = call
}

func memoKey(done []uint64, state string) string {
	var b strings.Builder
	b.Grow(len(done)*8 + 1 + len(state))
	for _, word := range done {
		for shift := 0; shift < 64; shift += 8 {
			b.WriteByte(byte(word >> shift))
		}
	}
	b.WriteByte('|')
	b.WriteString(state)
	return b.String()
}
