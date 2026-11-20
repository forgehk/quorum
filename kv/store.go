// Package kv is the replicated state machine: a string key-value store whose
// commands are safe to apply more than once.
//
// A client that times out cannot tell whether its command was committed, so it
// retries. Every command therefore carries a client id and a sequence number,
// and the store remembers the last sequence number and result for each client.
// A repeated command returns the remembered result and changes nothing.
package kv

import "encoding/json"

const (
	Get = "get"
	Put = "put"
	Del = "del"
	CAS = "cas" // set Value only if the current value equals Expect
)

type Command struct {
	Client uint64 `json:"c"`
	Seq    uint64 `json:"s"`
	Op     string `json:"o"`
	Key    string `json:"k"`
	Value  string `json:"v,omitempty"`
	Expect string `json:"e,omitempty"`
}

type Result struct {
	Value string `json:"v,omitempty"`  // get: the value read
	Found bool   `json:"f,omitempty"`  // get, del: the key existed
	OK    bool   `json:"ok,omitempty"` // cas: the swap happened
}

type session struct {
	Seq    uint64 `json:"s"`
	Result Result `json:"r"`
}

type Store struct {
	data     map[string]string
	sessions map[uint64]session
}

func New() *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]session{}}
}

// Apply runs a command once. A command already applied returns its first result.
func (s *Store) Apply(c Command) Result {
	if last, seen := s.sessions[c.Client]; seen && c.Seq <= last.Seq {
		return last.Result
	}
	result := s.execute(c)
	s.sessions[c.Client] = session{Seq: c.Seq, Result: result}
	return result
}

// Read answers a get from local state without recording it. It is only correct
// on a node that knows it is still the leader, which is the bug the simulator's
// unsafe-reads mode exists to demonstrate.
func (s *Store) Read(key string) Result {
	value, found := s.data[key]
	return Result{Value: value, Found: found}
}

func (s *Store) execute(c Command) Result {
	current, found := s.data[c.Key]
	switch c.Op {
	case Get:
		return Result{Value: current, Found: found}
	case Put:
		s.data[c.Key] = c.Value
	case Del:
		delete(s.data, c.Key)
		return Result{Found: found}
	case CAS:
		if found && current == c.Expect {
			s.data[c.Key] = c.Value
			return Result{OK: true}
		}
	}
	return Result{}
}

type image struct {
	Data     map[string]string  `json:"data"`
	Sessions map[uint64]session `json:"sessions"`
}

// Snapshot serializes the whole store, sessions included. The encoding sorts map
// keys, so equal stores produce equal bytes.
func (s *Store) Snapshot() []byte {
	out, err := json.Marshal(image{Data: s.data, Sessions: s.sessions})
	if err != nil {
		panic(err)
	}
	return out
}

func (s *Store) Restore(snapshot []byte) error {
	var in image
	if err := json.Unmarshal(snapshot, &in); err != nil {
		return err
	}
	s.data, s.sessions = in.Data, in.Sessions
	if s.data == nil {
		s.data = map[string]string{}
	}
	if s.sessions == nil {
		s.sessions = map[uint64]session{}
	}
	return nil
}

func Encode(c Command) []byte {
	out, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return out
}

func Decode(data []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(data, &c)
	return c, err
}
