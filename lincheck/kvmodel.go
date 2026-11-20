package lincheck

import "github.com/forgehk/quorum/kv"

// KV is the sequential specification of one key of the kv store. Histories are
// checked one key at a time: operations on different keys do not interact, so a
// history is linearizable exactly when each key's sub-history is.
//
// The state is "-" when the key is absent and "+" followed by the value otherwise.
var KV = Model{
	Init: func() string { return "-" },
	Step: func(state string, input, output any, known bool) (bool, string) {
		cmd := input.(kv.Command)
		present, value := state[0] == '+', state[1:]
		var out kv.Result
		if known {
			out = output.(kv.Result)
		}
		switch cmd.Op {
		case kv.Get:
			return !known || (out.Found == present && out.Value == value), state
		case kv.Put:
			return true, "+" + cmd.Value
		case kv.Del:
			return !known || out.Found == present, "-"
		case kv.CAS:
			if present && value == cmd.Expect {
				return !known || out.OK, "+" + cmd.Value
			}
			return !known || !out.OK, state
		}
		return false, state
	},
}

// CheckKV splits a history by key and checks each part. It returns the first key
// whose history is not linearizable.
func CheckKV(history []Op) (ok bool, badKey string) {
	byKey := map[string][]Op{}
	var order []string
	for _, op := range history {
		key := op.Input.(kv.Command).Key
		if _, seen := byKey[key]; !seen {
			order = append(order, key)
		}
		byKey[key] = append(byKey[key], op)
	}
	for _, key := range order {
		if !Check(KV, byKey[key]) {
			return false, key
		}
	}
	return true, ""
}
