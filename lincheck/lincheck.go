// Package lincheck checks whether a history of concurrent key-value
// operations is linearizable: whether every operation can be assigned a
// single instant between its call and return such that the resulting
// sequential order is a legal execution of a key-value map.
//
// The search is the Wing & Gong algorithm with Lowe's memoization (the same
// approach as Knossos and Porcupine). Keys are independent registers, so the
// history is split per key and each part is checked on its own
// (P-compositionality), which keeps the search small.
//
// Operations whose outcome the client never learned (timeouts, crashed
// connections) are "pending": they may have taken effect at any point after
// they were called, or not at all.
package lincheck

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
	"strings"
)

// Kind is the operation type.
type Kind uint8

const (
	Put Kind = iota
	Get
	Delete
	CAS
)

func (k Kind) String() string {
	return [...]string{"put", "get", "delete", "cas"}[k]
}

// Operation is one client call and, unless Pending, its observed result.
type Operation struct {
	Client int
	Kind   Kind
	Key    string

	// Inputs.
	Value        string // put value, or cas new value
	Expected     string // cas expected value
	ExpectAbsent bool   // cas: succeed only if the key is absent

	// Outputs, meaningful only when !Pending.
	Found   bool   // get: key existed; delete: key existed
	Out     string // get: value read; failed cas: current value ("" if absent)
	Swapped bool   // cas succeeded

	Call    int64 // invocation time (any monotonic unit)
	Return  int64 // response time
	Pending bool  // no response: outcome unknown
}

func (o Operation) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "c%d %s(%q", o.Client, o.Kind, o.Key)
	switch o.Kind {
	case Put:
		fmt.Fprintf(&b, ", %q)", o.Value)
	case CAS:
		if o.ExpectAbsent {
			fmt.Fprintf(&b, ", <absent> -> %q)", o.Value)
		} else {
			fmt.Fprintf(&b, ", %q -> %q)", o.Expected, o.Value)
		}
	default:
		b.WriteString(")")
	}
	if o.Pending {
		fmt.Fprintf(&b, " => ? [%d, ...]", o.Call)
		return b.String()
	}
	switch o.Kind {
	case Get:
		if o.Found {
			fmt.Fprintf(&b, " => %q", o.Out)
		} else {
			b.WriteString(" => <absent>")
		}
	case Delete:
		fmt.Fprintf(&b, " => existed=%v", o.Found)
	case CAS:
		fmt.Fprintf(&b, " => swapped=%v", o.Swapped)
		if !o.Swapped {
			fmt.Fprintf(&b, " current=%q", o.Out)
		}
	default:
		b.WriteString(" => ok")
	}
	fmt.Fprintf(&b, " [%d, %d]", o.Call, o.Return)
	return b.String()
}

// Result of a check.
type Result struct {
	OK bool
	// Unknown is set when the search hit its budget without an answer.
	Unknown bool
	// For a failure: the offending key, its history sorted by call time, the
	// longest prefix of it the search managed to linearize, and the first
	// completed operation that could not be placed after that prefix.
	Key        string
	History    []Operation
	Linearized []Operation
	Blocked    *Operation
	Explored   int
}

func (r Result) String() string {
	switch {
	case r.OK:
		return "linearizable"
	case r.Unknown:
		return fmt.Sprintf("unknown: search budget exhausted on key %q after %d states", r.Key, r.Explored)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "NOT linearizable on key %q (%d ops on this key)\n", r.Key, len(r.History))
	lin := r.Linearized
	if len(lin) > 20 {
		fmt.Fprintf(&b, "longest linearizable order found: %d ops, last 20:\n", len(lin))
		lin = lin[len(lin)-20:]
	} else {
		fmt.Fprintf(&b, "longest linearizable order found (%d ops):\n", len(lin))
	}
	for _, o := range lin {
		fmt.Fprintf(&b, "  %s\n", o)
	}
	if r.Blocked != nil {
		fmt.Fprintf(&b, "cannot be linearized next:\n  %s\n", *r.Blocked)
		b.WriteString("operations concurrent with it:\n")
		shown := 0
		for _, o := range r.History {
			if o.Call <= r.Blocked.Return && (o.Pending || o.Return >= r.Blocked.Call) && o != *r.Blocked {
				fmt.Fprintf(&b, "  %s\n", o)
				if shown++; shown == 40 {
					b.WriteString("  ...\n")
					break
				}
			}
		}
	}
	return b.String()
}

// DefaultBudget bounds the states explored per key.
const DefaultBudget = 2_000_000

// Check verifies a whole history with the default budget.
func Check(history []Operation) Result { return CheckBudget(history, DefaultBudget) }

// CheckBudget verifies a history, exploring at most budget states per key.
func CheckBudget(history []Operation, budget int) Result {
	byKey := map[string][]Operation{}
	for _, op := range history {
		// A get that never returned has no effect and constrains nothing.
		if op.Pending && op.Kind == Get {
			continue
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	total := 0
	for _, k := range keys {
		r := checkKey(k, byKey[k], budget)
		total += r.Explored
		if !r.OK {
			r.Explored = total
			return r
		}
	}
	return Result{OK: true, Explored: total}
}

// ---- model: one key as a register that may be absent ----

type state struct {
	present bool
	val     string
}

func (s state) encode() string {
	if !s.present {
		return "\x00"
	}
	return "\x01" + s.val
}

// step applies op to s, reporting whether the op's observed result is legal.
func step(s state, op *Operation) (state, bool) {
	switch op.Kind {
	case Put:
		return state{true, op.Value}, true
	case Get:
		if op.Found != s.present || (s.present && op.Out != s.val) {
			return s, false
		}
		return s, true
	case Delete:
		if !op.Pending && op.Found != s.present {
			return s, false
		}
		return state{}, true
	case CAS:
		match := (op.ExpectAbsent && !s.present) || (!op.ExpectAbsent && s.present && s.val == op.Expected)
		if !op.Pending {
			if op.Swapped != match {
				return s, false
			}
			if !match && op.Out != s.val {
				return s, false // reported the wrong current value
			}
		}
		if match {
			return state{true, op.Value}, true
		}
		return s, true
	}
	return s, false
}

// ---- search ----

type event struct {
	op     int
	isCall bool
	time   int64
	match  *event // call <-> return
	prev   *event
	next   *event
}

func lift(e *event) {
	e.prev.next = e.next
	if e.next != nil {
		e.next.prev = e.prev
	}
	r := e.match
	r.prev.next = r.next
	if r.next != nil {
		r.next.prev = r.prev
	}
}

func unlift(e *event) {
	r := e.match
	r.prev.next = r
	if r.next != nil {
		r.next.prev = r
	}
	e.prev.next = e
	if e.next != nil {
		e.next.prev = e
	}
}

// Memo keys use Zobrist hashing: each op gets two random 64-bit words, and
// the set of linearized ops is the XOR of its members' words, updated in O(1).
// Combined with a hash of the model state this gives a 128-bit key; a false
// collision would need a 2^-128-scale accident.
type zobrist struct {
	a, b []uint64
}

func newZobrist(n int) zobrist {
	rng := rand.New(rand.NewSource(int64(n)*7919 + 1))
	z := zobrist{a: make([]uint64, n), b: make([]uint64, n)}
	for i := 0; i < n; i++ {
		z.a[i], z.b[i] = rng.Uint64(), rng.Uint64()
	}
	return z
}

type memoKey [2]uint64

func stateHash(s state) memoKey {
	h1 := fnv.New64a()
	h1.Write([]byte(s.encode()))
	h2 := fnv.New64()
	h2.Write([]byte(s.encode()))
	return memoKey{h1.Sum64(), h2.Sum64()}
}

type frame struct {
	call  *event
	state state
}

func checkKey(key string, ops []Operation, budget int) Result {
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	events := make([]*event, 0, 2*len(ops))
	for i := range ops {
		ret := ops[i].Return
		if ops[i].Pending {
			ret = math.MaxInt64
		}
		c := &event{op: i, isCall: true, time: ops[i].Call}
		r := &event{op: i, time: ret}
		c.match, r.match = r, c
		events = append(events, c, r)
	}
	// Order by time; at equal times calls go first, so touching intervals
	// count as concurrent (the safe, permissive reading).
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].time != events[j].time {
			return events[i].time < events[j].time
		}
		return events[i].isCall && !events[j].isCall
	})
	head := &event{}
	prev := head
	for _, e := range events {
		prev.next, e.prev = e, prev
		prev = e
	}

	z := newZobrist(len(ops))
	var lin memoKey // Zobrist hash of the linearized set
	seen := map[memoKey]struct{}{}
	var stack []frame
	// best is the longest stack seen; bestValid is how much of best still
	// matches the current stack, so recording a new best copies only the
	// part that changed since the last one.
	var best []frame
	bestValid := 0
	cur := state{}
	explored := 0

	e := head.next
	for e != nil {
		if e.isCall {
			op := &ops[e.op]
			next, ok := step(cur, op)
			if ok {
				lin[0] ^= z.a[e.op]
				lin[1] ^= z.b[e.op]
				sh := stateHash(next)
				k := memoKey{lin[0] ^ sh[0], lin[1] ^ sh[1]}
				if _, dup := seen[k]; !dup {
					seen[k] = struct{}{}
					explored++
					if explored > budget {
						return Result{Unknown: true, Key: key, History: ops, Explored: explored}
					}
					stack = append(stack, frame{e, cur})
					if len(stack) > len(best) {
						best = append(best[:bestValid], stack[bestValid:]...)
						bestValid = len(best)
					}
					cur = next
					lift(e)
					e = head.next
					continue
				}
				lin[0] ^= z.a[e.op]
				lin[1] ^= z.b[e.op]
			}
			e = e.next
			continue
		}
		// A return event: its op must already be linearized to move past it.
		if ops[e.op].Pending {
			// Only pending ops remain; they may simply never have happened.
			return Result{OK: true, Explored: explored}
		}
		if len(stack) == 0 {
			return failure(key, ops, best, explored)
		}
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if len(stack) < bestValid {
			bestValid = len(stack)
		}
		cur = top.state
		lin[0] ^= z.a[top.call.op]
		lin[1] ^= z.b[top.call.op]
		unlift(top.call)
		e = top.call.next
	}
	return Result{OK: true, Explored: explored}
}

// failure builds the report: the best linearization found and the earliest
// completed op left over after it.
func failure(key string, ops []Operation, best []frame, explored int) Result {
	r := Result{Key: key, History: ops, Explored: explored, Linearized: make([]Operation, len(best))}
	done := make([]bool, len(ops))
	for i, f := range best {
		r.Linearized[i] = ops[f.call.op]
		done[f.call.op] = true
	}
	var blocked *Operation
	for i := range ops {
		if done[i] || ops[i].Pending {
			continue
		}
		if blocked == nil || ops[i].Return < blocked.Return {
			blocked = &ops[i]
		}
	}
	if blocked != nil {
		cp := *blocked
		r.Blocked = &cp
	}
	return r
}
