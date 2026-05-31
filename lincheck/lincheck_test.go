package lincheck

import (
	"fmt"
	"math/rand"
	"testing"
)

func put(c int, k, v string, call, ret int64) Operation {
	return Operation{Client: c, Kind: Put, Key: k, Value: v, Call: call, Return: ret}
}

func get(c int, k string, found bool, v string, call, ret int64) Operation {
	return Operation{Client: c, Kind: Get, Key: k, Found: found, Out: v, Call: call, Return: ret}
}

func del(c int, k string, existed bool, call, ret int64) Operation {
	return Operation{Client: c, Kind: Delete, Key: k, Found: existed, Call: call, Return: ret}
}

func cas(c int, k, exp string, absent bool, v string, swapped bool, cur string, call, ret int64) Operation {
	return Operation{Client: c, Kind: CAS, Key: k, Expected: exp, ExpectAbsent: absent, Value: v,
		Swapped: swapped, Out: cur, Call: call, Return: ret}
}

func pending(o Operation) Operation {
	o.Pending = true
	o.Return = 0
	return o
}

func TestHandWrittenHistories(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
		h    []Operation
	}{
		{"empty", true, nil},
		{"sequential", true, []Operation{
			put(1, "a", "1", 0, 1), get(1, "a", true, "1", 2, 3), del(1, "a", true, 4, 5), get(1, "a", false, "", 6, 7),
		}},
		{"stale read", false, []Operation{
			put(1, "a", "1", 0, 1), put(1, "a", "2", 2, 3), get(2, "a", true, "1", 4, 5),
		}},
		{"read of never-written value", false, []Operation{
			put(1, "a", "1", 0, 1), get(2, "a", true, "9", 2, 3),
		}},
		{"concurrent write visible later", true, []Operation{
			put(1, "a", "1", 0, 10), get(2, "a", false, "", 1, 2), get(2, "a", true, "1", 3, 4),
		}},
		{"value flips back to absent", false, []Operation{
			put(1, "a", "1", 0, 100), get(2, "a", true, "1", 10, 20), get(2, "a", false, "", 30, 40),
		}},
		{"two writers, readers agree on order", true, []Operation{
			put(1, "a", "x", 0, 50), put(2, "a", "y", 0, 50),
			get(3, "a", true, "x", 10, 20), get(3, "a", true, "y", 30, 40),
		}},
		{"two readers disagree on order", false, []Operation{
			put(1, "a", "x", 0, 100), put(2, "a", "y", 0, 100),
			get(3, "a", true, "x", 10, 20), get(3, "a", true, "y", 30, 40),
			get(4, "a", true, "y", 10, 20), get(4, "a", true, "x", 30, 40),
		}},
		{"cas both succeed from absent", false, []Operation{
			cas(1, "a", "", true, "p", true, "", 0, 10), cas(2, "a", "", true, "q", true, "", 0, 10),
		}},
		{"cas one wins", true, []Operation{
			cas(1, "a", "", true, "p", true, "", 0, 10), cas(2, "a", "", true, "q", false, "p", 0, 10),
		}},
		{"failed cas reports wrong current", false, []Operation{
			put(1, "a", "p", 0, 1), cas(2, "a", "z", false, "q", false, "WRONG", 2, 3),
		}},
		{"lost update", false, []Operation{
			put(1, "a", "0", 0, 1),
			cas(1, "a", "0", false, "1", true, "", 2, 10), cas(2, "a", "0", false, "2", true, "", 2, 10),
		}},
		{"delete reports existence", false, []Operation{
			del(1, "a", true, 0, 1),
		}},
		{"pending write never seen", true, []Operation{
			pending(put(1, "a", "1", 0, 0)), get(2, "a", false, "", 5, 6), get(2, "a", false, "", 7, 8),
		}},
		{"pending write seen later", true, []Operation{
			pending(put(1, "a", "1", 0, 0)), get(2, "a", false, "", 5, 6), get(2, "a", true, "1", 7, 8),
		}},
		{"pending write seen then unseen", false, []Operation{
			pending(put(1, "a", "1", 0, 0)), get(2, "a", true, "1", 5, 6), get(2, "a", false, "", 7, 8),
		}},
		{"pending write cannot apply before its call", false, []Operation{
			get(2, "a", true, "1", 0, 1), pending(put(1, "a", "1", 5, 0)),
		}},
		{"pending get ignored", true, []Operation{
			put(1, "a", "1", 0, 1), pending(get(2, "a", true, "zzz", 2, 0)),
		}},
		{"keys are independent", true, []Operation{
			put(1, "a", "1", 0, 1), put(1, "b", "2", 2, 3), get(2, "a", true, "1", 4, 5), get(2, "b", true, "2", 4, 5),
		}},
		{"touching intervals are concurrent", true, []Operation{
			put(1, "a", "1", 0, 5), get(2, "a", false, "", 5, 6),
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := Check(c.h)
			if r.OK != c.ok || r.Unknown {
				t.Fatalf("got %v, want ok=%v\n%s", r.OK, c.ok, r)
			}
		})
	}
}

// genLinearizable builds a random concurrent history that is linearizable
// by construction: ops execute sequentially at distinct instants, then each
// op's interval is stretched around its instant.
func genLinearizable(rng *rand.Rand, clients, ops, keys int) []Operation {
	state := map[string]*string{}
	var h []Operation
	busyUntil := make([]int64, clients)
	t := int64(0)
	for i := 0; i < ops; i++ {
		t += int64(1 + rng.Intn(3))
		c := rng.Intn(clients)
		if busyUntil[c] >= t { // a client has one op in flight at a time
			continue
		}
		key := fmt.Sprintf("k%d", rng.Intn(keys))
		cur := state[key]
		op := Operation{Client: c, Key: key}
		switch rng.Intn(4) {
		case 0:
			v := fmt.Sprintf("v%d", i)
			op.Kind, op.Value = Put, v
			state[key] = &v
		case 1:
			op.Kind = Get
			if cur != nil {
				op.Found, op.Out = true, *cur
			}
		case 2:
			op.Kind, op.Found = Delete, cur != nil
			state[key] = nil
		case 3:
			v := fmt.Sprintf("v%d", i)
			op.Kind, op.Value = CAS, v
			if cur == nil || rng.Intn(2) == 0 {
				op.ExpectAbsent = true
			} else {
				op.Expected = *cur
			}
			match := (op.ExpectAbsent && cur == nil) || (!op.ExpectAbsent && cur != nil && *cur == op.Expected)
			op.Swapped = match
			if match {
				state[key] = &v
			} else if cur != nil {
				op.Out = *cur
			}
		}
		op.Call = t - int64(rng.Intn(4))
		if op.Call <= busyUntil[c] {
			op.Call = busyUntil[c] + 1
		}
		if op.Call > t {
			op.Call = t
		}
		op.Return = t + int64(rng.Intn(6))
		busyUntil[c] = op.Return
		h = append(h, op)
	}
	return h
}

func TestRandomLinearizableHistoriesPass(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 300; i++ {
		h := genLinearizable(rng, 5, 150, 3)
		if r := Check(h); !r.OK {
			t.Fatalf("history %d wrongly rejected:\n%s", i, r)
		}
	}
}

func TestCorruptedHistoriesFail(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	caught := 0
	for i := 0; i < 200; i++ {
		h := genLinearizable(rng, 4, 80, 2)
		// Make one completed get return a value nobody ever wrote.
		for j := range h {
			if h[j].Kind == Get {
				h[j].Found, h[j].Out = true, "never-written"
				break
			}
		}
		if r := Check(h); !r.OK && !r.Unknown {
			caught++
		}
	}
	if caught == 0 {
		t.Fatal("no corrupted history was rejected")
	}
}

func TestPendingOpsPlausibility(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 200; i++ {
		h := genLinearizable(rng, 4, 100, 2)
		// Dropping a write's response keeps the history linearizable: the
		// write still happened, the client just did not hear back.
		for j := range h {
			if h[j].Kind != Get && rng.Intn(10) == 0 {
				h[j] = pending(h[j])
			}
		}
		if r := Check(h); !r.OK {
			t.Fatalf("history %d with pending ops wrongly rejected:\n%s", i, r)
		}
	}
}

func TestFailureReportNamesKey(t *testing.T) {
	r := Check([]Operation{put(1, "good", "1", 0, 1), put(1, "bad", "1", 0, 1), get(2, "bad", true, "2", 2, 3)})
	if r.OK || r.Key != "bad" || len(r.History) != 2 {
		t.Fatalf("report: %+v", r)
	}
	if s := r.String(); len(s) == 0 {
		t.Fatal("empty report")
	}
}

func BenchmarkCheck(b *testing.B) {
	rng := rand.New(rand.NewSource(4))
	h := genLinearizable(rng, 6, 600, 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if r := Check(h); !r.OK {
			b.Fatal(r)
		}
	}
}
