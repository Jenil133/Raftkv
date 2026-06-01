// Package chaos runs randomized fault-injection scenarios against an
// in-process cluster and checks the results: a linearizability check of every
// client operation, election safety (one leader per term), liveness after
// faults heal, and convergence of all replicas.
//
// A seed fixes the scenario: cluster shape, fault schedule and client
// workload. Goroutine scheduling is not deterministic, so a failing seed
// usually but not always reproduces; every failure report carries the full
// operation history needed to analyse it.
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Jenil133/raftkv/internal/cluster"
	"github.com/Jenil133/raftkv/lincheck"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/shard"
)

// Config describes one scenario.
type Config struct {
	Seed          int64
	Nodes         int
	Shards        int
	SnapshotEvery uint64
	Clients       int
	Keys          int
	FaultPhase    time.Duration
	OpTimeout     time.Duration
	// BuggyReads makes clients read straight from a random replica's local
	// state, bypassing Raft. It exists only to prove the harness catches
	// consistency bugs.
	BuggyReads bool
}

// RandomConfig derives a scenario from seed.
func RandomConfig(seed int64) Config {
	rng := rand.New(rand.NewSource(seed))
	return Config{
		Seed:          seed,
		Nodes:         []int{3, 5, 5}[rng.Intn(3)],
		Shards:        1 + rng.Intn(2),
		SnapshotEvery: []uint64{0, 8, 25}[rng.Intn(3)],
		Clients:       3 + rng.Intn(4),
		Keys:          2 + rng.Intn(4),
		FaultPhase:    time.Duration(400+rng.Intn(500)) * time.Millisecond,
		OpTimeout:     2 * time.Second,
	}
}

func (c Config) String() string {
	return fmt.Sprintf("seed=%d nodes=%d shards=%d snapshotEvery=%d clients=%d keys=%d faultPhase=%v buggyReads=%v",
		c.Seed, c.Nodes, c.Shards, c.SnapshotEvery, c.Clients, c.Keys, c.FaultPhase, c.BuggyReads)
}

// Report is the outcome of one run.
type Report struct {
	Config     Config
	Ops        int
	Pending    int
	Faults     []string
	Violations []string
	Lin        lincheck.Result
	Elapsed    time.Duration
}

// OK reports whether every check passed.
func (r *Report) OK() bool { return len(r.Violations) == 0 && r.Lin.OK }

func (r *Report) String() string {
	var b strings.Builder
	status := "PASS"
	if !r.OK() {
		status = "FAIL"
	}
	fmt.Fprintf(&b, "%s %s ops=%d pending=%d faults=%d elapsed=%v\n",
		status, r.Config, r.Ops, r.Pending, len(r.Faults), r.Elapsed.Round(time.Millisecond))
	if r.OK() {
		return b.String()
	}
	for _, v := range r.Violations {
		fmt.Fprintf(&b, "violation: %s\n", v)
	}
	b.WriteString("fault timeline:\n")
	for _, f := range r.Faults {
		fmt.Fprintf(&b, "  %s\n", f)
	}
	if !r.Lin.OK {
		b.WriteString(r.Lin.String())
	}
	return b.String()
}

type history struct {
	mu    sync.Mutex
	start time.Time
	ops   []lincheck.Operation
}

func (h *history) now() int64 { return int64(time.Since(h.start)) }

func (h *history) add(op lincheck.Operation) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

// Run executes one scenario and checks it.
func Run(cfg Config) (rep Report) {
	if cfg.OpTimeout == 0 {
		cfg.OpTimeout = 2 * time.Second
	}
	start := time.Now()
	rep.Config = cfg
	runner := &cluster.Runner{}
	defer func() {
		if p := recover(); p != nil {
			fe, ok := p.(*cluster.FatalError)
			if !ok {
				panic(p)
			}
			rep.Violations = append(rep.Violations, "harness: "+fe.Msg)
		}
		runner.Close()
		rep.Elapsed = time.Since(start)
	}()

	rng := rand.New(rand.NewSource(cfg.Seed))
	c := cluster.New(runner, cfg.Nodes,
		cluster.WithShards(cfg.Shards),
		cluster.WithSnapshotEvery(cfg.SnapshotEvery),
		cluster.WithNetSeed(cfg.Seed),
		cluster.WithRaftConfig(func(rc *raft.Config) {
			rc.ElectionTimeoutMin = 60 * time.Millisecond
			rc.ElectionTimeoutMax = 120 * time.Millisecond
			rc.HeartbeatInterval = 15 * time.Millisecond
		}),
	)
	c.WaitAllLeaders(5 * time.Second)

	h := &history{start: time.Now()}
	smp := startSampler(c)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(id int, seed int64) {
			defer wg.Done()
			runClient(c, cfg, id, seed, h, stop)
		}(i, rng.Int63())
	}

	rep.Faults = injectFaults(c, cfg, rng, h)

	if !waitAllLeaders(c, 5*time.Second) {
		rep.Violations = append(rep.Violations, "liveness: some shard had no leader 5s after faults healed")
	}
	time.Sleep(150 * time.Millisecond) // let traffic flow on the healed cluster
	close(stop)
	wg.Wait()

	if err := finalReads(c, cfg, h); err != nil {
		rep.Violations = append(rep.Violations, "liveness: "+err.Error())
	}
	rep.Violations = append(rep.Violations, smp.finish()...)
	if v := checkConvergence(c, 5*time.Second); v != "" {
		rep.Violations = append(rep.Violations, v)
	}

	for _, op := range h.ops {
		if op.Pending {
			rep.Pending++
		}
	}
	rep.Ops = len(h.ops)
	rep.Lin = lincheck.Check(h.ops)
	return rep
}

func newClient(c *cluster.Cluster) *shard.Client {
	cl := c.ShardClient()
	for _, s := range cl.Shards() {
		s.AttemptTimeout = 250 * time.Millisecond
	}
	return cl
}

func runClient(c *cluster.Cluster, cfg Config, id int, seed int64, h *history, stop <-chan struct{}) {
	rng := rand.New(rand.NewSource(seed))
	cl := newClient(c)
	lastSeen := map[string]string{}
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("k%d", rng.Intn(cfg.Keys))
		op := lincheck.Operation{Client: id, Key: key}
		ctx, cancel := context.WithTimeout(context.Background(), cfg.OpTimeout)
		var err error
		op.Call = h.now()
		switch r := rng.Intn(100); {
		case r < 35:
			op.Kind, op.Value = lincheck.Put, fmt.Sprintf("c%d-%d", id, n)
			err = cl.Put(ctx, key, []byte(op.Value))
		case r < 70:
			op.Kind = lincheck.Get
			var v []byte
			if cfg.BuggyReads {
				v, op.Found = buggyRead(c, cl, key, rng)
			} else {
				v, op.Found, err = cl.Get(ctx, key)
			}
			op.Out = string(v)
		case r < 90:
			op.Kind, op.Value = lincheck.CAS, fmt.Sprintf("c%d-%d", id, n)
			if last, ok := lastSeen[key]; ok && rng.Intn(3) != 0 {
				op.Expected = last
			} else {
				op.ExpectAbsent = true
			}
			var cur []byte
			op.Swapped, cur, err = cl.CAS(ctx, key, []byte(op.Expected), op.ExpectAbsent, []byte(op.Value))
			if !op.Swapped {
				op.Out = string(cur)
			}
		default:
			op.Kind = lincheck.Delete
			op.Found, err = cl.Delete(ctx, key)
		}
		op.Return = h.now()
		cancel()
		if err != nil {
			op.Pending = true
		} else if op.Kind == lincheck.Get {
			if op.Found {
				lastSeen[key] = op.Out
			} else {
				delete(lastSeen, key)
			}
		}
		h.add(op)
	}
}

// buggyRead reads a random replica's local state with no consensus at all.
func buggyRead(c *cluster.Cluster, cl *shard.Client, key string, rng *rand.Rand) ([]byte, bool) {
	g := cl.ShardFor(key)
	ids := append([]raft.NodeID(nil), c.IDs...)
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	for _, id := range ids {
		if srv := c.Server(id, g); srv != nil {
			return srv.Store().Get(key)
		}
	}
	return nil, false
}

func injectFaults(c *cluster.Cluster, cfg Config, rng *rand.Rand, h *history) []string {
	var timeline []string
	note := func(format string, args ...any) {
		timeline = append(timeline, fmt.Sprintf("%5dms %s", h.now()/1e6, fmt.Sprintf(format, args...)))
	}
	maxDown := (cfg.Nodes - 1) / 2
	down := map[raft.NodeID]bool{}
	upNodes := func() []raft.NodeID {
		var out []raft.NodeID
		for _, id := range c.IDs {
			if !down[id] {
				out = append(out, id)
			}
		}
		return out
	}
	pick := func(ids []raft.NodeID) raft.NodeID { return ids[rng.Intn(len(ids))] }

	deadline := time.Now().Add(cfg.FaultPhase)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(20+rng.Intn(100)) * time.Millisecond)
		switch rng.Intn(11) {
		case 0, 1: // crash a random node
			if len(down) < maxDown {
				id := pick(upNodes())
				c.Crash(id)
				down[id] = true
				note("crash node %d", id)
			}
		case 2: // crash the leader of a shard
			if len(down) < maxDown {
				g := rng.Intn(cfg.Shards)
				if ls := c.LeadersOf(g); len(ls) > 0 && !down[ls[0]] {
					c.Crash(ls[0])
					down[ls[0]] = true
					note("crash node %d (leader of shard %d)", ls[0], g)
				}
			}
		case 3: // restart a crashed node
			for id := range down {
				c.Restart(id)
				delete(down, id)
				note("restart node %d", id)
				break
			}
		case 4: // random two-way partition
			ids := append([]raft.NodeID(nil), c.IDs...)
			rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
			cut := 1 + rng.Intn(len(ids)-1)
			c.Net.Heal()
			c.Net.Partition(ids[:cut], ids[cut:])
			note("partition %v | %v", ids[:cut], ids[cut:])
		case 5: // isolate a shard leader (classic stale-leader scenario)
			g := rng.Intn(cfg.Shards)
			if ls := c.LeadersOf(g); len(ls) > 0 {
				c.Net.Isolate(ls[0])
				note("isolate node %d (leader of shard %d)", ls[0], g)
			}
		case 6:
			c.Net.Heal()
			note("heal network")
		case 7: // one-way link failure
			a, b := pick(c.IDs), pick(c.IDs)
			if a != b {
				c.Net.Block(a, b)
				note("block %d -> %d", a, b)
			}
		case 8:
			p := 0.05 + rng.Float64()*0.25
			c.Net.SetDropRate(p)
			note("drop %.0f%% of messages", p*100)
		case 9:
			hi := time.Duration(1+rng.Intn(20)) * time.Millisecond
			c.Net.SetDelay(0, hi)
			note("delay messages up to %v", hi)
		case 10:
			c.Net.SetDupRate(0.2)
			note("duplicate 20%% of messages")
		}
	}
	c.Net.Heal()
	c.Net.SetDropRate(0)
	c.Net.SetDelay(0, 0)
	c.Net.SetDupRate(0)
	for id := range down {
		c.Restart(id)
	}
	note("heal everything, restart all crashed nodes")
	return timeline
}

func waitAllLeaders(c *cluster.Cluster, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for g := 0; g < c.Shards(); g++ {
			if len(c.LeadersOf(g)) == 0 {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// finalReads reads every key once the cluster is healthy; these reads are
// part of the checked history.
func finalReads(c *cluster.Cluster, cfg Config, h *history) error {
	cl := newClient(c)
	for k := 0; k < cfg.Keys; k++ {
		key := fmt.Sprintf("k%d", k)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		op := lincheck.Operation{Client: cfg.Clients, Kind: lincheck.Get, Key: key, Call: h.now()}
		v, found, err := cl.Get(ctx, key)
		op.Return = h.now()
		cancel()
		if err != nil {
			return fmt.Errorf("final read of %s failed after heal: %v", key, err)
		}
		op.Found, op.Out = found, string(v)
		h.add(op)
	}
	return nil
}

// sampler watches for two leaders in the same term of the same shard.
type sampler struct {
	stop       chan struct{}
	done       chan struct{}
	violations []string
}

func startSampler(c *cluster.Cluster) *sampler {
	s := &sampler{stop: make(chan struct{}), done: make(chan struct{})}
	type tk struct {
		shard int
		term  uint64
	}
	go func() {
		defer close(s.done)
		leaders := map[tk]raft.NodeID{}
		reported := map[tk]bool{}
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
			}
			for g := 0; g < c.Shards(); g++ {
				for _, id := range c.IDs {
					n := c.Node(id, g)
					if n == nil {
						continue
					}
					st := n.Status()
					if st.Role != raft.Leader {
						continue
					}
					k := tk{g, st.Term}
					if prev, ok := leaders[k]; ok && prev != id && !reported[k] {
						reported[k] = true
						s.violations = append(s.violations,
							fmt.Sprintf("election safety: shard %d term %d has leaders %d and %d", g, st.Term, prev, id))
					}
					leaders[k] = id
				}
			}
		}
	}()
	return s
}

func (s *sampler) finish() []string {
	close(s.stop)
	<-s.done
	return s.violations
}

// checkConvergence waits for every replica of each shard to hold the same
// state machine contents.
func checkConvergence(c *cluster.Cluster, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		last = ""
		for g := 0; g < c.Shards() && last == ""; g++ {
			var ref map[string]string
			var refID raft.NodeID
			for _, id := range c.IDs {
				srv := c.Server(id, g)
				if srv == nil {
					continue
				}
				d := srv.Store().Dump()
				if ref == nil {
					ref, refID = d, id
					continue
				}
				if !reflect.DeepEqual(ref, d) {
					last = fmt.Sprintf("convergence: shard %d replicas %d and %d differ: %v vs %v",
						g, refID, id, sortedMap(ref), sortedMap(d))
					break
				}
			}
		}
		if last == "" {
			return ""
		}
		time.Sleep(10 * time.Millisecond)
	}
	return last
}

func sortedMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + m[k]
	}
	return "{" + strings.Join(parts, " ") + "}"
}
