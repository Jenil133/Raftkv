package chaos

import (
	"os"
	"strconv"
	"testing"
)

// runs returns how many scenarios to execute; RAFTKV_CHAOS_RUNS overrides.
func runs(t *testing.T, def int) int {
	if v := os.Getenv("RAFTKV_CHAOS_RUNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("bad RAFTKV_CHAOS_RUNS: %v", err)
		}
		return n
	}
	if testing.Short() {
		return 2
	}
	return def
}

func TestRandomizedFaultInjection(t *testing.T) {
	n := runs(t, 6)
	for i := 0; i < n; i++ {
		rep := Run(RandomConfig(int64(1000 + i)))
		if !rep.OK() {
			t.Fatalf("scenario failed:\n%s", rep.String())
		}
		if rep.Ops == 0 {
			t.Fatalf("no operations recorded: %s", rep.String())
		}
		t.Logf("%s", rep.String())
	}
}

// The harness must be able to fail: with reads served from arbitrary
// replicas (no consensus), the checker has to catch a stale read.
func TestHarnessDetectsStaleReads(t *testing.T) {
	for i := 0; i < 20; i++ {
		cfg := RandomConfig(int64(5000 + i))
		cfg.BuggyReads = true
		rep := Run(cfg)
		if !rep.Lin.OK && !rep.Lin.Unknown {
			t.Logf("caught after %d scenario(s):\n%s", i+1, rep.Lin.String())
			return
		}
	}
	t.Fatal("20 scenarios with unsafe replica reads all passed the linearizability check")
}
