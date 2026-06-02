// Command raftkvchaos runs many randomized fault-injection scenarios in
// parallel and checks each for linearizability, election safety, liveness
// and replica convergence.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jenil133/raftkv/chaos"
)

type summary struct {
	Runs       int     `json:"runs"`
	Passed     int     `json:"passed"`
	Failed     int     `json:"failed"`
	Unknown    int     `json:"checker_budget_exhausted"`
	Ops        int64   `json:"ops_checked"`
	Pending    int64   `json:"ops_indeterminate"`
	Faults     int64   `json:"faults_injected"`
	BaseSeed   int64   `json:"base_seed"`
	WallSec    float64 `json:"wall_seconds"`
	FailedSeed []int64 `json:"failed_seeds,omitempty"`
}

func main() {
	var (
		runs      = flag.Int("runs", 100, "number of scenarios to run")
		parallel  = flag.Int("parallel", max(1, runtime.NumCPU()/2), "scenarios to run at once")
		seed      = flag.Int64("seed", time.Now().UnixNano()%1_000_000_000, "base seed; run i uses seed+i")
		only      = flag.Int64("only", -1, "run just this one seed (to reproduce a failure)")
		keepGoing = flag.Bool("keep-going", false, "keep running after a failure")
		outDir    = flag.String("out", "chaos-results", "directory for failure reports and the summary")
		verbose   = flag.Bool("v", false, "print every run")
	)
	flag.Parse()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	seeds := make([]int64, 0, *runs)
	if *only >= 0 {
		seeds = append(seeds, *only)
	} else {
		for i := 0; i < *runs; i++ {
			seeds = append(seeds, *seed+int64(i))
		}
	}

	var (
		sum      = summary{BaseSeed: *seed}
		mu       sync.Mutex
		next     atomic.Int64
		stop     atomic.Bool
		finished atomic.Int64
		start    = time.Now()
	)
	work := func() {
		for !stop.Load() {
			i := next.Add(1) - 1
			if int(i) >= len(seeds) {
				return
			}
			rep := chaos.Run(chaos.RandomConfig(seeds[i]))
			mu.Lock()
			sum.Runs++
			sum.Ops += int64(rep.Ops)
			sum.Pending += int64(rep.Pending)
			sum.Faults += int64(len(rep.Faults))
			if rep.Lin.Unknown {
				sum.Unknown++
			}
			if rep.OK() {
				sum.Passed++
			} else {
				sum.Failed++
				sum.FailedSeed = append(sum.FailedSeed, seeds[i])
				path := filepath.Join(*outDir, fmt.Sprintf("failure-seed-%d.txt", seeds[i]))
				os.WriteFile(path, []byte(rep.String()), 0o644)
				fmt.Printf("FAIL seed %d (report: %s)\n%s\n", seeds[i], path, firstLines(rep.String(), 30))
				if !*keepGoing {
					stop.Store(true)
				}
			}
			if *verbose {
				fmt.Print(rep.String())
			}
			mu.Unlock()
			finished.Add(1)
		}
	}

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				n := finished.Load()
				el := time.Since(start)
				eta := time.Duration(0)
				if n > 0 {
					eta = time.Duration(float64(el) / float64(n) * float64(int64(len(seeds))-n))
				}
				mu.Lock()
				fmt.Printf("progress: %d/%d runs, %d failed, %d ops checked, elapsed %v, eta %v\n",
					n, len(seeds), sum.Failed, sum.Ops, el.Round(time.Second), eta.Round(time.Second))
				mu.Unlock()
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < *parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			work()
		}()
	}
	wg.Wait()
	close(done)

	sum.WallSec = time.Since(start).Seconds()
	b, _ := json.MarshalIndent(sum, "", "  ")
	os.WriteFile(filepath.Join(*outDir, "summary.json"), b, 0o644)
	fmt.Printf("\nruns=%d passed=%d failed=%d checker-budget-exhausted=%d ops-checked=%d indeterminate-ops=%d faults=%d wall=%v\n",
		sum.Runs, sum.Passed, sum.Failed, sum.Unknown, sum.Ops, sum.Pending, sum.Faults,
		time.Duration(sum.WallSec*float64(time.Second)).Round(time.Second))
	if sum.Failed > 0 {
		os.Exit(1)
	}
}

func firstLines(s string, n int) string {
	lines := 0
	for i, c := range s {
		if c == '\n' {
			lines++
			if lines == n {
				return s[:i] + "\n..."
			}
		}
	}
	return s
}
