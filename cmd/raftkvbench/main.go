// Command raftkvbench measures write throughput and latency. By default it
// starts an in-process cluster of real nodes (gRPC over loopback, WAL on
// disk) and drives it with concurrent clients; -endpoints targets an
// already-running cluster instead.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Jenil133/raftkv/internal/daemon"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/shard"
	"github.com/Jenil133/raftkv/storage"
	"github.com/Jenil133/raftkv/transport/grpctransport"
)

type result struct {
	Nodes      int     `json:"nodes"`
	Shards     int     `json:"shards"`
	Clients    int     `json:"clients"`
	ValueSize  int     `json:"value_size"`
	SyncMode   string  `json:"sync_mode"`
	Seconds    float64 `json:"seconds"`
	Ops        int64   `json:"ops"`
	Errors     int64   `json:"errors"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	P50Ms      float64 `json:"p50_ms"`
	P90Ms      float64 `json:"p90_ms"`
	P99Ms      float64 `json:"p99_ms"`
	P999Ms     float64 `json:"p999_ms"`
	MaxMs      float64 `json:"max_ms"`
	GOOS       string  `json:"goos"`
	GOARCH     string  `json:"goarch"`
	CPUs       int     `json:"cpus"`
	GoVersion  string  `json:"go_version"`
	InProcess  bool    `json:"in_process_cluster"`
	Conns      int     `json:"conns_per_node"`
	WarmupSecs float64 `json:"warmup_seconds"`
}

func main() {
	var (
		nodes     = flag.Int("nodes", 3, "cluster size (in-process mode)")
		shards    = flag.Int("shards", 1, "number of Raft groups")
		clients   = flag.Int("clients", 128, "concurrent client goroutines")
		duration  = flag.Duration("duration", 10*time.Second, "measured run time")
		warmup    = flag.Duration("warmup", 2*time.Second, "unmeasured warm-up time")
		valueSize = flag.Int("value-size", 128, "bytes per value")
		keys      = flag.Int("keys", 100000, "key space size")
		syncMode  = flag.String("sync", "full", "WAL durability in in-process mode: full, fsync or none")
		endpoints = flag.String("endpoints", "", "benchmark an existing cluster (id=host:port,...) instead of starting one")
		conns     = flag.Int("conns", 4, "gRPC connections per node")
		jsonOut   = flag.String("json", "", "also write the result as JSON to this file")
	)
	flag.Parse()

	res := result{
		Nodes: *nodes, Shards: *shards, Clients: *clients, ValueSize: *valueSize, SyncMode: *syncMode,
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, CPUs: runtime.NumCPU(), GoVersion: runtime.Version(),
		Conns: *conns, WarmupSecs: warmup.Seconds(),
	}

	var addrs map[raft.NodeID]string
	if *endpoints != "" {
		var err error
		if addrs, err = grpctransport.ParseAddrs(*endpoints); err != nil {
			fatal(err)
		}
		res.Nodes, res.SyncMode = len(addrs), "external"
	} else {
		var stop func()
		addrs, stop = startCluster(*nodes, *shards, *syncMode)
		defer stop()
		res.InProcess = true
	}

	// Shared connection pool; each client gets its own session (shard.Client).
	pool := make([]map[raft.NodeID]kvpb.KVClient, *conns)
	for i := range pool {
		pool[i] = map[raft.NodeID]kvpb.KVClient{}
		for id, addr := range addrs {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fatal(err)
			}
			defer conn.Close()
			pool[i][id] = kvpb.NewKVClient(conn)
		}
	}

	waitReady(pool[0], *shards)

	var (
		measuring atomic.Bool
		stopFlag  atomic.Bool
		ops, errs atomic.Int64
		wg        sync.WaitGroup
		lats      = make([][]time.Duration, *clients)
	)
	value := make([]byte, *valueSize)
	rand.New(rand.NewSource(1)).Read(value)

	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			cl := shard.NewClient(pool[c%len(pool)], *shards)
			rng := rand.New(rand.NewSource(int64(c)))
			local := make([]time.Duration, 0, 1<<16)
			for !stopFlag.Load() {
				key := fmt.Sprintf("bench-%d", rng.Intn(*keys))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t := time.Now()
				err := cl.Put(ctx, key, value)
				d := time.Since(t)
				cancel()
				if !measuring.Load() {
					continue
				}
				if err != nil {
					errs.Add(1)
					continue
				}
				ops.Add(1)
				local = append(local, d)
			}
			lats[c] = local
		}(c)
	}

	time.Sleep(*warmup)
	measuring.Store(true)
	start := time.Now()
	time.Sleep(*duration)
	measuring.Store(false)
	elapsed := time.Since(start)
	stopFlag.Store(true)
	wg.Wait()

	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		i := int(p * float64(len(all)-1))
		return float64(all[i].Microseconds()) / 1000
	}
	res.Seconds = elapsed.Seconds()
	res.Ops = ops.Load()
	res.Errors = errs.Load()
	res.OpsPerSec = float64(res.Ops) / elapsed.Seconds()
	res.P50Ms, res.P90Ms, res.P99Ms, res.P999Ms = pct(0.50), pct(0.90), pct(0.99), pct(0.999)
	if len(all) > 0 {
		res.MaxMs = float64(all[len(all)-1].Microseconds()) / 1000
	}

	fmt.Printf("raftkvbench: %d nodes, %d shard(s), %d clients, %dB values, sync=%s, %s/%s %d CPUs, %s\n",
		res.Nodes, res.Shards, res.Clients, res.ValueSize, res.SyncMode, res.GOOS, res.GOARCH, res.CPUs, res.GoVersion)
	fmt.Printf("  writes: %d in %.1fs = %.0f ops/s (%d errors)\n", res.Ops, res.Seconds, res.OpsPerSec, res.Errors)
	fmt.Printf("  latency: p50 %.2fms  p90 %.2fms  p99 %.2fms  p99.9 %.2fms  max %.2fms\n",
		res.P50Ms, res.P90Ms, res.P99Ms, res.P999Ms, res.MaxMs)
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
			fatal(err)
		}
	}
}

func startCluster(n, shards int, syncMode string) (map[raft.NodeID]string, func()) {
	mode, noSync, err := storage.ParseSyncMode(syncMode)
	if err != nil {
		fatal(err)
	}
	dir, err := os.MkdirTemp("", "raftkvbench")
	if err != nil {
		fatal(err)
	}
	addrs := map[raft.NodeID]string{}
	lis := map[raft.NodeID]net.Listener{}
	for i := 1; i <= n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fatal(err)
		}
		lis[raft.NodeID(i)] = l
		addrs[raft.NodeID(i)] = l.Addr().String()
	}
	var ds []*daemon.Daemon
	for id, l := range lis {
		d, err := daemon.Start(daemon.Options{
			ID:            id,
			Listener:      l,
			Peers:         addrs,
			DataDir:       filepath.Join(dir, fmt.Sprintf("node%d", id)),
			Shards:        shards,
			SnapshotEvery: 50000,
			NoSync:        noSync,
			SyncMode:      mode,
		})
		if err != nil {
			fatal(err)
		}
		ds = append(ds, d)
	}
	return addrs, func() {
		for _, d := range ds {
			d.Stop()
		}
		os.RemoveAll(dir)
	}
}

// waitReady blocks until a write succeeds on every shard.
func waitReady(eps map[raft.NodeID]kvpb.KVClient, shards int) {
	cl := shard.NewClient(eps, shards)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < shards*8; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("warm-%d", i), []byte("x")); err != nil {
			fatal(fmt.Errorf("cluster not ready: %w", err))
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "raftkvbench:", err)
	os.Exit(1)
}
