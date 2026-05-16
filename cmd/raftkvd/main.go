// Command raftkvd runs one node of a Raft-replicated key-value store.
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jenil133/raftkv/internal/daemon"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/transport/grpctransport"
)

func main() {
	var (
		id        = flag.Uint64("id", 0, "this node's id (must appear in -peers)")
		peers     = flag.String("peers", "", "cluster members as id=host:port,id=host:port,...")
		listen    = flag.String("listen", "", "address to listen on (default: this node's address from -peers)")
		dataDir   = flag.String("data", "", "directory for the write-ahead log (default: ./data/node<id>)")
		noSync    = flag.Bool("no-sync", false, "skip fsync (unsafe; benchmarks only)")
		election  = flag.Duration("election-timeout", 300*time.Millisecond, "minimum election timeout")
		heartbeat = flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
		verbose   = flag.Bool("v", false, "debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	addrs, err := grpctransport.ParseAddrs(*peers)
	if err != nil {
		logger.Error("bad -peers", "err", err)
		os.Exit(2)
	}
	nodeID := raft.NodeID(*id)
	self, ok := addrs[nodeID]
	if !ok {
		logger.Error("-id must be one of the ids in -peers")
		os.Exit(2)
	}
	if *listen == "" {
		*listen = self
	}
	if *dataDir == "" {
		*dataDir = "data/node" + itoa(*id)
	}

	d, err := daemon.Start(daemon.Options{
		ID:                 nodeID,
		Listen:             *listen,
		Peers:              addrs,
		DataDir:            *dataDir,
		NoSync:             *noSync,
		ElectionTimeoutMin: *election,
		HeartbeatInterval:  *heartbeat,
		Logger:             logger,
	})
	if err != nil {
		logger.Error("start failed", "err", err)
		os.Exit(1)
	}
	logger.Info("node started", "id", *id, "addr", d.Addr(), "data", *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	d.Stop()
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
