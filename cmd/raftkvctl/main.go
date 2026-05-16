// Command raftkvctl is a small CLI client for a raftkvd cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Jenil133/raftkv/kv"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/transport/grpctransport"
)

const usage = `usage: raftkvctl -endpoints id=host:port,... <command> [args]

commands:
  put <key> <value>
  get <key>
  del <key>
  cas <key> <expected> <value>     set value if current value equals expected
  cas-absent <key> <value>         set value only if key does not exist
`

func main() {
	endpoints := flag.String("endpoints", "1=127.0.0.1:7001", "cluster members as id=host:port,...")
	timeout := flag.Duration("timeout", 10*time.Second, "overall request timeout")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage); flag.PrintDefaults() }
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	addrs, err := grpctransport.ParseAddrs(*endpoints)
	if err != nil {
		fatal(err)
	}
	eps := make(map[raft.NodeID]kvpb.KVClient, len(addrs))
	for id, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fatal(err)
		}
		defer conn.Close()
		eps[id] = kvpb.NewKVClient(conn)
	}
	cl := kv.NewClient(eps)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	need := func(n int) {
		if len(args) != n+1 {
			flag.Usage()
			os.Exit(2)
		}
	}
	switch args[0] {
	case "put":
		need(2)
		check(cl.Put(ctx, args[1], []byte(args[2])))
		fmt.Println("OK")
	case "get":
		need(1)
		v, ok, err := cl.Get(ctx, args[1])
		check(err)
		if !ok {
			fmt.Println("(not found)")
			os.Exit(1)
		}
		fmt.Println(string(v))
	case "del":
		need(1)
		existed, err := cl.Delete(ctx, args[1])
		check(err)
		fmt.Println("existed:", existed)
	case "cas":
		need(3)
		swapped, cur, err := cl.CAS(ctx, args[1], []byte(args[2]), false, []byte(args[3]))
		check(err)
		fmt.Printf("swapped: %v current: %q\n", swapped, cur)
	case "cas-absent":
		need(2)
		swapped, cur, err := cl.CAS(ctx, args[1], nil, true, []byte(args[2]))
		check(err)
		fmt.Printf("swapped: %v current: %q\n", swapped, cur)
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func check(err error) {
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
