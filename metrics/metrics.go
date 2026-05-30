// Package metrics exports node health and performance to Prometheus.
package metrics

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Jenil133/raftkv/kv"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
)

// Registry holds every metric a node exports.
type Registry struct {
	reg      *prometheus.Registry
	requests *prometheus.HistogramVec
	fsync    *prometheus.HistogramVec
	raft     *raftCollector
}

// New creates a registry with Go runtime and process metrics included.
func New(node raft.NodeID) *Registry {
	r := &Registry{
		reg: prometheus.NewRegistry(),
		requests: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "raftkv",
			Name:      "request_duration_seconds",
			Help:      "Client request latency at the serving node, by shard, operation and outcome.",
			Buckets:   []float64{.0005, .001, .002, .004, .008, .016, .032, .064, .128, .256, .512, 1, 2},
		}, []string{"shard", "op", "status"}),
		fsync: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "raftkv",
			Name:      "wal_fsync_duration_seconds",
			Help:      "Time spent in fsync on the write-ahead log.",
			Buckets:   []float64{.0001, .0005, .001, .002, .004, .008, .016, .032, .064, .128},
		}, []string{"shard"}),
		raft: &raftCollector{node: strconv.FormatUint(uint64(node), 10)},
	}
	r.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		r.requests, r.fsync, r.raft,
	)
	return r
}

// KVObserver returns a kv.Observer that records request latency for shard.
func (r *Registry) KVObserver(shard int) kv.Observer {
	label := strconv.Itoa(shard)
	return func(op string, st kvpb.Status, d time.Duration) {
		r.requests.WithLabelValues(label, op, st.String()).Observe(d.Seconds())
	}
}

// FsyncObserver returns a callback recording WAL fsync latency for shard.
func (r *Registry) FsyncObserver(shard int) func(time.Duration) {
	h := r.fsync.WithLabelValues(strconv.Itoa(shard))
	return func(d time.Duration) { h.Observe(d.Seconds()) }
}

// AddRaft exports the state of a shard's Raft node, read at scrape time.
func (r *Registry) AddRaft(shard int, n *raft.Node) {
	r.raft.add(strconv.Itoa(shard), n)
}

// Handler serves the Prometheus exposition format.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{Registry: r.reg})
}

// Gatherer exposes the underlying registry (for tests).
func (r *Registry) Gatherer() prometheus.Gatherer { return r.reg }

// raftCollector reads Status and Stats from each Raft node on every scrape,
// so the consensus code needs no instrumentation of its own.
type raftCollector struct {
	node  string
	mu    sync.Mutex
	nodes []shardNode
}

type shardNode struct {
	shard string
	n     *raft.Node
}

func (c *raftCollector) add(shard string, n *raft.Node) {
	c.mu.Lock()
	c.nodes = append(c.nodes, shardNode{shard, n})
	c.mu.Unlock()
}

var (
	labels = []string{"node", "shard"}

	descTerm        = prometheus.NewDesc("raftkv_raft_term", "Current Raft term.", labels, nil)
	descRole        = prometheus.NewDesc("raftkv_raft_role", "Raft role: 0 follower, 1 candidate, 2 leader.", labels, nil)
	descIsLeader    = prometheus.NewDesc("raftkv_raft_is_leader", "1 if this node leads the shard.", labels, nil)
	descLeader      = prometheus.NewDesc("raftkv_raft_leader_id", "Node id this node believes leads the shard (0 = unknown).", labels, nil)
	descCommit      = prometheus.NewDesc("raftkv_raft_commit_index", "Highest log index known committed.", labels, nil)
	descApplied     = prometheus.NewDesc("raftkv_raft_applied_index", "Highest log index handed to the state machine.", labels, nil)
	descLast        = prometheus.NewDesc("raftkv_raft_last_index", "Index of the last log entry.", labels, nil)
	descSnapIndex   = prometheus.NewDesc("raftkv_raft_snapshot_index", "Last index covered by the latest snapshot.", labels, nil)
	descLogLen      = prometheus.NewDesc("raftkv_raft_log_entries", "Entries held in the log after compaction.", labels, nil)
	descElections   = prometheus.NewDesc("raftkv_raft_elections_total", "Elections started by this node.", labels, nil)
	descLeaderTerms = prometheus.NewDesc("raftkv_raft_leader_terms_total", "Terms in which this node became leader.", labels, nil)
	descSnapTaken   = prometheus.NewDesc("raftkv_raft_snapshots_taken_total", "Snapshots taken locally.", labels, nil)
	descSnapSent    = prometheus.NewDesc("raftkv_raft_snapshots_sent_total", "Snapshots sent to lagging followers.", labels, nil)
	descSnapInst    = prometheus.NewDesc("raftkv_raft_snapshots_installed_total", "Snapshots received from a leader and installed.", labels, nil)
)

func (c *raftCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descTerm, descRole, descIsLeader, descLeader, descCommit, descApplied, descLast,
		descSnapIndex, descLogLen, descElections, descLeaderTerms, descSnapTaken, descSnapSent, descSnapInst,
	} {
		ch <- d
	}
}

func (c *raftCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	nodes := append([]shardNode(nil), c.nodes...)
	c.mu.Unlock()
	for _, sn := range nodes {
		st, stats := sn.n.Status(), sn.n.Stats()
		g := func(d *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, c.node, sn.shard)
		}
		k := func(d *prometheus.Desc, v uint64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), c.node, sn.shard)
		}
		isLeader := 0.0
		if st.Role == raft.Leader {
			isLeader = 1
		}
		g(descTerm, float64(st.Term))
		g(descRole, float64(st.Role))
		g(descIsLeader, isLeader)
		g(descLeader, float64(st.Leader))
		g(descCommit, float64(st.CommitIndex))
		g(descApplied, float64(st.LastApplied))
		g(descLast, float64(st.LastIndex))
		g(descSnapIndex, float64(st.SnapshotIndex))
		g(descLogLen, float64(st.LogLength))
		k(descElections, stats.Elections)
		k(descLeaderTerms, stats.TermsAsLeader)
		k(descSnapTaken, stats.SnapshotsTaken)
		k(descSnapSent, stats.SnapshotsSent)
		k(descSnapInst, stats.SnapshotsInstalled)
	}
}
