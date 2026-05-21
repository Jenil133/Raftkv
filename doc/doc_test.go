package doc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jenil133/raftkv/doc"
	"github.com/Jenil133/raftkv/internal/cluster"
)

func newStore(t *testing.T) (*doc.Store, *cluster.Cluster) {
	t.Helper()
	c := cluster.New(t, 3, cluster.WithShards(3))
	c.WaitAllLeaders(5 * time.Second)
	return doc.NewStore(c.ShardClient()), c
}

func ctxTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func u64(v uint64) *uint64 { return &v }

func jsonEq(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("bad json %q: %v", got, err)
	}
	json.Unmarshal([]byte(want), &w)
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Fatalf("json = %s, want %s", gb, wb)
	}
}

func TestPutGetVersions(t *testing.T) {
	s, _ := newStore(t)
	ctx := ctxTimeout(t)

	d, err := s.Put(ctx, "users", "alice", []byte(`{"name":"Alice","age":30}`), nil)
	if err != nil || d.Version != 1 {
		t.Fatalf("first put: %+v %v", d, err)
	}
	d, err = s.Put(ctx, "users", "alice", []byte(`{"name":"Alice","age":31,"tags":["a","b"]}`), nil)
	if err != nil || d.Version != 2 {
		t.Fatalf("second put: %+v %v", d, err)
	}
	got, err := s.Get(ctx, "users", "alice")
	if err != nil || got.Version != 2 {
		t.Fatalf("get: %+v %v", got, err)
	}
	jsonEq(t, got.Data, `{"name":"Alice","age":31,"tags":["a","b"]}`)

	if _, err := s.Get(ctx, "users", "nobody"); !errors.Is(err, doc.ErrNotFound) {
		t.Fatalf("missing doc err = %v", err)
	}
	// Same id in another collection is a different document.
	if _, err := s.Get(ctx, "admins", "alice"); !errors.Is(err, doc.ErrNotFound) {
		t.Fatalf("collections leaked: %v", err)
	}
}

func TestSchemaLessAnyShape(t *testing.T) {
	s, _ := newStore(t)
	ctx := ctxTimeout(t)
	docs := []string{
		`{}`,
		`{"a":1}`,
		`{"nested":{"deep":{"deeper":[1,2,{"x":null}]}},"s":"str","b":true,"f":1.5}`,
		`{"big":12345678901234567890}`,
	}
	for i, body := range docs {
		id := fmt.Sprintf("d%d", i)
		if _, err := s.Put(ctx, "mixed", id, []byte(body), nil); err != nil {
			t.Fatalf("put %s: %v", body, err)
		}
		got, err := s.Get(ctx, "mixed", id)
		if err != nil {
			t.Fatal(err)
		}
		jsonEq(t, got.Data, body)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	s, _ := newStore(t)
	ctx := ctxTimeout(t)

	// Create-only succeeds once.
	if _, err := s.Put(ctx, "c", "x", []byte(`{"v":1}`), u64(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "c", "x", []byte(`{"v":2}`), u64(0)); !errors.Is(err, doc.ErrVersionConflict) {
		t.Fatalf("second create-only err = %v", err)
	}
	// Matching version applies; stale version is rejected.
	if _, err := s.Put(ctx, "c", "x", []byte(`{"v":3}`), u64(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "c", "x", []byte(`{"v":4}`), u64(1)); !errors.Is(err, doc.ErrVersionConflict) {
		t.Fatalf("stale put err = %v", err)
	}
	if _, err := s.Patch(ctx, "c", "x", []byte(`{"v":5}`), u64(1)); !errors.Is(err, doc.ErrVersionConflict) {
		t.Fatalf("stale patch err = %v", err)
	}
	if err := s.Delete(ctx, "c", "x", u64(1)); !errors.Is(err, doc.ErrVersionConflict) {
		t.Fatalf("stale delete err = %v", err)
	}
	got, _ := s.Get(ctx, "c", "x")
	if got.Version != 2 {
		t.Fatalf("conflicting writes changed the document: %+v", got)
	}
	if err := s.Delete(ctx, "c", "x", u64(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "c", "x"); !errors.Is(err, doc.ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if err := s.Delete(ctx, "c", "x", nil); !errors.Is(err, doc.ErrNotFound) {
		t.Fatalf("double delete err = %v", err)
	}
}

func TestPatchMergeSemantics(t *testing.T) {
	s, _ := newStore(t)
	ctx := ctxTimeout(t)
	s.Put(ctx, "p", "1", []byte(`{"a":1,"b":{"c":2,"d":3},"e":[1,2]}`), nil)

	d, err := s.Patch(ctx, "p", "1", []byte(`{"b":{"c":null,"x":9},"e":[7],"f":"new"}`), nil)
	if err != nil || d.Version != 2 {
		t.Fatalf("patch: %+v %v", d, err)
	}
	jsonEq(t, d.Data, `{"a":1,"b":{"d":3,"x":9},"e":[7],"f":"new"}`)

	if _, err := s.Patch(ctx, "p", "missing", []byte(`{"a":1}`), nil); !errors.Is(err, doc.ErrNotFound) {
		t.Fatalf("patch missing err = %v", err)
	}
	if _, err := s.Patch(ctx, "p", "1", []byte(`[1,2]`), nil); !errors.Is(err, doc.ErrInvalid) {
		t.Fatalf("non-object patch err = %v", err)
	}
}

func TestInvalidInput(t *testing.T) {
	s, _ := newStore(t)
	ctx := ctxTimeout(t)
	cases := []struct {
		name       string
		coll, id   string
		body       string
		shouldFail bool
	}{
		{"empty collection", "", "id", `{}`, true},
		{"slash in collection", "a/b", "id", `{}`, true},
		{"empty id", "c", "", `{}`, true},
		{"array body", "c", "id", `[1]`, true},
		{"scalar body", "c", "id", `42`, true},
		{"garbage body", "c", "id", `{oops`, true},
		{"slash in id is fine", "c", "a/b/c", `{"ok":true}`, false},
	}
	for _, tc := range cases {
		_, err := s.Put(ctx, tc.coll, tc.id, []byte(tc.body), nil)
		if tc.shouldFail && !errors.Is(err, doc.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", tc.name, err)
		}
		if !tc.shouldFail && err != nil {
			t.Errorf("%s: unexpected err %v", tc.name, err)
		}
	}
}

func TestScanPaginationAndIsolation(t *testing.T) {
	s, _ := newStore(t)
	ctx := ctxTimeout(t)
	for i := 0; i < 55; i++ {
		if _, err := s.Put(ctx, "list", fmt.Sprintf("%03d", i), []byte(fmt.Sprintf(`{"n":%d}`, i)), nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		s.Put(ctx, "listing", fmt.Sprintf("%03d", i), []byte(`{}`), nil) // shares a name prefix
		s.Put(ctx, "other", fmt.Sprintf("%03d", i), []byte(`{}`), nil)
	}

	var ids []string
	after := ""
	for {
		page, err := s.Scan(ctx, "list", after, 20)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, d := range page {
			if d.Collection != "list" || d.Version != 1 {
				t.Fatalf("bad doc in scan: %+v", d)
			}
			ids = append(ids, d.ID)
		}
		after = page[len(page)-1].ID
	}
	if len(ids) != 55 {
		t.Fatalf("scanned %d docs, want 55", len(ids))
	}
	for i, id := range ids {
		if id != fmt.Sprintf("%03d", i) {
			t.Fatalf("ids[%d] = %s: not in order", i, id)
		}
	}
}

func TestConcurrentPatchesToDifferentFieldsAllLand(t *testing.T) {
	s0, c := newStore(t)
	ctx := ctxTimeout(t)
	s0.Put(ctx, "race", "doc", []byte(`{}`), nil)

	const writers = 12
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := doc.NewStore(c.ShardClient()) // independent client per writer
			_, err := s.Patch(ctx, "race", "doc", []byte(fmt.Sprintf(`{"f%d":%d}`, i, i)), nil)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := s0.Get(ctx, "race", "doc")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]float64
	json.Unmarshal(got.Data, &m)
	if len(m) != writers {
		t.Fatalf("lost updates: %d of %d fields present: %s", len(m), writers, got.Data)
	}
	if got.Version != writers+1 {
		t.Fatalf("version = %d, want %d", got.Version, writers+1)
	}
}

func TestConcurrentCreateOnlyHasOneWinner(t *testing.T) {
	_, c := newStore(t)
	ctx := ctxTimeout(t)
	const racers = 10
	var wg sync.WaitGroup
	results := make(chan error, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := doc.NewStore(c.ShardClient())
			_, err := s.Put(ctx, "once", "k", []byte(fmt.Sprintf(`{"winner":%d}`, i)), u64(0))
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	wins, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, doc.ErrVersionConflict):
			conflicts++
		default:
			t.Fatal(err)
		}
	}
	if wins != 1 || conflicts != racers-1 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
}

func TestDocsSurviveNodeFailureAndRestart(t *testing.T) {
	s, c := newStore(t)
	ctx := ctxTimeout(t)
	for i := 0; i < 30; i++ {
		s.Put(ctx, "dur", fmt.Sprintf("%d", i), []byte(fmt.Sprintf(`{"i":%d}`, i)), nil)
	}
	c.Crash(c.IDs[0])
	if _, err := s.Patch(ctx, "dur", "5", []byte(`{"patched":true}`), nil); err != nil {
		t.Fatalf("patch with a node down: %v", err)
	}
	c.Restart(c.IDs[0])
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}
	c.WaitAllLeaders(5 * time.Second)
	d, err := s.Get(ctx, "dur", "5")
	if err != nil {
		t.Fatal(err)
	}
	jsonEq(t, d.Data, `{"i":5,"patched":true}`)
	if d.Version != 2 {
		t.Fatalf("version = %d", d.Version)
	}
}
