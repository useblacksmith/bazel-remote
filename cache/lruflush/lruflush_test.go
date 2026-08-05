package lruflush

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
)

type recordedPut struct {
	key       string
	body      []byte
	selection cache.S3BackendSelection
	hasSel    bool
}

type fakeSink struct {
	mu       sync.Mutex
	puts     []recordedPut
	failures int // fail the first N puts
}

func (s *fakeSink) PutArtifact(ctx context.Context, key string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failures > 0 {
		s.failures--
		return errors.New("injected put failure")
	}
	sel, ok := cache.S3BackendFromContext(ctx)
	s.puts = append(s.puts, recordedPut{
		key:       key,
		body:      append([]byte(nil), body...),
		selection: sel,
		hasSel:    ok,
	})
	return nil
}

func (s *fakeSink) recorded() []recordedPut {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedPut(nil), s.puts...)
}

func obsCtx(prefix string, sel *cache.S3BackendSelection) context.Context {
	ctx := context.Background()
	if prefix != "" {
		ctx = cache.WithStoragePrefix(ctx, prefix)
	}
	if sel != nil {
		ctx = cache.WithS3Backend(ctx, *sel)
	}
	return ctx
}

func closure(acHash string, ts int64, leaves ...string) cache.ACClosure {
	c := cache.ACClosure{
		AC:       cache.LRUObject{Hash: acHash, SizeOnDisk: 100},
		TSMillis: ts,
	}
	for _, leaf := range leaves {
		c.Leaves = append(c.Leaves, cache.LRUObject{Hash: leaf, SizeOnDisk: 10})
	}
	return c
}

// wideClosure returns a closure contributing exactly `objects` references
// (one AC + objects-1 distinct leaves).
func wideClosure(acHash string, ts int64, objects int) cache.ACClosure {
	c := cache.ACClosure{AC: cache.LRUObject{Hash: acHash, SizeOnDisk: 1}, TSMillis: ts}
	for j := 0; j < objects-1; j++ {
		c.Leaves = append(c.Leaves, cache.LRUObject{Hash: fmt.Sprintf("%s-leaf-%d", acHash, j), SizeOnDisk: 1})
	}
	return c
}

// newTestFlusher returns a Flusher whose periodic loop is effectively
// disabled so tests drive flushes deterministically via flush or Drain.
func newTestFlusher(sink Sink) *Flusher {
	f := New(sink, WithFlushInterval(time.Hour))
	f.Start()
	return f
}

func TestDrainFlushesBufferedClosuresInAccessOrder(t *testing.T) {
	sink := &fakeSink{}
	f := newTestFlusher(sink)
	sel := cache.S3BackendSelection{Endpoint: "http://minio-a:9000", Bucket: "tenant-a"}
	ctx := obsCtx("bazelre/prod/42/987/v7/", &sel)

	f.RecordACAccess(ctx, closure("ac-b", 2000, "leaf-1", "leaf-2"))
	f.RecordACAccess(ctx, closure("ac-a", 1000, "leaf-1"))
	// Re-access ac-b: dedups into the same entry, bumps its timestamp,
	// unions the new leaf.
	f.RecordACAccess(ctx, closure("ac-b", 3000, "leaf-3"))
	f.Drain()

	puts := sink.recorded()
	if len(puts) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(puts))
	}
	put := puts[0]
	if !strings.HasPrefix(put.key, "bazelre/prod/42/987/v7/lru/") {
		t.Fatalf("artifact key %q not under <prefix>lru/", put.key)
	}
	if !put.hasSel || put.selection != sel {
		t.Fatalf("artifact put did not carry the captured backend selection: %+v", put.selection)
	}

	header, closures, err := cache.ReadLRUArtifact(bytes.NewReader(put.body))
	if err != nil {
		t.Fatalf("artifact does not round-trip: %v", err)
	}
	if header.SchemaVersion != cache.LRUArtifactSchemaVersion {
		t.Fatalf("schema version = %d", header.SchemaVersion)
	}
	if header.EntryCount != 2 || len(closures) != 2 {
		t.Fatalf("expected 2 closures, header=%d got=%d", header.EntryCount, len(closures))
	}
	if header.WindowStartMs != 1000 {
		t.Fatalf("window start = %d, want earliest observation 1000", header.WindowStartMs)
	}
	// First-seen (access) order: ac-b was observed first.
	if closures[0].AC.Hash != "ac-b" || closures[1].AC.Hash != "ac-a" {
		t.Fatalf("closures out of access order: %s, %s", closures[0].AC.Hash, closures[1].AC.Hash)
	}
	if closures[0].TSMillis != 3000 {
		t.Fatalf("ac-b last-access = %d, want 3000", closures[0].TSMillis)
	}
	if len(closures[0].Leaves) != 3 {
		t.Fatalf("ac-b leaves = %d, want union of 3", len(closures[0].Leaves))
	}
}

func TestObservationWithoutPrefixIsDropped(t *testing.T) {
	sink := &fakeSink{}
	f := newTestFlusher(sink)
	f.RecordACAccess(obsCtx("", nil), closure("ac-a", 1000, "leaf-1"))
	f.Drain()
	if got := len(sink.recorded()); got != 0 {
		t.Fatalf("expected no artifacts for prefix-less observation, got %d", got)
	}
}

func TestOversizedClosureIsDroppedWhole(t *testing.T) {
	sink := &fakeSink{}
	f := newTestFlusher(sink)
	ctx := obsCtx("tenant/v1/", nil)

	f.RecordACAccess(ctx, wideClosure("ac-big", 1, capClosureLeaves+2))
	f.RecordACAccess(ctx, closure("ac-ok", 2, "leaf-a"))
	f.Drain()

	puts := sink.recorded()
	if len(puts) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(puts))
	}
	_, closures, err := cache.ReadLRUArtifact(bytes.NewReader(puts[0].body))
	if err != nil {
		t.Fatal(err)
	}
	if len(closures) != 1 || closures[0].AC.Hash != "ac-ok" {
		t.Fatalf("oversized closure leaked into artifact: %+v", closures)
	}
}

// TestBufferFullDropsInsteadOfFlushing pins the full-buffer policy: past
// capObjects new observations are dropped (advisory loss, self-correcting on
// the next access), never queued, never flushed early.
func TestBufferFullDropsInsteadOfFlushing(t *testing.T) {
	sink := &fakeSink{}
	f := newTestFlusher(sink)
	ctx := obsCtx("tenant/v1/", nil)

	// Fill to exactly capObjects, then one more observation must drop.
	per := 1000
	n := capObjects / per
	for i := 0; i < n; i++ {
		f.RecordACAccess(ctx, wideClosure(fmt.Sprintf("ac-%d", i), int64(i+1), per))
	}
	f.RecordACAccess(ctx, closure("ac-overflow", int64(n+1), "leaf-x"))
	// No flush may have fired: drop-not-flush under pressure.
	if got := len(sink.recorded()); got != 0 {
		t.Fatalf("full buffer triggered %d flushes, want 0", got)
	}
	f.Drain()

	puts := sink.recorded()
	if len(puts) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(puts))
	}
	_, closures, err := cache.ReadLRUArtifact(bytes.NewReader(puts[0].body))
	if err != nil {
		t.Fatal(err)
	}
	if len(closures) != n {
		t.Fatalf("closures = %d, want %d accepted before the cap", len(closures), n)
	}
	for _, cl := range closures {
		if cl.AC.Hash == "ac-overflow" {
			t.Fatal("over-cap observation leaked into the artifact")
		}
	}
}

// TestFlushClearsCapForNewObservations pins that a flush resets the global
// charge: after a window is detached and uploaded, the buffer accepts new
// observations again.
func TestFlushClearsCapForNewObservations(t *testing.T) {
	sink := &fakeSink{}
	f := newTestFlusher(sink)
	ctx := obsCtx("tenant/v1/", nil)

	per := 1000
	for i := 0; i < capObjects/per; i++ {
		f.RecordACAccess(ctx, wideClosure(fmt.Sprintf("ac-%d", i), int64(i+1), per))
	}
	f.flush(triggerPeriodic)
	f.RecordACAccess(ctx, closure("ac-after", 1, "leaf-a"))
	f.Drain()

	puts := sink.recorded()
	if len(puts) != 2 {
		t.Fatalf("expected 2 artifacts (window + post-flush), got %d", len(puts))
	}
	_, closures, err := cache.ReadLRUArtifact(bytes.NewReader(puts[1].body))
	if err != nil {
		t.Fatal(err)
	}
	if len(closures) != 1 || closures[0].AC.Hash != "ac-after" {
		t.Fatalf("post-flush observation missing: %+v", closures)
	}
}

// TestFailedUploadDropsWindow pins retry-free failure semantics: a failed PUT
// drops that window's observations and the flusher moves on.
func TestFailedUploadDropsWindow(t *testing.T) {
	sink := &fakeSink{failures: 1}
	f := newTestFlusher(sink)
	ctx := obsCtx("tenant/v1/", nil)

	f.RecordACAccess(ctx, closure("ac-a", 1, "leaf-a"))
	f.flush(triggerPeriodic)
	if got := len(sink.recorded()); got != 0 {
		t.Fatalf("failed upload produced %d artifacts, want 0", got)
	}
	// The next window is unaffected.
	f.RecordACAccess(ctx, closure("ac-b", 2, "leaf-b"))
	f.Drain()
	puts := sink.recorded()
	if len(puts) != 1 {
		t.Fatalf("expected 1 artifact after recovery, got %d", len(puts))
	}
	_, closures, err := cache.ReadLRUArtifact(bytes.NewReader(puts[0].body))
	if err != nil {
		t.Fatal(err)
	}
	if len(closures) != 1 || closures[0].AC.Hash != "ac-b" {
		t.Fatalf("recovered window content wrong: %+v", closures)
	}
}

// blockingSink parks every PutArtifact until gate closes, simulating a
// wedged MinIO.
type blockingSink struct {
	fakeSink
	gate    chan struct{}
	entered chan struct{}
}

func (s *blockingSink) PutArtifact(ctx context.Context, key string, body []byte) error {
	s.entered <- struct{}{}
	<-s.gate
	return s.fakeSink.PutArtifact(ctx, key, body)
}

// TestBlockedSinkBoundsMemoryAndNeverBlocksObservations pins the review's
// memory-bound concern in the serial design: with the flush goroutine wedged
// on a blocked backend, observations keep being accepted up to capObjects,
// are dropped past it, and RecordACAccess never blocks on the sink.
func TestBlockedSinkBoundsMemoryAndNeverBlocksObservations(t *testing.T) {
	sink := &blockingSink{
		gate:    make(chan struct{}),
		entered: make(chan struct{}, 64),
	}
	f := New(sink, WithFlushInterval(5*time.Millisecond))
	f.Start()
	ctx := obsCtx("tenant/v1/", nil)

	f.RecordACAccess(ctx, closure("ac-first", 1, "leaf-a"))
	// Wait for the flush loop to wedge inside the sink.
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flush loop never reached the sink")
	}

	// Pour in far more than the cap while the flush goroutine is stuck.
	per := 1000
	for i := 0; i < capObjects/per+10; i++ {
		f.RecordACAccess(ctx, wideClosure(fmt.Sprintf("ac-%d", i), int64(i+2), per))
	}
	f.mu.Lock()
	buffered := f.objects
	f.mu.Unlock()
	if buffered > capObjects {
		t.Fatalf("buffered objects %d exceed capObjects %d with a blocked sink", buffered, capObjects)
	}

	close(sink.gate)
	f.Drain()
}

func TestPrefixWithoutTrailingSlashNormalizes(t *testing.T) {
	sink := &fakeSink{}
	f := newTestFlusher(sink)
	f.RecordACAccess(obsCtx("tenant/v1", nil), closure("ac-a", 1, "leaf-a"))
	f.Drain()
	puts := sink.recorded()
	if len(puts) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(puts))
	}
	if !strings.HasPrefix(puts[0].key, "tenant/v1/lru/") {
		t.Fatalf("key %q not normalized to <prefix>/lru/", puts[0].key)
	}
}

func TestPeriodicTickerFlushes(t *testing.T) {
	sink := &fakeSink{}
	f := New(sink, WithFlushInterval(10*time.Millisecond))
	f.Start()
	f.RecordACAccess(obsCtx("tenant/v1/", nil), closure("ac-a", 1, "leaf-a"))

	deadline := time.After(5 * time.Second)
	for len(sink.recorded()) == 0 {
		select {
		case <-deadline:
			t.Fatal("periodic flush never fired")
		case <-time.After(5 * time.Millisecond):
		}
	}
	f.Drain()
	if got := len(sink.recorded()); got != 1 {
		t.Fatalf("expected exactly 1 artifact (drain had nothing left), got %d", got)
	}
}

func TestConcurrentObservationsAreSafe(t *testing.T) {
	sink := &fakeSink{}
	f := newTestFlusher(sink)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ctx := obsCtx(fmt.Sprintf("tenant-%d/v1/", g%2), nil)
			for i := 0; i < 500; i++ {
				f.RecordACAccess(ctx, closure(fmt.Sprintf("ac-%d-%d", g, i), int64(i+1), "leaf-a", "leaf-b"))
			}
		}(g)
	}
	wg.Wait()
	f.Drain()

	total := 0
	for _, p := range sink.recorded() {
		_, closures, err := cache.ReadLRUArtifact(bytes.NewReader(p.body))
		if err != nil {
			t.Fatal(err)
		}
		total += len(closures)
	}
	if total != 8*500 {
		t.Fatalf("closures across artifacts = %d, want %d", total, 8*500)
	}
}

func TestArtifactKeysNeverCollide(t *testing.T) {
	sink := &fakeSink{}
	f := newTestFlusher(sink)
	ctx := obsCtx("tenant/v1/", nil)
	// Two flushes of the same prefix in the same millisecond window must
	// yield distinct keys via the uniq discriminator.
	f.RecordACAccess(ctx, closure("ac-a", 1, "leaf-a"))
	f.flush(triggerPeriodic)
	f.RecordACAccess(ctx, closure("ac-b", 2, "leaf-b"))
	f.Drain()

	puts := sink.recorded()
	if len(puts) != 2 {
		t.Fatalf("expected 2 artifacts, got %d", len(puts))
	}
	if puts[0].key == puts[1].key {
		t.Fatalf("artifact keys collided: %q", puts[0].key)
	}
}
