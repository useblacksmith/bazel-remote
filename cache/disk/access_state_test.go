package disk

import (
	"bytes"
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

func putBlob(t *testing.T, c Cache, size int64) string {
	t.Helper()
	data, hash := testutils.RandomDataAndHash(size)
	if err := c.Put(context.Background(), cache.CAS, hash, size, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	return hash
}

func peekState(t *testing.T, c Cache, hash string) lruItem {
	t.Helper()
	dc := c.(*diskCache)
	dc.mu.Lock()
	defer dc.mu.Unlock()
	item, ok := dc.lru.Peek(cache.LookupKey(cache.CAS, hash))
	if !ok {
		t.Fatalf("%s not in index", hash)
	}
	return item
}

func TestAccessStateSurvivesRestart(t *testing.T) {
	cacheDir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	c, err := New(cacheDir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	// The boot scan trusts a record only for blobs created (file mtime)
	// no later than the file was written, so the index clock must stay
	// real time; the read is placed well after the writes.
	dc := c.(*diskCache)
	clock := uint32(time.Now().Unix())
	dc.lru.nowFn = func() uint32 { return clock }

	read := putBlob(t, c, 100)
	unread := putBlob(t, c, 100)
	clock += 1000
	readAt := clock
	rc, _, err := c.Get(context.Background(), cache.CAS, read, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()

	if err := c.SaveAccessState(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path.Join(cacheDir, accessStateFileName)); err != nil {
		t.Fatal(err)
	}

	// A blob written after the file was saved has no record and is
	// genuinely unknown to the next process.
	later := putBlob(t, c, 100)

	restarted, err := New(cacheDir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path.Join(cacheDir, accessStateFileName)); !os.IsNotExist(err) {
		t.Errorf("access state file not consumed: %v", err)
	}

	if got := peekState(t, restarted, read); got.accessState != itemStateTouched || got.lastAccess != readAt {
		t.Errorf("read blob: state %d lastAccess %d, want touched/%d", got.accessState, got.lastAccess, readAt)
	}
	if got := peekState(t, restarted, unread); got.accessState != itemStateNew {
		t.Errorf("unread blob: state %d, want new", got.accessState)
	}
	if got := peekState(t, restarted, later); got.accessState != itemStateBootScanned {
		t.Errorf("later blob: state %d, want boot-scanned", got.accessState)
	}

	// The census baseline reflects the restored states: only the later blob
	// is unknown, only the unread one is never-read.
	rdc := restarted.(*diskCache)
	rdc.census.mu.Lock()
	acc := rdc.census.tenant("")
	class := censusClassIdx(cache.CAS, 100)
	unknown := acc.residentUnknownBytesByClass[class]
	neverRead := acc.residentNeverReadBytesByClass[class]
	rdc.census.mu.Unlock()
	laterItem := peekState(t, restarted, later)
	unreadItem := peekState(t, restarted, unread)
	if unknown != laterItem.sizeOnDisk || neverRead != unreadItem.sizeOnDisk {
		t.Errorf("census unknown/never-read = %d/%d, want %d/%d", unknown, neverRead, laterItem.sizeOnDisk, unreadItem.sizeOnDisk)
	}

	// Restored recency orders the LRU: the blob read 1000 s after the
	// others were written is the most recent entry, not a timestamp tie.
	rdc.mu.Lock()
	frontKey := rdc.lru.ll.Front().Value.(*entry).key
	rdc.mu.Unlock()
	if frontKey != cache.LookupKey(cache.CAS, read) {
		t.Errorf("LRU front = %q, want the read blob", frontKey)
	}
}

func TestAccessStateIgnoresCorruptFile(t *testing.T) {
	cacheDir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	c, err := New(cacheDir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	hash := putBlob(t, c, 100)

	if err := os.WriteFile(path.Join(cacheDir, accessStateFileName), []byte("garbage"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path.Join(cacheDir, accessStateTempName), []byte("partial"), 0644); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(cacheDir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{accessStateFileName, accessStateTempName} {
		if _, err := os.Stat(path.Join(cacheDir, name)); !os.IsNotExist(err) {
			t.Errorf("%s not removed: %v", name, err)
		}
	}
	if got := peekState(t, restarted, hash); got.accessState != itemStateBootScanned {
		t.Errorf("state %d, want boot-scanned", got.accessState)
	}
}

func TestAccessStateRoundTrip(t *testing.T) {
	idx := &accessStateIndex{writtenAt: 100, records: map[accessStateKey]accessStateRecord{}}
	key := cache.LookupKey(cache.AC, testHashA)
	idx.records[accessStateKeyFor(key)] = accessStateRecord{lastAccess: 50, state: itemStateTouched}

	if _, ok := idx.lookup(key, 101); ok {
		t.Error("record applied to a blob created after the file was written")
	}
	rec, ok := idx.lookup(key, 80)
	if !ok || rec.lastAccess != 80 || rec.state != itemStateTouched {
		t.Errorf("lookup = %+v/%v, want lastAccess clamped to addedAt 80", rec, ok)
	}
	if _, ok := idx.lookup(cache.LookupKey(cache.AC, testHashB), 10); ok {
		t.Error("unexpected record for an unknown key")
	}
	var nilIdx *accessStateIndex
	if _, ok := nilIdx.lookup(key, 10); ok {
		t.Error("nil index returned a record")
	}
}

func TestAccessStateFilesNeverFailTheScan(t *testing.T) {
	cacheDir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	c, err := New(cacheDir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	putBlob(t, c, 100)
	for _, name := range []string{accessStateFileName, accessStateTempName} {
		if err := os.WriteFile(path.Join(cacheDir, name), []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	// The scan itself must tolerate the files, whether or not
	// consumeAccessState managed to delete them first.
	if _, err := c.(*diskCache).scanDir(nil); err != nil {
		t.Fatalf("scanDir with leftover access-state files: %v", err)
	}
}
