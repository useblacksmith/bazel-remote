package disk

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"sync"
	"time"
)

// The access-state file carries each entry's read history across a restart.
// Without it the boot scan only has file timestamps, and on a noatime mount
// those cannot tell a blob that was read a minute ago from one nobody ever
// asked for, so every pre-existing entry re-enters the index as
// itemStateBootScanned (read history unknown) and the tenant census reports
// its bytes as unknown until they are touched again.
//
// The file is written once, after the servers have stopped and no index
// mutation can follow, and consumed (then deleted) once, by the next boot
// scan. It is advisory only: a missing, truncated or stale file leaves the
// scan on file timestamps for the entries it cannot vouch for.
//
// Layout (little endian): magic, uint32 written-at unix seconds, uint64
// record count, then one record per entry: 16-byte FNV-1a hash of the
// lookup key, uint32 lastAccess, uint8 access state. Only entries whose
// state is known (itemStateNew or itemStateTouched) are written; a
// boot-scanned entry that was never touched would come back as boot-scanned
// anyway.
const (
	accessStateFileName = "access-state.v1"
	accessStateTempName = accessStateFileName + ".tmp"
	accessStateMagic    = "BRAS\x01"
	accessStateHdrLen   = len(accessStateMagic) + 4 + 8
	accessStateRecLen   = 16 + 4 + 1
)

type accessStateKey [16]byte

type accessStateRecord struct {
	lastAccess uint32
	state      uint8
}

// accessStateIndex is the consumed file, held only for the duration of the
// boot scan.
type accessStateIndex struct {
	writtenAt uint32
	records   map[accessStateKey]accessStateRecord
}

func accessStateKeyFor(lookupKey string) accessStateKey {
	h := fnv.New128a()
	_, _ = io.WriteString(h, lookupKey)
	var k accessStateKey
	h.Sum(k[:0])
	return k
}

// lookup returns the persisted state for an entry, or false when the file
// has nothing trustworthy for it: no record, or the blob was created after
// the file was written (a blob's mtime is its creation time; the record
// cannot be about this file).
func (idx *accessStateIndex) lookup(lookupKey string, addedAt uint32) (accessStateRecord, bool) {
	if idx == nil || addedAt > idx.writtenAt {
		return accessStateRecord{}, false
	}
	rec, ok := idx.records[accessStateKeyFor(lookupKey)]
	if !ok {
		return accessStateRecord{}, false
	}
	if rec.lastAccess < addedAt {
		rec.lastAccess = addedAt
	}
	return rec, true
}

// SaveAccessState writes the index's read history to the cache dir so the
// next boot scan can restore it. Call it only once no request can mutate
// the index any more (after the servers have stopped): it walks the whole
// index under the cache mutex. Hashing the keys is the dominant cost, so
// the walk hands batches of entries to one encoder per CPU and a single
// writer appends their output; record order is irrelevant to the reader.
func (c *diskCache) SaveAccessState() error {
	start := time.Now()
	tmp := path.Join(c.dir, accessStateTempName)
	final := path.Join(c.dir, accessStateFileName)

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	const batchSize = 1 << 14
	batches := make(chan []*entry, runtime.NumCPU()*2)
	encoded := make(chan []byte, runtime.NumCPU()*2)

	var encoders sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		encoders.Add(1)
		go func() {
			defer encoders.Done()
			for batch := range batches {
				buf := make([]byte, 0, len(batch)*accessStateRecLen)
				for _, kv := range batch {
					k := accessStateKeyFor(kv.key)
					buf = append(buf, k[:]...)
					buf = binary.LittleEndian.AppendUint32(buf, kv.value.lastAccess)
					buf = append(buf, kv.value.accessState)
				}
				encoded <- buf
			}
		}()
	}

	var writeErr error
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		w := bufio.NewWriterSize(f, 4<<20)
		// Header first; the record count is patched in once known.
		hdr := make([]byte, accessStateHdrLen)
		copy(hdr, accessStateMagic)
		_, writeErr = w.Write(hdr)
		for buf := range encoded {
			if writeErr == nil {
				_, writeErr = w.Write(buf)
			}
		}
		if writeErr == nil {
			writeErr = w.Flush()
		}
	}()

	c.mu.Lock()
	total := c.lru.Len()
	writtenAt := c.lru.nowFn()
	var count uint64
	batch := make([]*entry, 0, batchSize)
	for e := c.lru.ll.Front(); e != nil; e = e.Next() {
		kv := e.Value.(*entry)
		if kv.value.accessState == itemStateBootScanned {
			continue
		}
		batch = append(batch, kv)
		count++
		if len(batch) == batchSize {
			batches <- batch
			batch = make([]*entry, 0, batchSize)
		}
	}
	if len(batch) > 0 {
		batches <- batch
	}
	close(batches)
	encoders.Wait()
	close(encoded)
	<-writerDone
	c.mu.Unlock()

	err = writeErr
	if err == nil {
		hdr := make([]byte, accessStateHdrLen)
		copy(hdr, accessStateMagic)
		binary.LittleEndian.PutUint32(hdr[len(accessStateMagic):], writtenAt)
		binary.LittleEndian.PutUint64(hdr[len(accessStateMagic)+4:], count)
		_, err = f.WriteAt(hdr, 0)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, final)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("failed to save access state: %w", err)
	}
	log.Printf("Saved access state for %d of %d index entries in %s", count, total, time.Since(start).Round(time.Millisecond))
	return nil
}

// consumeAccessState reads and deletes the access-state file left by the
// previous process, if any. Errors are logged, not returned: the file is
// advisory and the boot scan proceeds on file timestamps without it.
func consumeAccessState(dir string) *accessStateIndex {
	_ = os.Remove(path.Join(dir, accessStateTempName))
	final := path.Join(dir, accessStateFileName)
	f, err := os.Open(final)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("Ignoring access state file: %v", err)
		}
		return nil
	}
	start := time.Now()
	idx, err := readAccessState(f)
	_ = f.Close()
	// Consumed exactly once: a later crash must not replay a stale file.
	if rmErr := os.Remove(final); rmErr != nil {
		log.Printf("Failed to remove consumed access state file: %v", rmErr)
	}
	if err != nil {
		log.Printf("Ignoring access state file: %v", err)
		return nil
	}
	log.Printf("Loaded access state for %d entries (written %s) in %s",
		len(idx.records), time.Unix(int64(idx.writtenAt), 0).UTC().Format(time.RFC3339),
		time.Since(start).Round(time.Millisecond))
	return idx
}

func readAccessState(r io.Reader) (*accessStateIndex, error) {
	br := bufio.NewReaderSize(r, 4<<20)
	hdr := make([]byte, accessStateHdrLen)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return nil, fmt.Errorf("short header: %w", err)
	}
	if string(hdr[:len(accessStateMagic)]) != accessStateMagic {
		return nil, errors.New("bad magic")
	}
	idx := &accessStateIndex{
		writtenAt: binary.LittleEndian.Uint32(hdr[len(accessStateMagic):]),
	}
	count := binary.LittleEndian.Uint64(hdr[len(accessStateMagic)+4:])
	if count > 1<<32 {
		return nil, fmt.Errorf("implausible record count %d", count)
	}
	idx.records = make(map[accessStateKey]accessStateRecord, int(count))
	rec := make([]byte, accessStateRecLen)
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(br, rec); err != nil {
			return nil, fmt.Errorf("truncated after %d of %d records: %w", i, count, err)
		}
		state := rec[20]
		if state != itemStateNew && state != itemStateTouched {
			return nil, fmt.Errorf("record %d has unknown state %d", i, state)
		}
		var k accessStateKey
		copy(k[:], rec[:16])
		idx.records[k] = accessStateRecord{
			lastAccess: binary.LittleEndian.Uint32(rec[16:]),
			state:      state,
		}
	}
	return idx, nil
}
