package cache

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// LRUArtifactSchemaVersion is stamped into every LRU observation artifact
// header. Bump it on any breaking change to the on-disk JSONL format so
// consumers can detect and skip incompatible artifacts.
const LRUArtifactSchemaVersion = 1

// LRUArtifactKeyInfix is the sub-prefix, under a cache entry's storage
// prefix, where LRU observation artifacts are written. The retention sweep
// MUST exclude this sub-prefix from its "delete everything else" pass.
const LRUArtifactKeyInfix = "lru/"

// lruArtifactKeyTimestampWidth zero-pads the window-end epoch-ms in object
// keys so a lexical (S3) listing sorts chronologically. 20 digits covers
// epoch-ms well beyond any realistic retention horizon.
const lruArtifactKeyTimestampWidth = 20

// LRUObject is a single AC or CAS object reference together with its on-disk
// (stored/compressed) byte size. SizeOnDisk uses the same unit as the
// footprint accumulator and the MinIO drift scan, never the logical size.
type LRUObject struct {
	Hash       string `json:"h"`
	SizeOnDisk int64  `json:"s"`
}

// ACClosure is one Action Cache entry together with the complete CAS leaf
// closure observed at an access (output files, expanded tree files, and
// stdout/stderr). Producers emit a closure only when every leaf size is
// resolved (complete-or-drop), so consumers never observe a partial closure.
type ACClosure struct {
	AC       LRUObject   `json:"ac"`
	Leaves   []LRUObject `json:"leaves"`
	TSMillis int64       `json:"ts_ms"`
}

// LRUObserver receives best-effort AC-access observations used to build LRU
// retention artifacts. Implementations MUST NOT affect cache request
// behavior and MUST tolerate concurrent calls.
type LRUObserver interface {
	RecordACAccess(ctx context.Context, closure ACClosure)
}

// ObserveACAccess forwards an AC-access observation when an observer is
// configured. A nil observer is a no-op, and panics from the observer are
// swallowed, so observation can never change cache request behavior. This
// mirrors ObserveOperation.
func ObserveACAccess(ctx context.Context, observer LRUObserver, closure ACClosure) {
	if observer == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	observer.RecordACAccess(ctx, closure)
}

// LRUArtifactHeader is the first JSONL line of an LRU observation artifact.
// It carries provenance only; the object key (not the header) encodes the
// generation via the storage prefix.
type LRUArtifactHeader struct {
	SchemaVersion int    `json:"schema_version"`
	Generation    string `json:"generation"`
	InstanceName  string `json:"instance_name"`
	Host          string `json:"host"`
	ProcessID     string `json:"process_id"`
	WindowStartMs int64  `json:"window_start_ms"`
	WindowEndMs   int64  `json:"window_end_ms"`
	EntryCount    int    `json:"entry_count"`
}

// LRUArtifactKey returns the object key for an LRU artifact under the given
// storage prefix. storagePrefix already encodes the generation and ends in a
// trailing slash (e.g. "bazelre/prod/42/987/v7/"); the result is
// "<storagePrefix>lru/<zero-padded windowEndMs>-<uniq>.jsonl". uniq is a
// caller-supplied host/process discriminator that prevents collisions when
// multiple processes flush the same window.
func LRUArtifactKey(storagePrefix string, windowEndMs int64, uniq string) string {
	return fmt.Sprintf("%s%s%0*d-%s.jsonl",
		storagePrefix, LRUArtifactKeyInfix, lruArtifactKeyTimestampWidth, windowEndMs, uniq)
}

// WriteLRUArtifact serializes an LRU artifact as JSONL to w: the header on
// the first line, then one ACClosure per line in the given order. Callers
// should set header.EntryCount to len(closures).
func WriteLRUArtifact(w io.Writer, header LRUArtifactHeader, closures []ACClosure) error {
	bw := bufio.NewWriter(w)
	if err := writeJSONLine(bw, header); err != nil {
		return err
	}
	for i := range closures {
		if err := writeJSONLine(bw, closures[i]); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func writeJSONLine(w io.Writer, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	_, err = w.Write([]byte{'\n'})
	return err
}

// ReadLRUArtifact parses a JSONL artifact written by WriteLRUArtifact. It is
// provided for Go consumers and tests; the header must be the first line.
func ReadLRUArtifact(r io.Reader) (LRUArtifactHeader, []ACClosure, error) {
	var header LRUArtifactHeader

	sc := bufio.NewScanner(r)
	// AC closures with many leaves can produce long lines; allow up to 16MiB.
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return header, nil, err
		}
		return header, nil, fmt.Errorf("lru artifact: empty input")
	}
	if err := json.Unmarshal(sc.Bytes(), &header); err != nil {
		return header, nil, fmt.Errorf("lru artifact: bad header: %w", err)
	}

	var closures []ACClosure
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var c ACClosure
		if err := json.Unmarshal(line, &c); err != nil {
			return header, nil, fmt.Errorf("lru artifact: bad closure line: %w", err)
		}
		closures = append(closures, c)
	}
	if err := sc.Err(); err != nil {
		return header, nil, err
	}
	return header, closures, nil
}
