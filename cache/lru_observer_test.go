package cache

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func sampleHeader() LRUArtifactHeader {
	return LRUArtifactHeader{
		SchemaVersion: LRUArtifactSchemaVersion,
		Generation:    "v7",
		InstanceName:  "bazel/prod/42/987",
		Host:          "host-1",
		ProcessID:     "proc-abc",
		WindowStartMs: 1719000000000,
		WindowEndMs:   1719000060000,
		EntryCount:    1,
	}
}

func sampleClosures() []ACClosure {
	return []ACClosure{
		{
			AC: LRUObject{Hash: "ac-hash", SizeOnDisk: 12345},
			Leaves: []LRUObject{
				{Hash: "leaf-1", SizeOnDisk: 678},
				{Hash: "leaf-2", SizeOnDisk: 910},
			},
			TSMillis: 1719000042000,
		},
	}
}

// TestLRUArtifactGoldenEncoding pins the exact on-disk JSONL bytes so the
// producer and any future consumer cannot silently drift from §5.2 of the
// design doc.
func TestLRUArtifactGoldenEncoding(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteLRUArtifact(&buf, sampleHeader(), sampleClosures()); err != nil {
		t.Fatalf("WriteLRUArtifact: %v", err)
	}

	want := `{"schema_version":1,"generation":"v7","instance_name":"bazel/prod/42/987","host":"host-1","process_id":"proc-abc","window_start_ms":1719000000000,"window_end_ms":1719000060000,"entry_count":1}` + "\n" +
		`{"ac":{"h":"ac-hash","s":12345},"leaves":[{"h":"leaf-1","s":678},{"h":"leaf-2","s":910}],"ts_ms":1719000042000}` + "\n"

	if got := buf.String(); got != want {
		t.Fatalf("golden mismatch:\n got: %q\nwant: %q", got, want)
	}
}

func TestLRUArtifactRoundTrip(t *testing.T) {
	header := sampleHeader()
	closures := sampleClosures()

	var buf bytes.Buffer
	if err := WriteLRUArtifact(&buf, header, closures); err != nil {
		t.Fatalf("WriteLRUArtifact: %v", err)
	}

	gotHeader, gotClosures, err := ReadLRUArtifact(&buf)
	if err != nil {
		t.Fatalf("ReadLRUArtifact: %v", err)
	}
	if !reflect.DeepEqual(gotHeader, header) {
		t.Fatalf("header round-trip mismatch:\n got: %+v\nwant: %+v", gotHeader, header)
	}
	if !reflect.DeepEqual(gotClosures, closures) {
		t.Fatalf("closures round-trip mismatch:\n got: %+v\nwant: %+v", gotClosures, closures)
	}
}

func TestReadLRUArtifactEmptyInput(t *testing.T) {
	if _, _, err := ReadLRUArtifact(strings.NewReader("")); err == nil {
		t.Fatal("expected error on empty input")
	}
}

// TestLRUArtifactKeySortsChronologically guarantees that a lexical S3 listing
// of LRU keys is in chronological (window-end) order, even across epoch-ms
// values with different digit counts.
func TestLRUArtifactKeySortsChronologically(t *testing.T) {
	const prefix = "bazelre/prod/42/987/v7/"

	early := LRUArtifactKey(prefix, 900000000000, "a") // 12-digit ms
	mid := LRUArtifactKey(prefix, 1719000000000, "a")  // 13-digit ms
	late := LRUArtifactKey(prefix, 1719000060000, "a") // 13-digit ms, later
	dupWindowB := LRUArtifactKey(prefix, 1719000000000, "b")

	keys := []string{late, mid, early, dupWindowB}
	sort.Strings(keys)

	want := []string{early, mid, dupWindowB, late}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("lexical sort not chronological:\n got: %v\nwant: %v", keys, want)
	}

	if !strings.HasPrefix(mid, prefix+LRUArtifactKeyInfix) {
		t.Fatalf("key %q missing expected prefix %q", mid, prefix+LRUArtifactKeyInfix)
	}
	if !strings.HasSuffix(mid, ".jsonl") {
		t.Fatalf("key %q missing .jsonl suffix", mid)
	}
}

type recordingObserver struct {
	got []ACClosure
}

func (r *recordingObserver) RecordACAccess(_ context.Context, c ACClosure) {
	r.got = append(r.got, c)
}

type panicObserver struct{}

func (panicObserver) RecordACAccess(context.Context, ACClosure) {
	panic("observer boom")
}

func TestObserveACAccessForwards(t *testing.T) {
	obs := &recordingObserver{}
	closure := ACClosure{AC: LRUObject{Hash: "ac", SizeOnDisk: 1}}

	ObserveACAccess(context.Background(), obs, closure)

	if len(obs.got) != 1 || obs.got[0].AC.Hash != "ac" {
		t.Fatalf("observer did not receive closure: %+v", obs.got)
	}
}

func TestObserveACAccessNilObserverIsNoop(t *testing.T) {
	// Must not panic with a nil observer.
	ObserveACAccess(context.Background(), nil, ACClosure{})
}

func TestObserveACAccessSwallowsPanic(t *testing.T) {
	// A panicking observer must not propagate into the caller.
	ObserveACAccess(context.Background(), panicObserver{}, ACClosure{AC: LRUObject{Hash: "x"}})
}
