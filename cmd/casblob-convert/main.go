// casblob-convert rewrites legacy uncompressed CAS blobs (*.v1 files) in a
// bazel-remote disk cache into zstd casblobs, so compressed-blobs/zstd reads
// stream stored bytes instead of re-compressing on every read.
//
// It is deliberately simple: a pool of workers each converting one object
// at a time, with an optional sleep between objects and an optional object
// limit per run. It is idempotent and resumable — every object is converted
// (or skipped) independently, so the program can be stopped and re-run at
// any time and it picks up whatever .v1 files remain. On a quiesced node
// run with -workers near the core count to saturate the NVMe; there is no
// reason to go slow while the server is stopped.
//
// IT MUST ONLY RUN WHILE bazel-remote IS STOPPED. The server's in-memory
// LRU index records each entry's exact filename (including the .v1 suffix);
// converting behind a running server breaks reads and evictions for the
// converted entries. On the next start, the server's startup scan indexes
// the converted casblobs natively. The program refuses to start if a
// bazel-remote process is running.
//
// Per object: read <hash>-<size>-<random>.v1 (raw bytes), write a zstd
// casblob to a temp file on the same filesystem (content sha256 verified
// against the filename hash by casblob.WriteAndClose), preserve the source
// file's atime/mtime so LRU recency survives the rescan, atomically rename
// to <hash>-<size>-<random> (same name, no suffix), then delete the .v1.
// A .v1 whose bytes fail hash verification is corrupt: it is deleted
// (removal turns it into a miss, and the client's re-upload heals it).
// Zero-byte blobs are skipped (casblob cannot represent them; the server
// keeps serving them as legacy files).
//
// Crash safety: the temp file lives outside the cache tree (the startup
// scan hard-fails on unknown entries inside it) and is renamed into place
// only after a fully verified write. A crash between rename and .v1 removal
// leaves both files; the next run just deletes the .v1.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"

	"github.com/djherbis/atime"
)

var v1Name = regexp.MustCompile(`^([a-f0-9]{64})(?:-([1-9][0-9]*))?-([0-9a-zA-Z]+)\.v1$`)

func main() {
	dir := flag.String("dir", "/mnt/bazel-cache/data", "bazel-remote cache data directory")
	tmp := flag.String("tmp", "", "temp dir for in-progress casblobs; must be on the same filesystem as -dir and OUTSIDE it (default: <dir>/../casblob-convert-tmp)")
	limit := flag.Int("limit", 0, "stop after converting this many objects (0 = no limit)")
	sleep := flag.Duration("sleep", 0, "per-worker pause between objects (rate limiting)")
	workers := flag.Int("workers", 8, "parallel conversion workers")
	dryRun := flag.Bool("dry-run", false, "list what would be converted without touching anything")
	force := flag.Bool("force", false, "skip the running-bazel-remote check (DANGEROUS)")
	flag.Parse()

	if !*force && bazelRemoteRunning() {
		log.Fatal("bazel-remote is running; stop it first (converting under a live server breaks its index)")
	}

	if *tmp == "" {
		*tmp = filepath.Join(filepath.Dir(filepath.Clean(*dir)), "casblob-convert-tmp")
	}
	if !*dryRun {
		if err := os.MkdirAll(*tmp, 0o755); err != nil {
			log.Fatalf("create temp dir: %v", err)
		}
		// Sweep partial casblobs from a previous interrupted run.
		if entries, err := os.ReadDir(*tmp); err == nil {
			for _, e := range entries {
				_ = os.Remove(filepath.Join(*tmp, e.Name()))
			}
		}
	}

	zstd, err := zstdimpl.Get("go")
	if err != nil {
		log.Fatalf("zstd: %v", err)
	}

	files, err := findV1Files(*dir)
	if err != nil {
		log.Fatalf("scan: %v", err)
	}
	log.Printf("found %d legacy .v1 CAS files under %s", len(files), *dir)

	var converted, skipped, corrupt, failed, bytesIn, bytesOut int64
	start := time.Now()

	if *dryRun {
		for i, path := range files {
			if *limit > 0 && i >= *limit {
				break
			}
			fmt.Println(path)
		}
		return
	}

	// -limit is approximate under parallelism: dispatch stops once the
	// converted count reaches it, so in-flight workers may overshoot by up
	// to -workers objects.
	paths := make(chan string, 256)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range paths {
				in, out, err := convertOne(zstd, *tmp, path)
				switch {
				case err == nil && out < 0: // skipped (zero-byte or already converted)
					atomic.AddInt64(&skipped, 1)
				case err == nil:
					atomic.AddInt64(&converted, 1)
					atomic.AddInt64(&bytesIn, in)
					atomic.AddInt64(&bytesOut, out)
				case err == errCorrupt:
					atomic.AddInt64(&corrupt, 1)
				default:
					atomic.AddInt64(&failed, 1)
					log.Printf("FAILED %s: %v", path, err)
				}
				if *sleep > 0 {
					time.Sleep(*sleep)
				}
			}
		}()
	}
	for _, path := range files {
		if *limit > 0 && atomic.LoadInt64(&converted) >= int64(*limit) {
			break
		}
		paths <- path
	}
	close(paths)
	wg.Wait()

	elapsed := time.Since(start)
	log.Printf("done in %s: converted=%d skipped=%d corrupt-deleted=%d failed=%d", elapsed.Round(time.Second), converted, skipped, corrupt, failed)
	if bytesIn > 0 && elapsed > 0 {
		log.Printf("bytes: %.1f GB raw -> %.1f GB casblob (%.2fx) at %.0f MB/s raw",
			gb(bytesIn), gb(bytesOut), float64(bytesIn)/float64(bytesOut),
			float64(bytesIn)/elapsed.Seconds()/(1<<20))
	}
	if failed > 0 {
		os.Exit(1)
	}
}

var errCorrupt = fmt.Errorf("corrupt v1 blob")

// convertOne converts a single .v1 file. Returns (rawBytes, casblobBytes).
// A (0, -1, nil) return means the file was skipped.
func convertOne(zstd zstdimpl.ZstdImpl, tmpDir string, path string) (int64, int64, error) {
	name := filepath.Base(path)
	m := v1Name.FindStringSubmatch(name)
	if m == nil {
		return 0, -1, fmt.Errorf("unrecognized filename %q", name)
	}
	hash := m[1]

	target := filepath.Join(filepath.Dir(path), name[:len(name)-len(".v1")])
	if _, err := os.Stat(target); err == nil {
		// Previous run crashed between rename and cleanup; finish the job.
		return 0, -1, os.Remove(path)
	}

	f, err := os.Open(path)
	if err != nil {
		return 0, -1, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return 0, -1, err
	}
	logicalSize := info.Size()
	if m[2] != "" {
		logicalSize, err = strconv.ParseInt(m[2], 10, 64)
		if err != nil || logicalSize != info.Size() {
			// Name/stat size disagreement means the file is truncated or
			// mangled; hash verification below would fail anyway.
			log.Printf("CORRUPT (size %d != stat %d), deleting: %s", logicalSize, info.Size(), path)
			return 0, -1, deleteCorrupt(path)
		}
	}
	if logicalSize == 0 {
		return 0, -1, nil // casblob cannot represent empty blobs; leave as legacy
	}

	tf, err := os.CreateTemp(tmpDir, "convert-*")
	if err != nil {
		return 0, -1, err
	}
	tmpPath := tf.Name()

	// WriteAndClose verifies the content sha256 against the filename hash
	// and closes tf.
	sizeOnDisk, err := casblob.WriteAndClose(zstd, f, tf, casblob.Zstandard, hash, logicalSize)
	if err != nil {
		_ = os.Remove(tmpPath)
		log.Printf("CORRUPT (%v), deleting: %s", err, path)
		return 0, -1, deleteCorrupt(path)
	}

	// Preserve recency so the post-restart rescan keeps LRU order.
	_ = os.Chtimes(tmpPath, atime.Get(info), info.ModTime())

	if err := os.Rename(tmpPath, target); err != nil {
		_ = os.Remove(tmpPath)
		return 0, -1, err
	}
	if err := os.Remove(path); err != nil {
		return 0, -1, err
	}
	return logicalSize, sizeOnDisk, nil
}

func deleteCorrupt(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return errCorrupt
}

// findV1Files returns all legacy CAS files: <dir>/cas.v2/xx/*.v1 and
// <dir>/<64-hex tenant>/cas.v2/xx/*.v1. AC/RAW entries are raw in every
// storage mode and never need conversion.
func findV1Files(dir string) ([]string, error) {
	var out []string
	for _, pattern := range []string{
		filepath.Join(dir, "cas.v2", "??", "*.v1"),
		filepath.Join(dir, "????????????????????????????????????????????????????????????????", "cas.v2", "??", "*.v1"),
	} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		out = append(out, matches...)
	}
	sort.Strings(out)
	return out, nil
}

func bazelRemoteRunning() bool {
	err := exec.Command("pgrep", "-x", "bazel-remote").Run()
	return err == nil
}

func gb(b int64) float64 { return float64(b) / (1 << 30) }
