// casblob-convert rewrites legacy uncompressed CAS blobs (*.v1 files) in a
// bazel-remote disk cache into zstd casblobs, so compressed-blobs/zstd reads
// stream stored bytes instead of re-compressing on every read.
//
// It has three modes:
//
//   - In-place (default): convert each .v1 in the live tree directly.
//     REQUIRES bazel-remote TO BE STOPPED — the server's in-memory LRU index
//     records each entry's exact filename (including the .v1 suffix), so
//     mutating the tree under a live server breaks reads and evictions. The
//     next startup scan indexes converted casblobs natively. Corrupt .v1s
//     (hash/size mismatch) are deleted: removal turns them into misses and
//     the client's re-upload heals them.
//
//   - -shadow DIR: convert .v1s into a parallel tree under DIR, leaving the
//     live tree untouched. SAFE WHILE bazel-remote IS SERVING — the server
//     never reads DIR (it must be OUTSIDE the data dir, on the same
//     filesystem). Corrupt .v1s are logged and left alone: the running
//     server owns the live tree. Sources evicted mid-run are skipped.
//
//   - -swap DIR: consume a shadow tree written by -shadow: for each shadow
//     casblob whose source .v1 still exists, refresh its atime/mtime from
//     the source (capturing recency accrued since the shadow write), rename
//     it over the target name and delete the .v1. Shadow files whose source
//     was evicted (or whose target already exists) are discarded — never
//     resurrect an entry the server dropped. REQUIRES bazel-remote TO BE
//     STOPPED. Pure metadata ops: millions of entries swap in minutes.
//
// The intended zero-downtime rollout is: run -shadow on the serving node
// overnight (throttle with -workers/-sleep), then per node: stop the
// server, run -swap, start with storage_mode zstd. Mixed stores are fully
// supported, so every stopping point is safe and every mode is idempotent
// and resumable.
//
// Per conversion: read <hash>-<size>-<random>.v1 (raw bytes), write a zstd
// casblob to a temp file on the same filesystem (content sha256 verified
// against the filename hash by casblob.WriteAndClose), copy the source's
// owner/mode (the converter usually runs as root; the server user must be
// able to read and evict the result) and atime/mtime (so LRU recency
// survives the rescan), then atomically rename to <hash>-<size>-<random>
// (same name, no suffix). Zero-byte blobs are skipped (casblob cannot
// represent them; the server keeps serving them as legacy files).
//
// Crash safety: temp files live outside the cache tree (the startup scan
// hard-fails on unknown entries inside it) and are renamed into place only
// after a fully verified write. In-place mode, a crash between rename and
// .v1 removal leaves both files; the next run just deletes the .v1.
package main

import (
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"

	"github.com/djherbis/atime"
)

var v1Name = regexp.MustCompile(`^([a-f0-9]{64})(?:-([1-9][0-9]*))?-([0-9a-zA-Z]+)\.v1$`)

func main() {
	dir := flag.String("dir", "/mnt/bazel-cache/data", "bazel-remote cache data directory")
	tmp := flag.String("tmp", "", "in-place mode: temp dir for in-progress casblobs; must be on the same filesystem as -dir and OUTSIDE it (default: <dir>/../casblob-convert-tmp)")
	shadow := flag.String("shadow", "", "convert into this shadow tree instead of in place; safe with bazel-remote running (must be on the same filesystem as -dir and OUTSIDE it)")
	swapDir := flag.String("swap", "", "swap a previously written shadow tree into the live tree (requires bazel-remote stopped)")
	limit := flag.Int("limit", 0, "stop after processing this many objects (0 = no limit; approximate under parallelism)")
	sleep := flag.Duration("sleep", 0, "per-worker pause between objects (rate limiting)")
	workers := flag.Int("workers", 8, "parallel workers")
	minFreeGB := flag.Int("min-free-gb", 0, "shadow mode: stop dispatching when the shadow filesystem's free space drops below this many GiB (0 = disabled); converted-so-far remains valid and a later run resumes")
	dryRun := flag.Bool("dry-run", false, "list what would be processed without touching anything")
	force := flag.Bool("force", false, "skip the running-bazel-remote check (DANGEROUS)")
	flag.Parse()

	if *shadow != "" && *swapDir != "" {
		log.Fatal("-shadow and -swap are mutually exclusive")
	}
	// Only shadow mode is safe under a live server.
	if *shadow == "" && !*force && bazelRemoteRunning() {
		log.Fatal("bazel-remote is running; stop it first (only -shadow mode is safe under a live server)")
	}

	var files []string
	var process func(path string) (int64, int64, error)
	var okLabel, corruptLabel string

	switch {
	case *swapDir != "":
		var err error
		files, err = findShadowFiles(*swapDir)
		if err != nil {
			log.Fatalf("scan shadow: %v", err)
		}
		log.Printf("found %d shadow casblobs under %s", len(files), *swapDir)
		process = func(path string) (int64, int64, error) {
			return swapOne(*dir, *swapDir, path)
		}
		okLabel, corruptLabel = "swapped", "corrupt"

	default:
		inPlace := *shadow == ""
		tmpDir := *tmp
		if inPlace {
			if tmpDir == "" {
				tmpDir = filepath.Join(filepath.Dir(filepath.Clean(*dir)), "casblob-convert-tmp")
			}
		} else {
			tmpDir = filepath.Join(*shadow, ".tmp")
		}
		if !*dryRun {
			if err := os.MkdirAll(tmpDir, 0o755); err != nil {
				log.Fatalf("create temp dir: %v", err)
			}
			// Sweep partial casblobs from a previous interrupted run.
			if entries, err := os.ReadDir(tmpDir); err == nil {
				for _, e := range entries {
					_ = os.Remove(filepath.Join(tmpDir, e.Name()))
				}
			}
		}

		zstd, err := zstdimpl.Get("go")
		if err != nil {
			log.Fatalf("zstd: %v", err)
		}

		files, err = findV1Files(*dir)
		if err != nil {
			log.Fatalf("scan: %v", err)
		}
		log.Printf("found %d legacy .v1 CAS files under %s", len(files), *dir)
		process = func(path string) (int64, int64, error) {
			targetDir := filepath.Dir(path)
			if !inPlace {
				rel, err := filepath.Rel(*dir, path)
				if err != nil {
					return 0, -1, err
				}
				targetDir = filepath.Join(*shadow, filepath.Dir(rel))
			}
			return convertOne(zstd, tmpDir, path, targetDir, inPlace)
		}
		okLabel = "converted"
		corruptLabel = "corrupt-deleted"
		if !inPlace {
			corruptLabel = "corrupt-skipped"
		}
	}

	var done, skipped, corrupt, failed, bytesIn, bytesOut int64
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
	// processed count reaches it, so in-flight workers may overshoot by up
	// to -workers objects.
	paths := make(chan string, 256)
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range paths {
				in, out, err := process(path)
				switch {
				case err == nil && out < 0: // skipped or discarded
					atomic.AddInt64(&skipped, 1)
				case err == nil:
					atomic.AddInt64(&done, 1)
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
	minFree := int64(*minFreeGB) << 30
	for i, path := range files {
		if *limit > 0 && atomic.LoadInt64(&done) >= int64(*limit) {
			break
		}
		// The shadow tree grows a serving node's disk usage; never let it
		// squeeze the live cache. Everything shadowed so far stays valid —
		// swap it, and the next cycle starts with more room (casblobs are
		// never larger than their raw sources by more than a small header).
		if minFree > 0 && *shadow != "" && i%64 == 0 {
			var st syscall.Statfs_t
			if err := syscall.Statfs(*shadow, &st); err == nil {
				if free := int64(st.Bavail) * int64(st.Bsize); free < minFree {
					log.Printf("stopping early: free space %.1f GB below -min-free-gb %d", gb(free), *minFreeGB)
					break
				}
			}
		}
		paths <- path
	}
	close(paths)
	wg.Wait()

	elapsed := time.Since(start)
	log.Printf("done in %s: %s=%d skipped=%d %s=%d failed=%d",
		elapsed.Round(time.Second), okLabel, done, skipped, corruptLabel, corrupt, failed)
	if bytesIn > 0 && bytesOut > 0 && elapsed > 0 {
		log.Printf("bytes: %.1f GB raw -> %.1f GB casblob (%.2fx) at %.0f MB/s raw",
			gb(bytesIn), gb(bytesOut), float64(bytesIn)/float64(bytesOut),
			float64(bytesIn)/elapsed.Seconds()/(1<<20))
	}
	if failed > 0 {
		os.Exit(1)
	}
}

var errCorrupt = fmt.Errorf("corrupt v1 blob")

// convertOne converts a single .v1 file. When target is empty the casblob
// replaces the source next to it (in-place mode, server stopped); otherwise
// it is written to target (shadow mode, server may be live) and the source
// is left untouched. Returns (rawBytes, casblobBytes); (0, -1, nil) means
// the file was skipped.
func convertOne(zstd zstdimpl.ZstdImpl, tmpDir, path, targetDir string, inPlace bool) (int64, int64, error) {
	name := filepath.Base(path)
	m := v1Name.FindStringSubmatch(name)
	if m == nil {
		return 0, -1, fmt.Errorf("unrecognized filename %q", name)
	}
	hash, random := m[1], m[3]

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) && !inPlace {
			return 0, -1, nil // evicted by the live server since the scan
		}
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
			return 0, -1, rejectCorrupt(path, inPlace, fmt.Sprintf("size %d != stat %d", logicalSize, info.Size()))
		}
	}
	if logicalSize == 0 {
		return 0, -1, nil // casblob cannot represent empty blobs; leave as legacy
	}

	// Casblob names must carry the logical size (FileLocation:
	// <hash>-<size>-<random>); the startup scan would otherwise index the
	// entry with its on-disk (compressed) size and size-checked requests
	// would spuriously miss. Legacy .v1 names carry no size field.
	target := filepath.Join(targetDir, fmt.Sprintf("%s-%d-%s", hash, logicalSize, random))
	if _, err := os.Stat(target); err == nil {
		if inPlace {
			// Previous run crashed between rename and cleanup; finish the job.
			return 0, -1, os.Remove(path)
		}
		return 0, -1, nil // already shadowed by a previous run
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
		return 0, -1, rejectCorrupt(path, inPlace, err.Error())
	}

	// The converter typically runs as root while bazel-remote runs as its
	// own user; the casblob must be readable and evictable exactly like the
	// source .v1, so copy the source's owner and mode (CreateTemp gives
	// 0600 owned by the converter's user, which the service cannot read).
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		if err := os.Chown(tmpPath, int(st.Uid), int(st.Gid)); err != nil {
			_ = os.Remove(tmpPath)
			return 0, -1, err
		}
	}
	if err := os.Chmod(tmpPath, info.Mode().Perm()); err != nil {
		_ = os.Remove(tmpPath)
		return 0, -1, err
	}

	// Preserve recency so the post-restart rescan keeps LRU order. Shadow
	// swaps refresh this again from the source at swap time.
	_ = os.Chtimes(tmpPath, atime.Get(info), info.ModTime())

	if !inPlace {
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			_ = os.Remove(tmpPath)
			return 0, -1, err
		}
	}
	if err := os.Rename(tmpPath, target); err != nil {
		_ = os.Remove(tmpPath)
		return 0, -1, err
	}
	if inPlace {
		if err := os.Remove(path); err != nil {
			return 0, -1, err
		}
	}
	return logicalSize, sizeOnDisk, nil
}

// rejectCorrupt handles a .v1 that failed verification. In-place (server
// stopped) it is deleted: removal turns it into a miss and the client's
// re-upload heals it. In shadow mode the live server owns the tree, so the
// file is only logged and left alone.
func rejectCorrupt(path string, inPlace bool, reason string) error {
	if !inPlace {
		log.Printf("CORRUPT (%s), leaving in place: %s", reason, path)
		return errCorrupt
	}
	log.Printf("CORRUPT (%s), deleting: %s", reason, path)
	if err := os.Remove(path); err != nil {
		return err
	}
	return errCorrupt
}

var casblobName = regexp.MustCompile(`^([a-f0-9]{64})-([0-9]+)-([0-9a-zA-Z]+)$`)

// swapOne moves one shadow casblob into the live tree, replacing its source
// .v1. Returns (0, -1, nil) when the shadow file is discarded instead.
// Shadow names carry the logical size (<hash>-<size>-<random>) while legacy
// sources do not (<hash>-<random>.v1), so the source name is reassembled
// from the parsed components.
func swapOne(dataDir, shadowDir, shadowPath string) (int64, int64, error) {
	rel, err := filepath.Rel(shadowDir, shadowPath)
	if err != nil {
		return 0, -1, err
	}
	m := casblobName.FindStringSubmatch(filepath.Base(shadowPath))
	if m == nil {
		return 0, -1, fmt.Errorf("unrecognized shadow filename %q", filepath.Base(shadowPath))
	}
	targetDir := filepath.Join(dataDir, filepath.Dir(rel))
	target := filepath.Join(targetDir, filepath.Base(shadowPath))
	source := filepath.Join(targetDir, m[1]+"-"+m[3]+".v1")

	sinfo, err := os.Stat(source)
	if err != nil {
		// The server evicted (or a previous swap already consumed) the
		// source since the shadow write: never resurrect an entry the
		// server dropped.
		return 0, -1, os.Remove(shadowPath)
	}
	if _, err := os.Stat(target); err == nil {
		// Target name already occupied; keep the server's copy.
		return 0, -1, os.Remove(shadowPath)
	}

	// Carry recency accrued since the shadow write into LRU order.
	_ = os.Chtimes(shadowPath, atime.Get(sinfo), sinfo.ModTime())

	if err := os.Rename(shadowPath, target); err != nil {
		return 0, 0, err
	}
	if err := os.Remove(source); err != nil {
		return 0, 0, err
	}
	return sinfo.Size(), 0, nil
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

// findShadowFiles returns every casblob in a shadow tree, skipping the
// in-progress temp dir.
func findShadowFiles(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".tmp" {
				return filepath.SkipDir
			}
			return nil
		}
		out = append(out, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func bazelRemoteRunning() bool {
	err := exec.Command("pgrep", "-x", "bazel-remote").Run()
	return err == nil
}

func gb(b int64) float64 { return float64(b) / (1 << 30) }
