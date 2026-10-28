package server

import (
	"bytes"
	"context"
	"io"
	stdlog "log"
	"os"
	"sync"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	trustScopeMainPrefix = "bazel/staging/us-west/42/9876/v0/turbo/main"
	trustScopePRPrefix   = "bazel/staging/us-west/42/9876/v0/turbo/pr"
)

func trustScopeServer(t *testing.T, depsCheck bool) (*grpcServer, disk.Cache) {
	t.Helper()
	dir, err := os.MkdirTemp("", "bazel-remote-trust-scope-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	diskCache, err := disk.New(dir, 10*maxChunkSize*2, disk.WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	discard := stdlog.New(io.Discard, "", 0)
	return &grpcServer{
		cache:               diskCache,
		accessLogger:        discard,
		errorLogger:         discard,
		depsCheck:           depsCheck,
		maxCasBlobSizeBytes: 1 << 40,
		readChunkSizeBytes:  maxChunkSize,
	}, diskCache
}

func mainCtx() context.Context {
	return cache.WithStoragePrefix(context.Background(), trustScopeMainPrefix)
}

func prCtx() context.Context {
	return cache.WithFallbackStoragePrefixes(
		cache.WithStoragePrefix(context.Background(), trustScopePRPrefix),
		[]string{trustScopeMainPrefix})
}

type trustScopeReadServer struct {
	bytestream.ByteStream_ReadServer
	ctx  context.Context
	data bytes.Buffer
}

func (s *trustScopeReadServer) Context() context.Context { return s.ctx }
func (s *trustScopeReadServer) Send(r *bytestream.ReadResponse) error {
	_, err := s.data.Write(r.Data)
	return err
}

func putCAS(t *testing.T, c disk.Cache, ctx context.Context, size int) *pb.Digest {
	t.Helper()
	data, hash := testutils.RandomDataAndHash(int64(size))
	if err := c.Put(ctx, cache.CAS, hash, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	return &pb.Digest{Hash: hash, SizeBytes: int64(len(data))}
}

func putAC(t *testing.T, s *grpcServer, ctx context.Context, exitCode int32) (*pb.Digest, error) {
	t.Helper()
	_, hash := testutils.RandomDataAndHash(64)
	digest := &pb.Digest{Hash: hash, SizeBytes: 64}
	_, err := s.UpdateActionResult(ctx, &pb.UpdateActionResultRequest{
		ActionDigest: digest,
		ActionResult: &pb.ActionResult{ExitCode: exitCode},
	})
	return digest, err
}

func TestTrustScopeReadFallback(t *testing.T) {
	for _, depsCheck := range []bool{false, true} {
		t.Run(map[bool]string{false: "nodepscheck", true: "depscheck"}[depsCheck], func(t *testing.T) {
			s, diskCache := trustScopeServer(t, depsCheck)

			mainAC, err := putAC(t, s, mainCtx(), 7)
			if err != nil {
				t.Fatal(err)
			}
			prAC, err := putAC(t, s, prCtx(), 9)
			if err != nil {
				t.Fatal(err)
			}

			// pr reads pr, then main.
			got, err := s.GetActionResult(prCtx(), &pb.GetActionResultRequest{ActionDigest: mainAC})
			if err != nil || got.ExitCode != 7 {
				t.Fatalf("pr scope should fall back to main AC: result=%v err=%v", got, err)
			}
			got, err = s.GetActionResult(prCtx(), &pb.GetActionResultRequest{ActionDigest: prAC})
			if err != nil || got.ExitCode != 9 {
				t.Fatalf("pr scope should read its own AC: result=%v err=%v", got, err)
			}

			// main never reads pr: the write under pr did not fall back either.
			_, err = s.GetActionResult(mainCtx(), &pb.GetActionResultRequest{ActionDigest: prAC})
			if status.Code(err) != codes.NotFound {
				t.Fatalf("main scope must not see pr AC: %v", err)
			}

			mainBlob := putCAS(t, diskCache, mainCtx(), 1024)
			prBlob := putCAS(t, diskCache, prCtx(), 2048)

			missing, err := s.FindMissingBlobs(prCtx(), &pb.FindMissingBlobsRequest{BlobDigests: []*pb.Digest{mainBlob, prBlob}})
			if err != nil || len(missing.MissingBlobDigests) != 0 {
				t.Fatalf("pr scope should find both blobs: missing=%v err=%v", missing, err)
			}
			missing, err = s.FindMissingBlobs(mainCtx(), &pb.FindMissingBlobsRequest{BlobDigests: []*pb.Digest{mainBlob, prBlob}})
			if err != nil || len(missing.MissingBlobDigests) != 1 || missing.MissingBlobDigests[0].Hash != prBlob.Hash {
				t.Fatalf("main scope should miss only the pr blob: missing=%v err=%v", missing, err)
			}

			batch, err := s.BatchReadBlobs(prCtx(), &pb.BatchReadBlobsRequest{Digests: []*pb.Digest{mainBlob, prBlob}})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range batch.Responses {
				if r.Status.Code != int32(code.Code_OK) || int64(len(r.Data)) != r.Digest.SizeBytes {
					t.Fatalf("pr scope batch read should hit %s: %v", r.Digest.Hash, r.Status)
				}
			}
			batch, err = s.BatchReadBlobs(mainCtx(), &pb.BatchReadBlobsRequest{Digests: []*pb.Digest{prBlob}})
			if err != nil || batch.Responses[0].Status.Code != int32(code.Code_NOT_FOUND) {
				t.Fatalf("main scope batch read must miss the pr blob: %v err=%v", batch, err)
			}

			rs := &trustScopeReadServer{ctx: prCtx()}
			if err := s.Read(&bytestream.ReadRequest{ResourceName: "blobs/" + mainBlob.Hash + "/1024"}, rs); err != nil {
				t.Fatalf("pr scope bytestream read should fall back to main: %v", err)
			}
			if int64(rs.data.Len()) != mainBlob.SizeBytes {
				t.Fatalf("short read: %d", rs.data.Len())
			}
			rs = &trustScopeReadServer{ctx: mainCtx()}
			err = s.Read(&bytestream.ReadRequest{ResourceName: "blobs/" + prBlob.Hash + "/2048"}, rs)
			if status.Code(err) != codes.NotFound {
				t.Fatalf("main scope bytestream read must miss the pr blob: %v", err)
			}
		})
	}
}

func TestTrustScopeACImmutable(t *testing.T) {
	s, _ := trustScopeServer(t, true)

	digest, err := putAC(t, s, cache.WithACImmutable(mainCtx()), 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateActionResult(cache.WithACImmutable(mainCtx()), &pb.UpdateActionResultRequest{
		ActionDigest: digest,
		ActionResult: &pb.ActionResult{ExitCode: 2},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("immutable overwrite should be ALREADY_EXISTS, got %v", err)
	}
	got, err := s.GetActionResult(mainCtx(), &pb.GetActionResultRequest{ActionDigest: digest})
	if err != nil || got.ExitCode != 1 {
		t.Fatalf("original AC should survive: %v %v", got, err)
	}

	// The same key is free under another scope: immutability is per prefix.
	if _, err := s.UpdateActionResult(cache.WithACImmutable(prCtx()), &pb.UpdateActionResultRequest{
		ActionDigest: digest,
		ActionResult: &pb.ActionResult{ExitCode: 3},
	}); err != nil {
		t.Fatalf("pr scope write of a key that only exists in main should succeed: %v", err)
	}

	// Legacy (no flag) overwrites as before.
	if _, err := s.UpdateActionResult(mainCtx(), &pb.UpdateActionResultRequest{
		ActionDigest: digest,
		ActionResult: &pb.ActionResult{ExitCode: 4},
	}); err != nil {
		t.Fatalf("overwrite without the immutable flag should succeed: %v", err)
	}
	got, err = s.GetActionResult(mainCtx(), &pb.GetActionResultRequest{ActionDigest: digest})
	if err != nil || got.ExitCode != 4 {
		t.Fatalf("legacy overwrite should win: %v %v", got, err)
	}
}

func TestReadContexts(t *testing.T) {
	legacy := context.Background()
	if got := cache.ReadContexts(legacy); len(got) != 1 || got[0] != legacy {
		t.Fatalf("no metadata must yield the request context only: %v", got)
	}

	ctx := cache.WithFallbackStoragePrefixes(
		cache.WithStoragePrefix(legacy, "a/pr"), []string{"a/pr", "a/main", "a/other"})
	got := cache.ReadContexts(ctx)
	var prefixes []string
	for _, c := range got {
		p, _ := cache.StoragePrefixFromContext(c)
		prefixes = append(prefixes, p)
		if len(cache.FallbackStoragePrefixesFromContext(c)) != 0 && c != ctx {
			t.Fatal("fallback contexts must not carry fallbacks of their own")
		}
	}
	want := []string{"a/pr", "a/main", "a/other"}
	if len(prefixes) != len(want) {
		t.Fatalf("got %v want %v", prefixes, want)
	}
	for i := range want {
		if prefixes[i] != want[i] {
			t.Fatalf("got %v want %v", prefixes, want)
		}
	}
}

func TestStoragePrefixInterceptorTrustScopeMetadata(t *testing.T) {
	lift := func(pairs ...string) (context.Context, error) {
		md := metadata.Pairs(append([]string{cache.StoragePrefixGRPCMetadataKey, trustScopePRPrefix}, pairs...)...)
		return storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), nil)
	}

	t.Run("legacy request carries neither", func(t *testing.T) {
		ctx, err := lift()
		if err != nil {
			t.Fatal(err)
		}
		if cache.FallbackStoragePrefixesFromContext(ctx) != nil || cache.ACImmutableFromContext(ctx) {
			t.Fatal("no trust-scope metadata must leave the context unchanged")
		}
	})

	t.Run("scoped prefix and fallbacks are lifted", func(t *testing.T) {
		ctx, err := lift(cache.FallbackPrefixesGRPCMetadataKey, trustScopeMainPrefix+", a/b/main", cache.ACImmutableGRPCMetadataKey, "1")
		if err != nil {
			t.Fatal(err)
		}
		if p, _ := cache.StoragePrefixFromContext(ctx); p != trustScopePRPrefix {
			t.Fatalf("prefix %q", p)
		}
		fb := cache.FallbackStoragePrefixesFromContext(ctx)
		if len(fb) != 2 || fb[0] != trustScopeMainPrefix || fb[1] != "a/b/main" {
			t.Fatalf("fallbacks %v", fb)
		}
		if !cache.ACImmutableFromContext(ctx) {
			t.Fatal("immutable flag not lifted")
		}
	})

	for _, invalid := range []string{"/abs/main", "a/../main", "a//main", ".."} {
		t.Run("rejects fallback "+invalid, func(t *testing.T) {
			_, err := lift(cache.FallbackPrefixesGRPCMetadataKey, trustScopeMainPrefix+","+invalid)
			requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "invalid_fallback")
		})
	}

	t.Run("rejects duplicate fallback metadata", func(t *testing.T) {
		_, err := lift(cache.FallbackPrefixesGRPCMetadataKey, "a/main", cache.FallbackPrefixesGRPCMetadataKey, "b/main")
		requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "duplicate_fallback")
	})

	t.Run("rejects non-1 immutable flag", func(t *testing.T) {
		_, err := lift(cache.ACImmutableGRPCMetadataKey, "true")
		requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "invalid_ac_immutable")
	})
}

func TestTrustScopeACImmutableConcurrentWritersKeepFirst(t *testing.T) {
	s, _ := trustScopeServer(t, true)
	_, hash := testutils.RandomDataAndHash(64)
	digest := &pb.Digest{Hash: hash, SizeBytes: 64}

	const writers = 32
	var wg sync.WaitGroup
	oks := make([]int32, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.UpdateActionResult(cache.WithACImmutable(mainCtx()), &pb.UpdateActionResultRequest{
				ActionDigest: digest,
				ActionResult: &pb.ActionResult{ExitCode: int32(i + 1)},
			})
			if err == nil {
				oks[i] = 1
			} else if status.Code(err) != codes.AlreadyExists {
				t.Errorf("writer %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	winner := int32(0)
	for i, ok := range oks {
		if ok == 1 {
			if winner != 0 {
				t.Fatalf("more than one writer succeeded: %d and %d", winner, i+1)
			}
			winner = int32(i + 1)
		}
	}
	if winner == 0 {
		t.Fatal("no writer succeeded")
	}
	got, err := s.GetActionResult(mainCtx(), &pb.GetActionResultRequest{ActionDigest: digest})
	if err != nil || got.ExitCode != winner {
		t.Fatalf("stored result %v (err %v) is not the single successful write %d", got, err, winner)
	}
}

func TestTrustScopeDepsCheckResolvesFallbackBlobs(t *testing.T) {
	s, c := trustScopeServer(t, true)
	blob := putCAS(t, c, mainCtx(), 128)
	_, acHash := testutils.RandomDataAndHash(32)
	acDigest := &pb.Digest{Hash: acHash, SizeBytes: 32}
	_, err := s.UpdateActionResult(prCtx(), &pb.UpdateActionResultRequest{
		ActionDigest: acDigest,
		ActionResult: &pb.ActionResult{OutputFiles: []*pb.OutputFile{{Path: "out", Digest: blob}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := s.GetActionResult(prCtx(), &pb.GetActionResultRequest{
		ActionDigest:      acDigest,
		InlineOutputFiles: []string{"out"},
	})
	if err != nil {
		t.Fatalf("pr result whose output blob lives only in main must validate: %v", err)
	}
	if len(got.OutputFiles) != 1 || len(got.OutputFiles[0].Contents) != 128 {
		t.Fatalf("output file was not inlined from the fallback scope: %v", got.OutputFiles)
	}

	if _, err := s.GetActionResult(mainCtx(), &pb.GetActionResultRequest{ActionDigest: acDigest}); status.Code(err) != codes.NotFound {
		t.Fatalf("main must not read pr's action result, got %v", err)
	}
}

// containsContextRecorder stands in for a disk cache fronting a proxy whose
// Contains would answer through the forwarded fallback list.
type containsContextRecorder struct {
	disk.Cache
	containsCtx context.Context
}

func (c *containsContextRecorder) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	c.containsCtx = ctx
	return c.Cache.Contains(ctx, kind, hash, size)
}

func TestTrustScopeACImmutableExistenceCheckIgnoresFallbacks(t *testing.T) {
	s, c := trustScopeServer(t, false)
	rec := &containsContextRecorder{Cache: c}
	s.cache = rec

	_, acHash := testutils.RandomDataAndHash(32)
	acDigest := &pb.Digest{Hash: acHash, SizeBytes: 32}
	if _, err := s.UpdateActionResult(mainCtx(), &pb.UpdateActionResultRequest{
		ActionDigest: acDigest, ActionResult: &pb.ActionResult{ExitCode: 1},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.UpdateActionResult(cache.WithACImmutable(prCtx()), &pb.UpdateActionResultRequest{
		ActionDigest: acDigest, ActionResult: &pb.ActionResult{ExitCode: 2},
	}); err != nil {
		t.Fatalf("pr create-only write of a key that exists only in main must succeed: %v", err)
	}
	if fb := cache.FallbackStoragePrefixesFromContext(rec.containsCtx); len(fb) != 0 {
		t.Fatalf("create-only existence check must not carry fallbacks, got %v", fb)
	}
}
