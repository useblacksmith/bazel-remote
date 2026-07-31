package grpcproxy

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	asset "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/asset/v1"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
)

// markedRejection builds the exact error shape the L1 trust interceptors
// mint: InvalidArgument plus a google.rpc.ErrorInfo detail in our domain.
// Built by hand here (not via the server package) so this test pins the
// wire contract itself, not a shared helper.
func markedRejection(t *testing.T, reason string) error {
	t.Helper()
	st := status.New(codes.InvalidArgument, "unknown x-blacksmith-s3-endpoint metadata value")
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Domain:   cache.TrustRejectionErrorDomain,
		Reason:   reason,
		Metadata: map[string]string{"cause": "unknown"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return detailed.Err()
}

func TestMissForBackendErrorConfigRejection(t *testing.T) {
	marked := markedRejection(t, cache.RejectionReasonS3BackendSelector)

	before := testutil.ToFloat64(configRejectMisses.WithLabelValues("ac_get"))
	if !missForBackendError(context.Background(), marked, "ac_get") {
		t.Fatal("marked trust rejection must degrade to a miss")
	}
	if got := testutil.ToFloat64(configRejectMisses.WithLabelValues("ac_get")) - before; got != 1 {
		t.Fatalf("configRejectMisses delta = %v, want 1", got)
	}

	// The storage-prefix reason shares the domain and degrades identically.
	if !missForBackendError(context.Background(), markedRejection(t, cache.RejectionReasonStoragePrefix), "cas_read") {
		t.Fatal("marked storage-prefix rejection must degrade to a miss")
	}
}

func TestMissForBackendErrorUnmarkedErrorsStayStrict(t *testing.T) {
	// Customer-caused validation errors and server bugs carry no marker and
	// must keep failing strictly — the degradation is marker-based, never
	// error-class based. Plain non-status errors (application failures
	// stringified upstream, hex-decode errors) are included: only
	// unambiguous transport-class errors may degrade.
	for _, err := range []error{
		status.Error(codes.InvalidArgument, "invalid resource name"),
		status.Error(codes.Internal, "backend exploded"),
		status.Error(codes.Unknown, "unknown"),
		status.Error(codes.FailedPrecondition, "nope"),
		status.Error(codes.ResourceExhausted, "asset api quota"),
		errors.New("plain application error"),
	} {
		if missForBackendError(context.Background(), err, "ac_get") {
			t.Fatalf("unmarked error %v must not degrade to a miss", err)
		}
	}
}

func TestMissForBackendErrorClientCanceledIsMeteredSeparately(t *testing.T) {
	// A Canceled observed after the caller's own context died is the build
	// giving up, not L1 trouble: it still degrades to a miss (the caller is
	// gone) but must stay off the alertable transport-miss series.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	transportBefore := testutil.ToFloat64(transportMisses.WithLabelValues("cas_read"))
	clientBefore := testutil.ToFloat64(clientCanceledMisses.WithLabelValues("cas_read"))

	if !missForBackendError(canceledCtx, status.Error(codes.Canceled, "context canceled"), "cas_read") {
		t.Fatal("caller-canceled transport error must still degrade to a miss")
	}
	if got := testutil.ToFloat64(clientCanceledMisses.WithLabelValues("cas_read")) - clientBefore; got != 1 {
		t.Fatalf("clientCanceledMisses delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(transportMisses.WithLabelValues("cas_read")) - transportBefore; got != 0 {
		t.Fatalf("transportMisses delta = %v, want 0 (client cancel must not pollute the alertable series)", got)
	}

	// The same code with a live caller context is backend trouble and stays
	// on the transport series.
	if !missForBackendError(context.Background(), status.Error(codes.Canceled, "stream torn down"), "cas_read") {
		t.Fatal("backend-origin Canceled must degrade to a metered transport miss")
	}
	if got := testutil.ToFloat64(transportMisses.WithLabelValues("cas_read")) - transportBefore; got != 1 {
		t.Fatalf("transportMisses delta = %v, want 1", got)
	}
}

func TestEnsureOutgoingMetadataValueConflictPrefersRequestScoped(t *testing.T) {
	// Idempotency is by value, not key presence: the same value is a no-op,
	// a conflicting value is overridden by the request-scoped one (silently
	// deferring to a foreign value could route a tenant's data to the wrong
	// keyspace).
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		cache.StoragePrefixGRPCMetadataKey, "tenant-a/")

	same := ensureOutgoingMetadataValue(ctx, cache.StoragePrefixGRPCMetadataKey, "tenant-a/")
	md, _ := metadata.FromOutgoingContext(same)
	if got := md.Get(cache.StoragePrefixGRPCMetadataKey); len(got) != 1 || got[0] != "tenant-a/" {
		t.Fatalf("same-value attach must be a no-op, got %v", got)
	}

	overridden := ensureOutgoingMetadataValue(ctx, cache.StoragePrefixGRPCMetadataKey, "tenant-b/")
	md, _ = metadata.FromOutgoingContext(overridden)
	if got := md.Get(cache.StoragePrefixGRPCMetadataKey); len(got) != 1 || got[0] != "tenant-b/" {
		t.Fatalf("conflicting value must be overridden with exactly one request-scoped copy, got %v", got)
	}
}

// rejectingBackend simulates an L1 whose trust interceptor rejects every
// cache RPC with the marked error (config race: this client's selector is
// not in the L1's allowlist yet).
type rejectingBackend struct {
	pb.UnimplementedActionCacheServer
	pb.UnimplementedContentAddressableStorageServer
	pb.UnimplementedCapabilitiesServer
	bs.UnimplementedByteStreamServer
	asset.UnimplementedFetchServer
	err error
}

func (b *rejectingBackend) GetActionResult(context.Context, *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	return nil, b.err
}

func (b *rejectingBackend) FindMissingBlobs(context.Context, *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	return nil, b.err
}

func (b *rejectingBackend) FetchBlob(context.Context, *asset.FetchBlobRequest) (*asset.FetchBlobResponse, error) {
	return nil, b.err
}

// Read rejects the stream from the handler, which the client observes on
// its first Recv — exactly how the L1's trust interceptors reject
// header-first ByteStream reads.
func (b *rejectingBackend) Read(*bs.ReadRequest, bs.ByteStream_ReadServer) error {
	return b.err
}

func newRejectingFixture(t *testing.T, backendErr error) *remoteGrpcProxyCache {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	backend := &rejectingBackend{err: backendErr}
	pb.RegisterActionCacheServer(srv, backend)
	pb.RegisterContentAddressableStorageServer(srv, backend)
	bs.RegisterByteStreamServer(srv, backend)
	asset.RegisterFetchServer(srv, backend)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)

	cc, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return &remoteGrpcProxyCache{
		clients:      NewGrpcClients(cc),
		accessLogger: logger,
		errorLogger:  logger,
	}
}

func TestMarkedRejectionDegradesACGetToMiss(t *testing.T) {
	// AC Get is the build-failing path: a returned error from a proxy Get is
	// wrapped as INTERNAL by the disk layer and surfaces to the build.
	r := newRejectingFixture(t, markedRejection(t, cache.RejectionReasonS3BackendSelector))
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	before := testutil.ToFloat64(configRejectMisses.WithLabelValues("ac_get"))
	rc, size, err := r.Get(context.Background(), cache.AC, hash, -1)
	if err != nil {
		t.Fatalf("marked rejection must degrade AC Get to a miss, got err %v", err)
	}
	if rc != nil || size != -1 {
		t.Fatalf("expected miss (nil, -1), got (%v, %d)", rc, size)
	}
	if got := testutil.ToFloat64(configRejectMisses.WithLabelValues("ac_get")) - before; got != 1 {
		t.Fatalf("configRejectMisses{ac_get} delta = %v, want 1", got)
	}
}

func TestMarkedRejectionDegradesContainsToMiss(t *testing.T) {
	r := newRejectingFixture(t, markedRejection(t, cache.RejectionReasonS3BackendSelector))
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	before := testutil.ToFloat64(configRejectMisses.WithLabelValues("cas_contains"))
	exists, size := r.Contains(context.Background(), cache.CAS, hash, 4)
	if exists || size != -1 {
		t.Fatalf("expected miss (false, -1), got (%v, %d)", exists, size)
	}
	if got := testutil.ToFloat64(configRejectMisses.WithLabelValues("cas_contains")) - before; got != 1 {
		t.Fatalf("configRejectMisses{cas_contains} delta = %v, want 1", got)
	}
}

func TestMarkedRejectionDegradesUnknownSizeContainsToMiss(t *testing.T) {
	// The size<0 Contains path goes through fetchBlobDigest (asset API); a
	// rejection there must hit the same meters as every other read path —
	// previously it returned an unclassified, uncounted miss.
	r := newRejectingFixture(t, markedRejection(t, cache.RejectionReasonS3BackendSelector))
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	before := testutil.ToFloat64(configRejectMisses.WithLabelValues("cas_contains"))
	exists, size := r.Contains(context.Background(), cache.CAS, hash, -1)
	if exists || size != -1 {
		t.Fatalf("expected miss (false, -1), got (%v, %d)", exists, size)
	}
	if got := testutil.ToFloat64(configRejectMisses.WithLabelValues("cas_contains")) - before; got != 1 {
		t.Fatalf("configRejectMisses{cas_contains} delta = %v, want 1", got)
	}
}

// TestMarkedRejectionDegradesStreamingCASGetToMiss covers the streaming
// read path: a ByteStream Read rejection surfaces on the client's first
// Recv, after the stream object exists. Get must pull that first message
// eagerly and degrade a marked rejection to a metered miss, identically to
// the unary paths — previously the error escaped into the disk layer's
// mid-fill catch-all and was never counted as a config-reject miss.
func TestMarkedRejectionDegradesStreamingCASGetToMiss(t *testing.T) {
	r := newRejectingFixture(t, markedRejection(t, cache.RejectionReasonS3BackendSelector))
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	before := testutil.ToFloat64(configRejectMisses.WithLabelValues("cas_read"))
	rc, size, err := r.Get(context.Background(), cache.CAS, hash, 4)
	if err != nil {
		t.Fatalf("marked rejection must degrade streaming CAS Get to a miss, got err %v", err)
	}
	if rc != nil || size != -1 {
		t.Fatalf("expected miss (nil, -1), got (%v, %d)", rc, size)
	}
	if got := testutil.ToFloat64(configRejectMisses.WithLabelValues("cas_read")) - before; got != 1 {
		t.Fatalf("configRejectMisses{cas_read} delta = %v, want 1", got)
	}
}

func TestStreamingCASGetNotFoundIsCleanMiss(t *testing.T) {
	// A deferred NotFound (missing blob) on the first Recv is a plain,
	// unmetered miss — not an error and not a config-reject.
	r := newRejectingFixture(t, status.Error(codes.NotFound, "blob not found"))
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	before := testutil.ToFloat64(configRejectMisses.WithLabelValues("cas_read"))
	rc, size, err := r.Get(context.Background(), cache.CAS, hash, 4)
	if err != nil || rc != nil || size != -1 {
		t.Fatalf("expected clean miss (nil, -1, nil), got (%v, %d, %v)", rc, size, err)
	}
	if got := testutil.ToFloat64(configRejectMisses.WithLabelValues("cas_read")) - before; got != 0 {
		t.Fatalf("configRejectMisses{cas_read} delta = %v, want 0", got)
	}
}

func TestUnmarkedErrorStillFailsStreamingCASGet(t *testing.T) {
	// Unmarked, non-transport errors keep failing strictly on the stream
	// path, matching the unary contract.
	r := newRejectingFixture(t, status.Error(codes.Internal, "backend exploded"))
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	_, _, err := r.Get(context.Background(), cache.CAS, hash, 4)
	if status.Code(err) != codes.Internal {
		t.Fatalf("unmarked Internal must surface as an error, got %v", err)
	}
}

func TestUnmarkedInvalidArgumentStillFailsACGet(t *testing.T) {
	r := newRejectingFixture(t, status.Error(codes.InvalidArgument, "malformed request"))
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	_, _, err := r.Get(context.Background(), cache.AC, hash, -1)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unmarked InvalidArgument must surface as an error, got %v", err)
	}
}
