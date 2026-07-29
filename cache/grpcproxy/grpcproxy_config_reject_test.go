package grpcproxy

import (
	"context"
	"net"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
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
	if !missForBackendError(marked, "ac_get") {
		t.Fatal("marked trust rejection must degrade to a miss")
	}
	if got := testutil.ToFloat64(configRejectMisses.WithLabelValues("ac_get")) - before; got != 1 {
		t.Fatalf("configRejectMisses delta = %v, want 1", got)
	}

	// The storage-prefix reason shares the domain and degrades identically.
	if !missForBackendError(markedRejection(t, cache.RejectionReasonStoragePrefix), "cas_read") {
		t.Fatal("marked storage-prefix rejection must degrade to a miss")
	}
}

func TestMissForBackendErrorUnmarkedErrorsStayStrict(t *testing.T) {
	// Customer-caused validation errors and server bugs carry no marker and
	// must keep failing strictly — the degradation is marker-based, never
	// error-class based.
	for _, err := range []error{
		status.Error(codes.InvalidArgument, "invalid resource name"),
		status.Error(codes.Internal, "backend exploded"),
		status.Error(codes.Unknown, "unknown"),
		status.Error(codes.FailedPrecondition, "nope"),
	} {
		if missForBackendError(err, "ac_get") {
			t.Fatalf("unmarked error %v must not degrade to a miss", err)
		}
	}
}

// rejectingBackend simulates an L1 whose trust interceptor rejects every
// cache RPC with the marked error (config race: this client's selector is
// not in the L1's allowlist yet).
type rejectingBackend struct {
	pb.UnimplementedActionCacheServer
	pb.UnimplementedContentAddressableStorageServer
	pb.UnimplementedCapabilitiesServer
	err error
}

func (b *rejectingBackend) GetActionResult(context.Context, *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	return nil, b.err
}

func (b *rejectingBackend) FindMissingBlobs(context.Context, *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	return nil, b.err
}

func newRejectingFixture(t *testing.T, backendErr error) *remoteGrpcProxyCache {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	backend := &rejectingBackend{err: backendErr}
	pb.RegisterActionCacheServer(srv, backend)
	pb.RegisterContentAddressableStorageServer(srv, backend)
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

func TestUnmarkedInvalidArgumentStillFailsACGet(t *testing.T) {
	r := newRejectingFixture(t, status.Error(codes.InvalidArgument, "malformed request"))
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	_, _, err := r.Get(context.Background(), cache.AC, hash, -1)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unmarked InvalidArgument must surface as an error, got %v", err)
	}
}
