package server

import (
	"context"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Shared plumbing for the tenant-metadata trust interceptors (storage prefix
// and S3 backend selector). Both features forward per-tenant request
// metadata from a trusted upstream and enforce it fail-closed at the L1
// boundary, so they share the exemption list, the exactly-one-value
// extraction rule, and the context-carrying stream wrapper by contract.

// exemptFromTenantMetadata reports whether a gRPC method carries no tenant
// data and is therefore allowed without tenant metadata (storage prefix,
// auth secret, backend selector). Both trust interceptors share this list by
// contract: a method that needs no tenant context for one feature needs none
// for the other.
func exemptFromTenantMetadata(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(fullMethod, "/build.bazel.remote.execution.v2.Capabilities/")
}

// singleMetadataValue extracts the exactly-one value both trust contracts
// require for key. cause is "" on success, otherwise "missing" or
// "duplicate" (honoring one of several values would risk routing a tenant's
// data to the wrong keyspace or shard, so duplicates are rejected loudly).
func singleMetadataValue(md metadata.MD, key string) (value string, cause string) {
	values := md.Get(key)
	switch len(values) {
	case 0:
		return "", "missing"
	case 1:
		return values[0], ""
	default:
		return "", "duplicate"
	}
}

// trustRejectionLogEvery rate-limits rejection logging to one line per
// (reason, cause) pair per interval. A misconfigured fleet can reject at
// full request rate, and rejections are already metered per cause
// (bazel_remote_..._rejected_total) — the log line exists so a live
// debugging session on the node sees the incident in journald at all
// (validated 2026-07-30: counters incremented, journal stayed empty),
// not to reproduce the counter's volume.
const trustRejectionLogEvery = 30 * time.Second

var trustRejectionLastLog sync.Map // "reason/cause" -> *atomic.Int64 (unix nanos)

func logTrustRejection(reason, cause, message string) {
	gateAny, _ := trustRejectionLastLog.LoadOrStore(reason+"/"+cause, new(atomic.Int64))
	gate := gateAny.(*atomic.Int64)
	last := gate.Load()
	now := time.Now().UnixNano()
	if now-last < int64(trustRejectionLogEvery) || !gate.CompareAndSwap(last, now) {
		return
	}
	log.Printf("trust interceptor rejected request (reason=%s cause=%s, logged at most once per %v per cause; see the *_rejected_total counters for volume): %s",
		reason, cause, trustRejectionLogEvery, message)
}

// trustRejection mints an InvalidArgument rejection carrying the typed
// google.rpc.ErrorInfo marker (cache.TrustRejectionErrorDomain). The trusted
// upstream's grpcproxy degrades marked rejections to metered cache misses —
// config races (allowlist drift, FA/L1 version skew) must not fail builds —
// while every unmarked error keeps failing strictly. Only rejections minted
// by our own trust interceptors may carry this marker.
func trustRejection(reason, cause, format string, args ...interface{}) error {
	st := status.Newf(codes.InvalidArgument, format, args...)
	logTrustRejection(reason, cause, st.Message())
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Domain:   cache.TrustRejectionErrorDomain,
		Reason:   reason,
		Metadata: map[string]string{"cause": cause},
	})
	if err != nil {
		// Marshalling the detail cannot reasonably fail; degrade to the
		// unmarked rejection (strict client-side failure) rather than panic.
		return st.Err()
	}
	return detailed.Err()
}

// tenantMetadataServerStream carries the context enriched by a trust
// interceptor (storage prefix and/or backend selector lifted from validated
// metadata) into the wrapped handler's stream.
type tenantMetadataServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *tenantMetadataServerStream) Context() context.Context {
	return s.ctx
}
