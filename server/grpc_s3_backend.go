package server

import (
	"context"

	"github.com/buchgr/bazel-remote/v2/cache"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// S3-backend trust interceptors: when an L1 bazel-remote node is configured
// with a map of allowlisted S3 backends (multi-shard MinIO), the trusted
// upstream (FA host) forwards each tenant's pinned backing-store endpoint as
// gRPC metadata (cache.S3BackendGRPCMetadataKey). These interceptors validate
// the forwarded endpoint against the allowlist and lift it onto the request
// context, from which the s3proxy routes reads and write-through to the
// matching MinIO cluster.
//
// The trust model mirrors the storage-prefix interceptors (fail-closed):
// when a backends map is configured, every cache RPC must carry exactly one
// allowlisted endpoint or it is rejected at the boundary — a request routed
// to a guessed backend would read or write another shard's keyspace. Health
// and capabilities RPCs are exempt, exactly like the storage-prefix contract.
// These interceptors are only installed in multi-backend mode; single-backend
// deployments ignore the metadata entirely (backward compatible).

func s3BackendFromIncomingContext(ctx context.Context, allowed map[string]bool) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Errorf(codes.InvalidArgument,
			"missing %s metadata", cache.S3BackendGRPCMetadataKey)
	}

	values := md.Get(cache.S3BackendGRPCMetadataKey)
	switch {
	case len(values) == 0:
		return ctx, status.Errorf(codes.InvalidArgument,
			"missing %s metadata", cache.S3BackendGRPCMetadataKey)
	case len(values) > 1:
		// Duplicate values mean an attachment bug upstream; honoring either
		// risks routing a tenant's data to the wrong shard, so reject loudly.
		return ctx, status.Errorf(codes.InvalidArgument,
			"duplicate %s metadata (%d values)", cache.S3BackendGRPCMetadataKey, len(values))
	}
	selector := values[0]
	// Exact opaque string match against the configured backends map keys —
	// no URL normalization. The upstream must forward the adoption payload's
	// bazelre_cache_endpoint verbatim, and the map must be keyed by the same
	// strings.
	if !allowed[selector] {
		return ctx, status.Errorf(codes.InvalidArgument,
			"unknown %s metadata value %q", cache.S3BackendGRPCMetadataKey, selector)
	}
	return cache.WithS3Backend(ctx, selector), nil
}

// GRPCS3BackendUnaryServerInterceptor returns a unary interceptor that
// enforces the fail-closed backend-selection contract and lifts the forwarded
// backend selector onto the context. allowed holds the configured backends
// map keys (tenant-facing endpoint URLs).
func GRPCS3BackendUnaryServerInterceptor(allowed map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if exemptFromStoragePrefix(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, err := s3BackendFromIncomingContext(ctx, allowed)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// GRPCS3BackendStreamServerInterceptor returns a stream interceptor that
// enforces the fail-closed backend-selection contract and lifts the forwarded
// backend selector onto the context.
func GRPCS3BackendStreamServerInterceptor(allowed map[string]bool) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if exemptFromStoragePrefix(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, err := s3BackendFromIncomingContext(ss.Context(), allowed)
		if err != nil {
			return err
		}
		return handler(srv, &s3BackendServerStream{ServerStream: ss, ctx: ctx})
	}
}

type s3BackendServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *s3BackendServerStream) Context() context.Context {
	return s.ctx
}
