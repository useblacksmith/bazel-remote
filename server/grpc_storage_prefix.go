package server

import (
	"context"
	"crypto/subtle"
	"strings"

	"github.com/buchgr/bazel-remote/v2/cache"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Storage-prefix trust interceptors: when bazel-remote runs as a shared L1
// node in front of an object-storage backend, upstream embedded bazel-remote
// instances (on FA hosts) forward each request's tenant storage prefix as
// gRPC metadata (cache.StoragePrefixGRPCMetadataKey). These interceptors lift
// that header onto the request context, which both partitions the local disk
// cache keyspace per tenant and scopes backend object keys.
//
// Trust-on is fail-closed: every cache RPC must carry exactly one valid
// prefix (and the shared secret, when configured) or it is rejected at the
// boundary. There is no accept-unscoped mode — a request that would land in
// a shared keyspace is an isolation bug, and the server rejection is the
// mechanism that surfaces it loudly. Health and capabilities RPCs are exempt
// so probes and CheckCapabilities work without tenant context.

// exemptFromStoragePrefix reports whether a gRPC method carries no tenant
// data and is therefore allowed without prefix/auth metadata.
func exemptFromStoragePrefix(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(fullMethod, "/build.bazel.remote.execution.v2.Capabilities/")
}

func storagePrefixFromIncomingContext(ctx context.Context, authSecret string) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Errorf(codes.InvalidArgument,
			"missing %s metadata", cache.StoragePrefixGRPCMetadataKey)
	}

	if authSecret != "" {
		secrets := md.Get(cache.AuthSecretGRPCMetadataKey)
		if len(secrets) != 1 ||
			subtle.ConstantTimeCompare([]byte(secrets[0]), []byte(authSecret)) != 1 {
			return ctx, status.Errorf(codes.Unauthenticated,
				"missing or invalid %s metadata", cache.AuthSecretGRPCMetadataKey)
		}
	}

	values := md.Get(cache.StoragePrefixGRPCMetadataKey)
	switch {
	case len(values) == 0:
		return ctx, status.Errorf(codes.InvalidArgument,
			"missing %s metadata", cache.StoragePrefixGRPCMetadataKey)
	case len(values) > 1:
		// Duplicate prefixes mean an attachment bug upstream; honoring either
		// value risks silent cross-tenant reads/writes, so reject loudly.
		return ctx, status.Errorf(codes.InvalidArgument,
			"duplicate %s metadata (%d values)", cache.StoragePrefixGRPCMetadataKey, len(values))
	}
	prefix := values[0]
	if !cache.ValidStoragePrefix(prefix) {
		return ctx, status.Errorf(codes.InvalidArgument,
			"invalid %s metadata value", cache.StoragePrefixGRPCMetadataKey)
	}
	return cache.WithRequiredStoragePrefix(cache.WithStoragePrefix(ctx, prefix)), nil
}

// GRPCStoragePrefixUnaryServerInterceptor returns a unary interceptor that
// enforces the fail-closed trust contract and lifts the forwarded storage
// prefix onto the context. authSecret, when non-empty, must match the
// cache.AuthSecretGRPCMetadataKey metadata on every non-exempt RPC.
func GRPCStoragePrefixUnaryServerInterceptor(authSecret string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if exemptFromStoragePrefix(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, err := storagePrefixFromIncomingContext(ctx, authSecret)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// GRPCStoragePrefixStreamServerInterceptor returns a stream interceptor that
// enforces the fail-closed trust contract and lifts the forwarded storage
// prefix onto the context.
func GRPCStoragePrefixStreamServerInterceptor(authSecret string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if exemptFromStoragePrefix(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, err := storagePrefixFromIncomingContext(ss.Context(), authSecret)
		if err != nil {
			return err
		}
		return handler(srv, &storagePrefixServerStream{ServerStream: ss, ctx: ctx})
	}
}

type storagePrefixServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *storagePrefixServerStream) Context() context.Context {
	return s.ctx
}
