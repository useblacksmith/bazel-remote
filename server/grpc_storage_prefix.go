package server

import (
	"context"

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
// Only enable this on deployments where every gRPC peer is trusted (private
// network / authenticated), since the prefix determines which tenant's data
// is read and written.

func storagePrefixFromIncomingContext(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, nil
	}
	values := md.Get(cache.StoragePrefixGRPCMetadataKey)
	if len(values) == 0 {
		return ctx, nil
	}
	prefix := values[0]
	if !cache.ValidStoragePrefix(prefix) {
		return ctx, status.Errorf(codes.InvalidArgument,
			"invalid %s metadata value", cache.StoragePrefixGRPCMetadataKey)
	}
	return cache.WithRequiredStoragePrefix(cache.WithStoragePrefix(ctx, prefix)), nil
}

// GRPCStoragePrefixUnaryServerInterceptor returns a unary interceptor that
// lifts a forwarded storage prefix from request metadata onto the context.
func GRPCStoragePrefixUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, err := storagePrefixFromIncomingContext(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// GRPCStoragePrefixStreamServerInterceptor returns a stream interceptor that
// lifts a forwarded storage prefix from request metadata onto the context.
func GRPCStoragePrefixStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := storagePrefixFromIncomingContext(ss.Context())
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
