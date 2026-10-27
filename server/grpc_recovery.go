package server

import (
	"context"
	"log"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpc-go does not recover panics raised inside handlers, so without these
// interceptors one malformed request (e.g. a crafted zstd frame handed to a
// decoder bug) exits the whole process and takes every other client's
// in-flight RPC with it. Both interceptors turn a handler panic into
// codes.Internal for that one RPC and log the stack.

func GRPCRecoveryUnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = recoveredPanicStatus(info.FullMethod, r)
			resp = nil
		}
	}()
	return handler(ctx, req)
}

func GRPCRecoveryStreamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = recoveredPanicStatus(info.FullMethod, r)
		}
	}()
	return handler(srv, ss)
}

func recoveredPanicStatus(method string, r interface{}) error {
	log.Printf("GRPC PANIC RECOVERED %s: %v\n%s", method, r, debug.Stack())
	return status.Errorf(codes.Internal, "internal error handling %s", method)
}
