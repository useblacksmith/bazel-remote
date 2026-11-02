package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A client that opens a stream and never finishes it keeps GracefulStop
// waiting forever; stopGrpcServer must still return once the bound expires.
func TestStopGrpcServerBoundsAHungStream(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	streamOpened := make(chan struct{})
	desc := grpc.StreamDesc{StreamName: "Hang", ServerStreams: true, ClientStreams: true}
	svc := &grpc.ServiceDesc{
		ServiceName: "test.Hang",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    desc.StreamName,
			ServerStreams: true,
			ClientStreams: true,
			Handler: func(_ any, ss grpc.ServerStream) error {
				close(streamOpened)
				<-ss.Context().Done()
				return ss.Context().Err()
			},
		}},
	}
	s := grpc.NewServer()
	s.RegisterService(svc, struct{}{})
	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := conn.NewStream(ctx, &desc, "/test.Hang/Hang"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-streamOpened:
	case <-time.After(5 * time.Second):
		t.Fatal("stream never reached the handler")
	}

	start := time.Now()
	done := make(chan struct{})
	go func() {
		stopGrpcServer(s, 200*time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stopGrpcServer did not return with a hung stream open")
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("stopGrpcServer returned after %s, before the drain bound", elapsed)
	}
}

func TestStopGrpcServerReturnsEarlyWhenIdle(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	go func() { _ = s.Serve(lis) }()

	start := time.Now()
	stopGrpcServer(s, 10*time.Second)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("idle server took %s to stop", elapsed)
	}
}
