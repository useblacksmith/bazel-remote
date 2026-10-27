package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"testing"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// craftedZstdFrame is the two-block frame from klauspost/compress
// TestDecoderHuffWeightsTableLog (GHSA-fpmh-frq8-7rj8): versions up to
// v1.20.1 panic decoding it in both DecodeAll and the streaming decoder.
func craftedZstdFrame() []byte {
	return slices.Concat(
		[]byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38, 0x74, 0x06, 0x00, 0x42, 0xb8, 0x32, 0x0d, 0x1a, 0x00, 0x00, 0x00, 0x90, 0xaf, 0xfa, 0xff, 0x03, 0x00, 0x80, 0xff, 0x7f},
		bytes.Repeat([]byte{0xa5, 0x94, 0x52, 0x4a, 0x29}, 37),
		[]byte{0xa5, 0x94, 0x12, 0x00, 0xad, 0x56, 0x03, 0xee, 0xff, 0xdf, 0xb3, 0x1a, 0x1c, 0x1a, 0x00, 0xe0, 0xff, 0x1f, 0x0f, 0xb1, 0x9c, 0x5a, 0xcf, 0x4d, 0xe4, 0x1d, 0x7b, 0xab, 0x99, 0x5e, 0x01, 0x51, 0xa4, 0x73, 0xd8, 0xb1, 0xf3, 0x39, 0x4b, 0x67, 0xf6, 0xab, 0x1a, 0xab, 0x1a, 0xab, 0x1a},
		bytes.Repeat([]byte{0x8c, 0x31, 0xc6, 0x18, 0x63}, 1365), []byte{0x8c, 0x31},
		bytes.Repeat([]byte{0x63, 0x8c, 0x31, 0xc6, 0x18}, 1365), []byte{0x63, 0x2c},
		bytes.Repeat([]byte{0x31, 0xc6, 0x18, 0x63, 0x8c}, 1365), []byte{0x31, 0x46},
		bytes.Repeat([]byte{0x31, 0xc6, 0x18, 0x63, 0x8c}, 1365), []byte{0x31, 0x06, 0x00},
	)
}

// An arbitrary digest: the frame never decodes, so its content hash is moot.
func craftedZstdDigest() *pb.Digest {
	sum := sha256.Sum256([]byte("crafted zstd frame"))
	return &pb.Digest{Hash: hex.EncodeToString(sum[:]), SizeBytes: 4096}
}

func TestGrpcBatchUpdateBlobsCraftedZstdFrameIsRejected(t *testing.T) {
	t.Parallel()

	fixture := grpcTestSetup(t)

	resp, err := fixture.casClient.BatchUpdateBlobs(ctx, &pb.BatchUpdateBlobsRequest{
		Requests: []*pb.BatchUpdateBlobsRequest_Request{{
			Digest:     craftedZstdDigest(),
			Data:       craftedZstdFrame(),
			Compressor: pb.Compressor_ZSTD,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := codes.Code(resp.Responses[0].Status.Code); got == codes.OK {
		t.Fatal("crafted zstd frame was accepted")
	}

	if _, err := fixture.capabilitiesClient.GetCapabilities(ctx, &pb.GetCapabilitiesRequest{}); err != nil {
		t.Fatalf("server unusable after crafted frame: %v", err)
	}
}

func TestGrpcByteStreamWriteCraftedZstdFrameIsRejected(t *testing.T) {
	t.Parallel()

	fixture := grpcTestSetup(t)
	digest := craftedZstdDigest()

	bswc, err := fixture.bsClient.Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = bswc.Send(&bytestream.WriteRequest{
		ResourceName: fmt.Sprintf("instance/uploads/%s/compressed-blobs/zstd/%s/%d",
			uuid.New().String(), digest.Hash, digest.SizeBytes),
		FinishWrite: true,
		Data:        craftedZstdFrame(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bswc.CloseAndRecv(); err == nil {
		t.Fatal("crafted zstd frame was accepted")
	}

	if _, err := fixture.capabilitiesClient.GetCapabilities(ctx, &pb.GetCapabilitiesRequest{}); err != nil {
		t.Fatalf("server unusable after crafted frame: %v", err)
	}
}

func TestGRPCRecoveryInterceptorsTurnPanicIntoInternal(t *testing.T) {
	t.Parallel()

	resp, err := GRPCRecoveryUnaryServerInterceptor(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/test/Unary"},
		func(context.Context, interface{}) (interface{}, error) { panic("boom") })
	if resp != nil || status.Code(err) != codes.Internal {
		t.Fatalf("unary: got (%v, %v), want (nil, Internal)", resp, err)
	}

	err = GRPCRecoveryStreamServerInterceptor(nil, nil,
		&grpc.StreamServerInfo{FullMethod: "/test/Stream"},
		func(interface{}, grpc.ServerStream) error { panic("boom") })
	if status.Code(err) != codes.Internal {
		t.Fatalf("stream: got %v, want Internal", err)
	}
}
