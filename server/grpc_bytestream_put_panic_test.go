package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type panickingPutCache struct{ disk.Cache }

func (panickingPutCache) Put(context.Context, cache.EntryKind, string, int64, io.Reader) error {
	panic("decoder bug")
}

func TestGrpcByteStreamWritePutPanicFailsOnlyThatWrite(t *testing.T) {
	t.Parallel()

	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()
	diskCache, err := disk.New(dir, 1024*1024, disk.WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	fixture := grpcTestSetupWithCustomCache(t, false, true, panickingPutCache{diskCache})

	blob, hash := testutils.RandomDataAndHash(1024)
	bswc, err := fixture.bsClient.Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = bswc.Send(&bytestream.WriteRequest{
		ResourceName: fmt.Sprintf("instance/uploads/%s/blobs/%s/%d", uuid.New().String(), hash, len(blob)),
		FinishWrite:  true,
		Data:         blob,
	})
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	_, err = bswc.CloseAndRecv()
	if status.Code(err) != codes.Internal {
		t.Fatalf("got %v, want Internal", err)
	}

	if _, err := fixture.capabilitiesClient.GetCapabilities(ctx, &pb.GetCapabilitiesRequest{}); err != nil {
		t.Fatalf("server unusable after put panic: %v", err)
	}
}
