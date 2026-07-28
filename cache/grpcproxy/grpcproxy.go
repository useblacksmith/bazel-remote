package grpcproxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	asset "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/asset/v1"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// The maximum chunk size to write back to the client in Send calls.
	// Inspired by Goma's FileBlob.FILE_CHUNK maxium size.
	maxChunkSize = 2 * 1024 * 1024 // 2M

	// uploadTimeout bounds a single asynchronous backend upload. Uploads
	// previously ran on context.Background() with no deadline, so a hung
	// backend connection could pin an upload worker forever. The bound is
	// generous (large CAS blobs can be slow on a busy link); it exists to
	// reclaim workers, not to police latency.
	uploadTimeout = 10 * time.Minute
)

var transportMisses = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_grpcproxy_transport_miss_total",
	Help: "Proxy backend requests degraded to cache misses by transport-level failures (fail-open).",
}, []string{"method"})

// errBlobNotFound is fetchBlobDigest's not-found sentinel; callers treat it
// as a cache miss, never a request-failing error.
var errBlobNotFound = errors.New("blob not found")

// isTransportFailure reports whether err is a transport-level failure
// reaching the proxy backend (unreachable node, refused/reset connection,
// missed deadline) rather than an application-level response. The fail-open
// contract maps these to cache misses on the read path: the disk layer wraps
// any error returned from a proxy Get as INTERNAL and surfaces it to the
// build, and an unreachable L1 must never fail a build.
func isTransportFailure(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	if !ok {
		// Non-gRPC errors from the client stack (dial, net, context) are
		// transport-class by construction.
		return true
	}
	switch s.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return true
	}
	return false
}

type GrpcClients struct {
	asset asset.FetchClient
	bs    bs.ByteStreamClient
	ac    pb.ActionCacheClient
	cas   pb.ContentAddressableStorageClient
	cap   pb.CapabilitiesClient
}

func contains[A comparable](arr []A, value A) bool {
	for _, v := range arr {
		if v == value {
			return true
		}
	}
	return false
}

func NewGrpcClients(cc *grpc.ClientConn) *GrpcClients {
	return &GrpcClients{
		asset: asset.NewFetchClient(cc),
		bs:    bs.NewByteStreamClient(cc),
		ac:    pb.NewActionCacheClient(cc),
		cas:   pb.NewContentAddressableStorageClient(cc),
		cap:   pb.NewCapabilitiesClient(cc),
	}
}

func (c *GrpcClients) CheckCapabilities(zstd bool) error {
	resp, err := c.cap.GetCapabilities(context.Background(), &pb.GetCapabilitiesRequest{})
	if err != nil {
		return err
	}
	if !resp.CacheCapabilities.ActionCacheUpdateCapabilities.UpdateEnabled {
		return errors.New("proxy backend does not allow action cache updates")
	}
	if !contains(resp.CacheCapabilities.DigestFunctions, pb.DigestFunction_SHA256) {
		return errors.New("proxy backend does not support sha256")
	}
	if zstd && !contains(resp.CacheCapabilities.SupportedCompressors, pb.Compressor_ZSTD) {
		return errors.New("compression required but the grpc proxy does not support it")
	}
	return nil
}

type remoteGrpcProxyCache struct {
	clients      *GrpcClients
	uploadQueue  chan<- backendproxy.UploadReq
	accessLogger cache.Logger
	errorLogger  cache.Logger
	v2mode       bool
}

func New(clients *GrpcClients, storageMode string,
	accessLogger cache.Logger, errorLogger cache.Logger,
	numUploaders, maxQueuedUploads int) cache.Proxy {

	proxy := &remoteGrpcProxyCache{
		clients:      clients,
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
		v2mode:       storageMode == "zstd",
	}

	proxy.uploadQueue = backendproxy.StartUploaders(proxy, numUploaders, maxQueuedUploads)

	return proxy
}

// Helper function for logging responses
func logResponse(logger cache.Logger, method string, msg string, kind cache.EntryKind, hash string) {
	logger.Printf("GRPC PROXY %s %s %s: %s", strings.ToUpper(method), strings.ToUpper(kind.String()), hash, msg)
}

// withOutgoingStoragePrefix forwards the request-scoped storage prefix (when
// one is attached to ctx) to the downstream bazel-remote as gRPC metadata, so
// the downstream node can preserve tenant isolation on its local disk and its
// own storage backend.
func withOutgoingStoragePrefix(ctx context.Context) context.Context {
	if prefix, ok := cache.StoragePrefixFromContext(ctx); ok {
		return metadata.AppendToOutgoingContext(ctx, cache.StoragePrefixGRPCMetadataKey, prefix)
	}
	return ctx
}

// uploadContext rebuilds an outgoing context for an asynchronous upload from
// the prefix captured at enqueue time (the original request context is long
// gone by the time upload workers run). The context carries a deadline so a
// hung backend cannot pin an upload worker indefinitely; callers must call
// the returned cancel func.
func uploadContext(item backendproxy.UploadReq) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	if item.RequestScopedStoragePrefix && item.StoragePrefix != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, cache.StoragePrefixGRPCMetadataKey, item.StoragePrefix)
	}
	return ctx, cancel
}

func (r *remoteGrpcProxyCache) UploadFile(item backendproxy.UploadReq) {
	defer func() { _ = item.Rc.Close() }()

	if item.RequireStoragePrefix && !item.RequestScopedStoragePrefix {
		logResponse(r.errorLogger, "Upload", "missing request-scoped storage prefix", item.Kind, item.Hash)
	}

	switch item.Kind {
	case cache.RAW:
		// RAW cache entries are a special case of AC, used when --disable_http_ac_validation
		// is enabled. We can treat them as AC in this scope
		fallthrough
	case cache.AC:
		data := make([]byte, item.SizeOnDisk)
		read := int64(0)
		for {
			n, err := item.Rc.Read(data[read:])
			if n > 0 {
				read += int64(n)
			}
			if err == io.EOF || read == item.SizeOnDisk {
				break
			}
		}
		if read != item.SizeOnDisk {
			logResponse(r.errorLogger, "Update", "Unexpected short read", item.Kind, item.Hash)
			return
		}
		ar := &pb.ActionResult{}
		err := proto.Unmarshal(data, ar)
		if err != nil {
			logResponse(r.errorLogger, "Update", err.Error(), item.Kind, item.Hash)
			return
		}
		digest := &pb.Digest{
			Hash:      item.Hash,
			SizeBytes: item.LogicalSize,
		}

		req := &pb.UpdateActionResultRequest{
			ActionDigest: digest,
			ActionResult: ar,
		}
		ctx, cancel := uploadContext(item)
		defer cancel()
		_, err = r.clients.ac.UpdateActionResult(ctx, req)
		if err != nil {
			logResponse(r.errorLogger, "Update", err.Error(), item.Kind, item.Hash)
		}
		return
	case cache.CAS:
		ctx, cancel := uploadContext(item)
		defer cancel()
		stream, err := r.clients.bs.Write(ctx)
		if err != nil {
			logResponse(r.errorLogger, "Write", err.Error(), item.Kind, item.Hash)
			return
		}

		bufSize := item.SizeOnDisk
		if bufSize > maxChunkSize {
			bufSize = maxChunkSize
		}
		buf := make([]byte, bufSize)

		template := "uploads/%s/blobs/%s/%d"
		if r.v2mode {
			template = "uploads/%s/compressed-blobs/zstd/%s/%d"
		}
		resourceName := fmt.Sprintf(template, uuid.New().String(), item.Hash, item.LogicalSize)

		firstIteration := true
		for {
			n, err := item.Rc.Read(buf)
			if err != nil && err != io.EOF {
				logResponse(r.errorLogger, "Write", err.Error(), item.Kind, item.Hash)
				err := stream.CloseSend()
				if err != nil {
					logResponse(r.errorLogger, "Write", err.Error(), item.Kind, item.Hash)
				}
				return
			}
			if n > 0 {
				rn := ""
				if firstIteration {
					firstIteration = false
					rn = resourceName
				}
				req := &bs.WriteRequest{
					ResourceName: rn,
					Data:         buf[:n],
				}
				err := stream.Send(req)
				if err != nil {
					logResponse(r.errorLogger, "Write", err.Error(), item.Kind, item.Hash)
					return
				}
			} else {
				_, err = stream.CloseAndRecv()
				if err != nil {
					logResponse(r.errorLogger, "Write", err.Error(), item.Kind, item.Hash)
					return
				}
				logResponse(r.accessLogger, "Write", "Success", item.Kind, item.Hash)
				return
			}
		}
	default:
		logResponse(r.errorLogger, "Write", "Unexpected kind", item.Kind, item.Hash)
		return
	}
}

func (r *remoteGrpcProxyCache) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	if r.uploadQueue == nil {
		_ = rc.Close()
		return
	}

	// Capture the request-scoped storage prefix at enqueue time; uploads are
	// asynchronous and the request context is gone when workers run.
	prefix, requestScoped := cache.StoragePrefixFromContext(ctx)

	item := backendproxy.UploadReq{
		Hash:                       hash,
		LogicalSize:                logicalSize,
		SizeOnDisk:                 sizeOnDisk,
		Kind:                       kind,
		Rc:                         rc,
		StoragePrefix:              prefix,
		RequestScopedStoragePrefix: requestScoped,
		RequireStoragePrefix:       cache.StoragePrefixRequiredFromContext(ctx),
	}

	select {
	case r.uploadQueue <- item:
	default:
		r.errorLogger.Printf("too many uploads queued")
		_ = rc.Close()
	}
}

func (r *remoteGrpcProxyCache) fetchBlobDigest(ctx context.Context, hash string) (*pb.Digest, error) {
	decoded, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	q := asset.Qualifier{
		Name:  "checksum.sri",
		Value: fmt.Sprintf("sha256-%s", base64.StdEncoding.EncodeToString(decoded)),
	}
	freq := asset.FetchBlobRequest{
		Uris:       []string{},
		Qualifiers: []*asset.Qualifier{&q},
	}

	res, err := r.clients.asset.FetchBlob(ctx, &freq)
	if err != nil {
		return nil, err
	}

	if res.Status.GetCode() == int32(codes.NotFound) {
		return nil, errBlobNotFound
	}
	if res.Status.GetCode() != int32(codes.OK) {
		return nil, errors.New(res.Status.Message)
	}
	return res.BlobDigest, nil
}

func (r *remoteGrpcProxyCache) Get(ctx context.Context, kind cache.EntryKind, hash string, size int64) (io.ReadCloser, int64, error) {
	ctx = withOutgoingStoragePrefix(ctx)
	switch kind {
	case cache.RAW:
		// RAW cache entries are a special case of AC, used when --disable_http_ac_validation
		// is enabled. We can treat them as AC in this scope
		fallthrough
	case cache.AC:
		digest := pb.Digest{
			Hash:      hash,
			SizeBytes: -1,
		}

		req := &pb.GetActionResultRequest{ActionDigest: &digest}

		res, err := r.clients.ac.GetActionResult(ctx, req)
		status, ok := status.FromError(err)
		if ok && status.Code() == codes.NotFound {
			return nil, -1, nil
		}

		if err != nil {
			logResponse(r.errorLogger, "Get", err.Error(), kind, hash)
			if isTransportFailure(err) {
				transportMisses.WithLabelValues("ac_get").Inc()
				return nil, -1, nil
			}
			return nil, -1, err
		}
		data, err := proto.Marshal(res)
		if err != nil {
			logResponse(r.errorLogger, "Get", err.Error(), kind, hash)
			return nil, -1, err
		}

		logResponse(r.accessLogger, "Get", "Success", kind, hash)
		return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil

	case cache.CAS:
		if size < 0 {
			// We don't know the size, so send a FetchBlob request first to get the digest
			digest, err := r.fetchBlobDigest(ctx, hash)
			if err != nil {
				logResponse(r.errorLogger, "Fetch", err.Error(), kind, hash)
				if errors.Is(err, errBlobNotFound) {
					return nil, -1, nil
				}
				if isTransportFailure(err) {
					transportMisses.WithLabelValues("cas_fetch").Inc()
					return nil, -1, nil
				}
				return nil, -1, err
			}
			size = digest.SizeBytes
		}

		template := "blobs/%s/%d"
		if r.v2mode {
			template = "compressed-blobs/zstd/%s/%d"
		}
		req := bs.ReadRequest{
			ResourceName: fmt.Sprintf(template, hash, size),
		}
		stream, err := r.clients.bs.Read(ctx, &req)
		if err != nil {
			logResponse(r.errorLogger, "Read", err.Error(), kind, hash)
			if isTransportFailure(err) {
				transportMisses.WithLabelValues("cas_read").Inc()
				return nil, -1, nil
			}
			return nil, -1, err
		}
		logResponse(r.errorLogger, "Read", "Completed", kind, hash)
		rc := StreamReadCloser[*bs.ReadResponse]{Stream: stream}
		return &rc, size, nil
	default:
		return nil, -1, fmt.Errorf("unexpected kind %s", kind)
	}
}

func (r *remoteGrpcProxyCache) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	ctx = withOutgoingStoragePrefix(ctx)
	switch kind {
	case cache.RAW:
		// RAW cache entries are a special case of AC, used when --disable_http_ac_validation
		// is enabled. We can treat them as AC in this scope
		fallthrough
	case cache.AC:
		// There's not "contains" method for the action cache so the best we can do
		// is to get the object and discard the result
		// We don't expect this to ever be called anyways since it is not part of the grpc protocol
		rc, size, err := r.Get(ctx, kind, hash, size)
		if rc != nil {
			_ = rc.Close()
		}
		if err != nil || size < 0 {
			return false, -1
		}
		return true, size
	case cache.CAS:
		if size < 0 {
			// If don't know the size, use the remote asset api to find the missing blob
			digest, err := r.fetchBlobDigest(ctx, hash)
			if err != nil {
				logResponse(r.errorLogger, "Contains", err.Error(), kind, hash)
				return false, -1
			}
			logResponse(r.accessLogger, "Contains", "Success", kind, hash)
			return true, digest.SizeBytes
		}

		// If we know the size, prefer using the remote execution api
		req := &pb.FindMissingBlobsRequest{
			BlobDigests: []*pb.Digest{{
				Hash:      hash,
				SizeBytes: size,
			}},
		}
		res, err := r.clients.cas.FindMissingBlobs(ctx, req)
		if err != nil {
			logResponse(r.errorLogger, "Contains", err.Error(), kind, hash)
			return false, -1
		}
		for range res.MissingBlobDigests {
			logResponse(r.accessLogger, "Contains", "Not Found", kind, hash)
			return false, -1
		}
		logResponse(r.errorLogger, "Contains", "Success", kind, hash)
		return true, size
	default:
		logResponse(r.errorLogger, "Contains", "Unexpected kind", kind, hash)
		return false, -1
	}
}
