package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"syscall"

	"github.com/buchgr/bazel-remote/v2/cache/azblobproxy"
	"github.com/buchgr/bazel-remote/v2/cache/gcsproxy"
	"github.com/buchgr/bazel-remote/v2/cache/grpcproxy"
	"github.com/buchgr/bazel-remote/v2/cache/httpproxy"
	"github.com/buchgr/bazel-remote/v2/cache/s3proxy"
	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
)

// aggregateUploadFDBudgetFraction caps how much of the process's soft
// RLIMIT_NOFILE the multi-backend upload queues may collectively pin in the
// worst case. Half is deliberately conservative: the other half must stay
// free for the serving path (client connections, disk cache FDs), which is
// the tier that must never lose to background write-through.
const aggregateUploadFDBudgetFraction = 0.5

// assertAggregateUploadFDBudget fails startup when the worst-case FD
// consumption of the per-backend upload queues (backends × queue slots, one
// open reader each) exceeds the budget fraction of the soft NOFILE limit.
func assertAggregateUploadFDBudget(backendCount, maxQueuedUploadsPerBackend int) error {
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		// No limit visible (unusual platform) — nothing to assert against.
		return nil
	}
	budget := uint64(float64(rlimit.Cur) * aggregateUploadFDBudgetFraction)
	aggregate := uint64(backendCount) * uint64(maxQueuedUploadsPerBackend)
	if aggregate > budget {
		return fmt.Errorf(
			"aggregate upload-queue FD budget exceeded: %d backends × %d max_queued_uploads = %d worst-case open readers, over %d (%.0f%% of soft RLIMIT_NOFILE %d); lower max_queued_uploads (it applies PER backend) or raise LimitNOFILE",
			backendCount, maxQueuedUploadsPerBackend, aggregate,
			budget, aggregateUploadFDBudgetFraction*100, rlimit.Cur)
	}
	return nil
}

func getTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	config := &tls.Config{}
	if certFile != "" && keyFile != "" {
		readCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}

		config.Certificates = []tls.Certificate{readCert}
	}
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		if added := caCertPool.AppendCertsFromPEM(caCert); !added {
			return nil, fmt.Errorf("failed to add CA cert to cert pool")
		}
		config.RootCAs = caCertPool
	}
	return config, nil
}

func (c *Config) setProxy() error {
	if c.GoogleCloudStorage != nil {
		proxyCache, err := gcsproxy.New(c.GoogleCloudStorage.Bucket,
			c.GoogleCloudStorage.UseDefaultCredentials, c.GoogleCloudStorage.JSONCredentialsFile,
			c.StorageMode, c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads)
		if err != nil {
			return err
		}

		c.ProxyBackend = proxyCache
		return nil
	}

	if c.GRPCBackend != nil {
		var opts []grpc.DialOption
		if c.GRPCBackend.BaseURL.Scheme == "grpcs" {
			config, err := getTLSConfig(c.GRPCBackend.CertFile, c.GRPCBackend.KeyFile, c.GRPCBackend.CaFile)
			if err != nil {
				return err
			}
			creds := credentials.NewTLS(config)
			opts = append(opts, grpc.WithTransportCredentials(creds))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		if password, ok := c.GRPCBackend.BaseURL.User.Password(); ok {
			username := c.GRPCBackend.BaseURL.User.Username()
			auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
			header := fmt.Sprintf("Basic %s", auth)
			unaryAuth := func(ctx context.Context, method string, req, res interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return invoker(metadata.AppendToOutgoingContext(ctx, "Authorization", header), method, req, res, cc, opts...)
			}
			streamAuth := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return streamer(metadata.AppendToOutgoingContext(ctx, "Authorization", header), desc, cc, method, opts...)
			}
			opts = append(opts, grpc.WithChainUnaryInterceptor(unaryAuth), grpc.WithStreamInterceptor(streamAuth))
		}

		metrics := grpc_prometheus.NewClientMetrics(func(o *prom.CounterOpts) { o.Namespace = "proxy" })
		metrics.EnableClientHandlingTimeHistogram(func(o *prom.HistogramOpts) { o.Namespace = "proxy" })
		err := prom.Register(metrics)
		if err != nil {
			return err
		}
		opts = append(opts, grpc.WithChainStreamInterceptor(metrics.StreamClientInterceptor()))
		opts = append(opts, grpc.WithChainUnaryInterceptor(metrics.UnaryClientInterceptor()))

		conn, err := grpc.NewClient(c.GRPCBackend.BaseURL.Host, opts...)
		if err != nil {
			return err
		}
		clients := grpcproxy.NewGrpcClients(conn)
		err = clients.CheckCapabilities(context.Background(), c.StorageMode == "zstd")
		if err != nil {
			return err
		}
		proxy := grpcproxy.New(clients, c.StorageMode,
			c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads)

		c.ProxyBackend = proxy
	}

	if c.HTTPBackend != nil {
		httpClient := &http.Client{}
		if c.HTTPBackend.BaseURL.Scheme == "https" {
			config, err := getTLSConfig(c.HTTPBackend.CertFile, c.HTTPBackend.KeyFile, c.HTTPBackend.CaFile)
			if err != nil {
				return err
			}
			tr := &http.Transport{TLSClientConfig: config}
			httpClient.Transport = tr
		}

		proxyCache, err := httpproxy.New(c.HTTPBackend.BaseURL, c.StorageMode,
			httpClient, c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads)
		if err != nil {
			return err
		}

		c.ProxyBackend = proxyCache
		return nil
	}

	if c.S3CloudStorage != nil {
		// Multi-backend mode: an allowlisted selector → backend map, one
		// s3proxy backend (own minio client, transport, upload queue) per
		// entry, routed per-request from the validated gRPC metadata
		// selector on the context.
		if len(c.S3CloudStorage.Backends) > 0 {
			specs, err := c.S3CloudStorage.backendSpecs()
			if err != nil {
				return err
			}
			// Upload pools are per backend; resolve the (lower) multi-backend
			// defaults unless explicitly overridden at the top level.
			numUploaders, maxQueuedUploads := c.perBackendUploadLimits()
			// The queue limits are PER BACKEND and every queued upload holds
			// an open file descriptor (the reader is opened before enqueue),
			// so the process-wide worst case is len(backends) × queue size.
			// Assert that aggregate against the process's actual FD budget at
			// startup — a config that could exhaust NOFILE under write
			// pressure must fail the converge, not the serving path later.
			if err := assertAggregateUploadFDBudget(len(specs), maxQueuedUploads); err != nil {
				return err
			}
			proxy, err := s3proxy.NewMulti(
				specs,
				c.S3CloudStorage.UpdateTimestamps,
				c.S3CloudStorage.ConnRecycleInterval,
				c.StorageMode, c.AccessLogger, c.ErrorLogger, numUploaders, maxQueuedUploads,
				s3proxy.PrometheusMetrics())
			if err != nil {
				return err
			}
			c.ProxyBackend = proxy
			return nil
		}

		creds, err := c.S3CloudStorage.GetCredentials()
		if err != nil {
			return err
		}

		bucketLookupType, err := parseBucketLookupType(c.S3CloudStorage.BucketLookupType)
		if err != nil {
			return err
		}
		// Standalone deployments (e.g. an L1 node) have no embedder-provided
		// metrics sink; export the prefix-safety signal via this package's
		// own Prometheus counters so it is never dark.
		c.ProxyBackend = s3proxy.New(
			c.S3CloudStorage.Endpoint,
			c.S3CloudStorage.Bucket,
			bucketLookupType,
			c.S3CloudStorage.Prefix,
			creds,
			c.S3CloudStorage.DisableSSL,
			c.S3CloudStorage.UpdateTimestamps,
			c.S3CloudStorage.Region,
			c.S3CloudStorage.MaxIdleConns,
			c.S3CloudStorage.ConnRecycleInterval,
			c.StorageMode, c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads,
			s3proxy.PrometheusMetrics())
		return nil
	}

	if c.AzBlobConfig != nil {
		creds, err := c.AzBlobConfig.GetCredentials()
		if err != nil {
			return err
		}

		c.ProxyBackend = azblobproxy.New(
			c.AzBlobConfig.StorageAccount,
			c.AzBlobConfig.ContainerName,
			c.AzBlobConfig.Prefix,
			creds,
			c.AzBlobConfig.SharedKey,
			c.AzBlobConfig.UpdateTimestamps,
			c.StorageMode, c.AccessLogger, c.ErrorLogger, c.NumUploaders, c.MaxQueuedUploads,
		)
		return nil
	}

	return nil
}

func parseBucketLookupType(typeStr string) (minio.BucketLookupType, error) {
	valMap := map[string]minio.BucketLookupType{
		"auto": minio.BucketLookupAuto,
		"dns":  minio.BucketLookupDNS,
		"path": minio.BucketLookupPath,
	}

	val, ok := valMap[typeStr]
	if !ok {
		return 0, fmt.Errorf("unsupported bucket_lookup_type value : %s", typeStr)
	}

	return val, nil
}
