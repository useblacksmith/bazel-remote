// Package httpproxy is a cache implementation that can proxy artifacts
// from/to another HTTP-based remote cache.
package httpproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type remoteHTTPProxyCache struct {
	remote       *http.Client
	baseURL      string
	accessLogger cache.Logger
	errorLogger  cache.Logger
	requestURL   func(hash string, kind cache.EntryKind) string
	v2mode       bool
}

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_http_cache_hits",
		Help: "The total number of HTTP backend cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_http_cache_misses",
		Help: "The total number of HTTP backend cache misses",
	})
)

// New creates a cache that proxies requests to a HTTP remote cache.
// `storageMode` must be one of "uncompressed" (which expects legacy
// CAS blobs) or "zstd" (which expects cas.v2 blobs).
func New(baseURL *url.URL, storageMode string, remote *http.Client,
	accessLogger cache.Logger, errorLogger cache.Logger,
) (cache.Proxy, error) {

	proxy := &remoteHTTPProxyCache{
		remote:       remote,
		baseURL:      strings.TrimRight(baseURL.String(), "/"),
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
		v2mode:       storageMode == "zstd",
	}

	switch storageMode {
	case "zstd":
		proxy.requestURL = func(hash string, kind cache.EntryKind) string {
			if kind == cache.CAS {
				return fmt.Sprintf("%s/cas.v2/%s", proxy.baseURL, hash)
			}

			return fmt.Sprintf("%s/%s/%s", proxy.baseURL, kind, hash)
		}
	case "uncompressed":
		proxy.requestURL = func(hash string, kind cache.EntryKind) string {
			return fmt.Sprintf("%s/%s/%s", proxy.baseURL, kind, hash)
		}
	default:
		return nil, fmt.Errorf("invalid http_proxy.mode specified: %q", storageMode)
	}

	return proxy, nil
}

// Helper function for logging responses
func logResponse(logger cache.Logger, method string, code int, url string) {
	logger.Printf("HTTP %s %d %s", method, code, url)
}

func (r *remoteHTTPProxyCache) Get(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (io.ReadCloser, int64, error) {
	url := r.requestURL(hash, kind)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cacheMisses.Inc()
		return nil, -1, err
	}

	rsp, err := r.remote.Do(req)
	if err != nil {
		cacheMisses.Inc()
		return nil, -1, err
	}

	logResponse(r.accessLogger, "DOWNLOAD", rsp.StatusCode, url)

	if rsp.StatusCode == http.StatusNotFound {
		cacheMisses.Inc()
		return nil, -1, nil
	}

	if rsp.StatusCode != http.StatusOK {
		// If the failed http response contains some data then
		// forward up to 1 KiB.
		var errorBytes []byte
		errorBytes, err = io.ReadAll(io.LimitReader(rsp.Body, 1024))
		var errorText string
		if err == nil {
			errorText = string(errorBytes)
		}

		cacheMisses.Inc()
		return nil, -1, &cache.Error{
			Code: rsp.StatusCode,
			Text: errorText,
		}
	}

	if kind == cache.CAS && r.v2mode {
		cacheHits.Inc()
		return casblob.ExtractLogicalSize(rsp.Body)
	}

	sizeBytesStr := rsp.Header.Get("Content-Length")
	if sizeBytesStr == "" {
		err = errors.New("missing Content-Length header")
		cacheMisses.Inc()
		return nil, -1, err
	}

	sizeBytesInt, err := strconv.Atoi(sizeBytesStr)
	if err != nil {
		cacheMisses.Inc()
		return nil, -1, err
	}
	sizeBytes := int64(sizeBytesInt)

	cacheHits.Inc()

	return rsp.Body, sizeBytes, nil
}

func (r *remoteHTTPProxyCache) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {

	url := r.requestURL(hash, kind)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, -1
	}

	rsp, err := r.remote.Do(req)
	if err == nil && rsp.StatusCode == http.StatusOK {
		if kind != cache.CAS || !r.v2mode {
			return true, rsp.ContentLength
		}

		// We don't know the content size without reading the file header
		// and that could be very costly for the backend server. So return
		// "unknown size".
		return true, -1
	}

	return false, -1
}
