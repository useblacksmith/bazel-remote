package cache

// Trust-interceptor rejection marker: the wire contract that lets the
// upstream grpcproxy client distinguish rejections minted by our own L1
// trust interceptors (config races: allowlist drift, FA/L1 version skew)
// from genuine application errors (customer-caused validation failures,
// server bugs).
//
// The L1 interceptors attach a google.rpc.ErrorInfo detail with
// TrustRejectionErrorDomain to every rejection they mint. The client
// degrades a backend error to a metered cache miss ONLY when this marker is
// present; all unmarked InvalidArgument/Internal/Unknown errors keep failing
// strictly. This is deliberately narrow — matching on the marker rather than
// on error classes means a config-race rejection can never be confused with
// a real validation error, in either direction.
//
// Auth rejections (Unauthenticated) carry no marker: they are already
// degraded via their own code-based check and counter.
const TrustRejectionErrorDomain = "cache.blacksmith.sh"

// ErrorInfo.Reason values for TrustRejectionErrorDomain. The finer-grained
// cause (missing/duplicate/unknown/invalid) travels in ErrorInfo.Metadata
// under "cause".
const (
	// RejectionReasonS3BackendSelector marks rejections from the S3
	// backend-selector trust interceptor (missing, duplicate, or
	// non-allowlisted x-blacksmith-s3-endpoint metadata).
	RejectionReasonS3BackendSelector = "S3_BACKEND_SELECTOR_REJECTED"

	// RejectionReasonStoragePrefix marks rejections from the storage-prefix
	// trust interceptor (missing, duplicate, or invalid
	// x-blacksmith-storage-prefix metadata).
	RejectionReasonStoragePrefix = "STORAGE_PREFIX_REJECTED"
)
