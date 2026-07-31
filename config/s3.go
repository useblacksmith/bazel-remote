package config

import (
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache/s3proxy"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3CloudStorageConfig stores the configuration of an S3 API proxy backend.
type S3CloudStorageConfig struct {
	Endpoint                 string `yaml:"endpoint"`
	Bucket                   string `yaml:"bucket"`
	Prefix                   string `yaml:"prefix"`
	AuthMethod               string `yaml:"auth_method"`
	AccessKeyID              string `yaml:"access_key_id"`
	SecretAccessKey          string `yaml:"secret_access_key"`
	SessionToken             string `yaml:"session_token"`
	SignatureType            string `yaml:"signature_type"`
	DisableSSL               bool   `yaml:"disable_ssl"`
	UpdateTimestamps         bool   `yaml:"update_timestamps"`
	IAMRoleEndpoint          string `yaml:"iam_role_endpoint"`
	Region                   string `yaml:"region"`
	KeyVersion               *int   `yaml:"key_version"`
	AWSProfile               string `yaml:"aws_profile"`
	AWSSharedCredentialsFile string `yaml:"aws_shared_credentials_file"`
	BucketLookupType         string `yaml:"bucket_lookup_type"`
	MaxIdleConns             int    `yaml:"max_idle_conns"`

	// ConnRecycleInterval periodically closes the backend transport's idle
	// connections so new dials re-resolve DNS. MinIO clusters have no load
	// balancer — the endpoint is a DNS name round-robinning bare node IPs —
	// so a long-lived proxy that never re-dials pins its traffic to whichever
	// nodes it happened to connect to first. YAML-only (no flag/env surface),
	// as a duration string ("90s", "5m"); see UnmarshalYAML. Unset or zero
	// resolves to defaultConnRecycleInterval at config load; negative
	// disables recycling. The s3proxy receives the resolved, concrete value.
	ConnRecycleInterval time.Duration `yaml:"-"`

	// ReadTimeout overrides the s3proxy's overall per-call read deadline
	// (miss fall-through GetObject including its streamed body, Contains
	// StatObject). YAML-only, as a duration string ("60s", "5m"); see
	// UnmarshalYAML. Unset or zero keeps the s3proxy default (5m).
	// Connection establishment (dial / TLS / response headers) is bounded
	// separately and much tighter by the s3proxy's constant connectTimeout —
	// this knob only governs how long a healthy, answering backend may take
	// to finish a read, so raising it does not slow down failure detection
	// on a dead one.
	ReadTimeout time.Duration `yaml:"-"`

	// Backends optionally declares a map of allowlisted S3 backends for
	// multi-shard deployments (an L1 node in front of several MinIO
	// clusters). Keys are the tenant-facing endpoint selectors — the exact
	// `bazelre_cache_endpoint` values the trusted upstream forwards as
	// cache.S3BackendGRPCMetadataKey gRPC metadata, e.g.
	// "http://staging-minio.uswest.blacksmith.sh:9000" — matched as opaque
	// strings, no URL normalization. Each entry also carries an allowed
	// bucket set (its default bucket plus extra_buckets, see
	// S3BackendConfig) against which the forwarded
	// cache.S3BucketGRPCMetadataKey value is validated. Exactly one entry
	// must set `default: true`; it serves requests that carry no selector
	// (HTTP API paths, RAW entries), in its default bucket. When this map is
	// empty the proxy behaves exactly as before: one backend from the fields
	// above, selector and bucket metadata ignored.
	Backends map[string]S3BackendConfig `yaml:"backends,omitempty"`
}

// UnmarshalYAML decodes S3CloudStorageConfig with conn_recycle_interval
// accepted as a duration string ("90s", "5m", "-1s" to disable) — yaml.v3
// cannot decode time.Duration natively.
func (s3c *S3CloudStorageConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type Aux S3CloudStorageConfig
	var aux struct {
		Aux                 `yaml:",inline"`
		ConnRecycleInterval string `yaml:"conn_recycle_interval"`
		ReadTimeout         string `yaml:"read_timeout"`
	}

	if err := unmarshal(&aux); err != nil {
		return err
	}
	*s3c = S3CloudStorageConfig(aux.Aux)
	if aux.ConnRecycleInterval != "" {
		d, err := time.ParseDuration(aux.ConnRecycleInterval)
		if err != nil {
			return fmt.Errorf("invalid s3_proxy conn_recycle_interval %q: %w", aux.ConnRecycleInterval, err)
		}
		s3c.ConnRecycleInterval = d
	}
	if aux.ReadTimeout != "" {
		d, err := time.ParseDuration(aux.ReadTimeout)
		if err != nil {
			return fmt.Errorf("invalid s3_proxy read_timeout %q: %w", aux.ReadTimeout, err)
		}
		if d <= 0 {
			return fmt.Errorf("s3_proxy read_timeout must be positive, got %q", aux.ReadTimeout)
		}
		s3c.ReadTimeout = d
	}
	return nil
}

// defaultConnRecycleInterval is applied at config load when
// conn_recycle_interval is unset. Modest by design: dials to the LAN-local
// MinIO endpoints are cheap, and frequent re-dials are what rotate traffic
// across the DNS round-robin.
const defaultConnRecycleInterval = 5 * time.Minute

// S3BackendConfig describes one entry of the allowlisted backends map. Every
// field is optional and inherits from the surrounding S3CloudStorageConfig
// when unset; an unset endpoint is derived from the map key's URL (host:port,
// with disable_ssl implied by an http scheme), which covers the common case
// where the tenant-facing selector is also the address this node dials. Set
// `endpoint` explicitly when the dial address differs (e.g. the L1 reaches
// MinIO over a private VLAN address while tenants are pinned to the public
// DNS name).
//
// The entry's resolved bucket (own `bucket` or the inherited top-level one)
// is its DEFAULT bucket: it serves selector-less default-backend traffic
// (the firewall-gated HTTP side door) and is always in the entry's allowed
// bucket set. extra_buckets extends that set for tenants whose namespaces
// were allocated before a bucket rename — web snapshots `bazelre_cache_bucket`
// per namespace at allocation, so one endpoint can legitimately serve several
// buckets. The gRPC trust interceptor accepts a forwarded (endpoint, bucket)
// pair only when the bucket is in this set.
type S3BackendConfig struct {
	Endpoint        string   `yaml:"endpoint"`
	Bucket          string   `yaml:"bucket"`
	ExtraBuckets    []string `yaml:"extra_buckets"`
	Prefix          string   `yaml:"prefix"`
	AccessKeyID     string   `yaml:"access_key_id"`
	SecretAccessKey string   `yaml:"secret_access_key"`
	DisableSSL      *bool    `yaml:"disable_ssl"`
	Region          string   `yaml:"region"`
	MaxIdleConns    int      `yaml:"max_idle_conns"`
	Default         bool     `yaml:"default"`
}

// AllowedBackends returns, for each valid backend selector, the bucket set
// the fail-closed gRPC interceptor accepts for it: the entry's default
// bucket plus its extra_buckets. Only valid to call after validateConfig has
// passed (bucket resolution cannot fail then).
func (s3c *S3CloudStorageConfig) AllowedBackends() (map[string]map[string]bool, error) {
	allowed := make(map[string]map[string]bool, len(s3c.Backends))
	for key := range s3c.Backends {
		buckets, err := s3c.allowedBucketsForBackend(key)
		if err != nil {
			return nil, err
		}
		allowed[key] = buckets
	}
	return allowed, nil
}

// allowedBucketsForBackend resolves one backends-map entry's allowed bucket
// set: the resolved default bucket (the entry's own, or the inherited
// top-level one) plus extra_buckets. The default must be non-empty and no
// bucket may repeat within the entry — a duplicate is always a config typo,
// and catching it loudly beats silently deduplicating.
func (s3c *S3CloudStorageConfig) allowedBucketsForBackend(key string) (map[string]bool, error) {
	backend := s3c.Backends[key]
	def := backend.Bucket
	if def == "" {
		def = s3c.Bucket
	}
	if def == "" {
		return nil, fmt.Errorf("s3.backends entry %q has no bucket", key)
	}
	buckets := map[string]bool{def: true}
	for _, bucket := range backend.ExtraBuckets {
		if bucket == "" {
			return nil, fmt.Errorf("s3.backends entry %q has an empty extra_buckets value", key)
		}
		if buckets[bucket] {
			return nil, fmt.Errorf("s3.backends entry %q lists bucket %q more than once across 'bucket' and 'extra_buckets'", key, bucket)
		}
		buckets[bucket] = true
	}
	return buckets, nil
}

// mergedBackendConfig resolves one backends-map entry into a complete
// single-backend config, inheriting unset fields from the top-level config.
func (s3c *S3CloudStorageConfig) mergedBackendConfig(key string) (S3CloudStorageConfig, error) {
	backend := s3c.Backends[key]

	merged := *s3c
	merged.Backends = nil

	if backend.Endpoint != "" {
		merged.Endpoint = backend.Endpoint
		if backend.DisableSSL != nil {
			merged.DisableSSL = *backend.DisableSSL
		}
	} else {
		// Derive the dial address from the selector key itself.
		u, err := url.Parse(key)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return merged, fmt.Errorf("s3.backends key %q is not an http(s) URL and the entry sets no 'endpoint'", key)
		}
		merged.Endpoint = u.Host
		if backend.DisableSSL != nil {
			merged.DisableSSL = *backend.DisableSSL
		} else {
			merged.DisableSSL = u.Scheme == "http"
		}
	}

	if backend.Bucket != "" {
		merged.Bucket = backend.Bucket
	}
	if backend.Prefix != "" {
		merged.Prefix = backend.Prefix
	}
	if backend.Region != "" {
		merged.Region = backend.Region
	}
	if backend.MaxIdleConns != 0 {
		merged.MaxIdleConns = backend.MaxIdleConns
	}
	if backend.AccessKeyID != "" || backend.SecretAccessKey != "" {
		merged.AuthMethod = s3proxy.AuthMethodAccessKey
		merged.AccessKeyID = backend.AccessKeyID
		merged.SecretAccessKey = backend.SecretAccessKey
		merged.SessionToken = ""
	}

	if merged.Endpoint == "" {
		return merged, fmt.Errorf("s3.backends entry %q has no endpoint", key)
	}
	if merged.Bucket == "" {
		return merged, fmt.Errorf("s3.backends entry %q has no bucket", key)
	}

	return merged, nil
}

// backendSpecs resolves the backends map into s3proxy backend specs
// (credentials included). Only valid to call when len(Backends) > 0 and
// validateConfig has passed.
func (s3c *S3CloudStorageConfig) backendSpecs() ([]s3proxy.BackendSpec, error) {
	specs := make([]s3proxy.BackendSpec, 0, len(s3c.Backends))
	for key, backend := range s3c.Backends {
		merged, err := s3c.mergedBackendConfig(key)
		if err != nil {
			return nil, err
		}
		creds, err := merged.GetCredentials()
		if err != nil {
			return nil, fmt.Errorf("s3.backends entry %q: %w", key, err)
		}
		lookupTypeStr := merged.BucketLookupType
		if lookupTypeStr == "" {
			lookupTypeStr = "auto"
		}
		lookupType, err := parseBucketLookupType(lookupTypeStr)
		if err != nil {
			return nil, fmt.Errorf("s3.backends entry %q: %w", key, err)
		}
		specs = append(specs, s3proxy.BackendSpec{
			Key:              key,
			Endpoint:         merged.Endpoint,
			Bucket:           merged.Bucket,
			BucketLookupType: lookupType,
			Prefix:           merged.Prefix,
			Credentials:      creds,
			DisableSSL:       merged.DisableSSL,
			Region:           merged.Region,
			MaxIdleConns:     merged.MaxIdleConns,
			Default:          backend.Default,
		})
	}
	return specs, nil
}

func (s3c S3CloudStorageConfig) GetCredentials() (*credentials.Credentials, error) {
	switch s3c.AuthMethod {
	case s3proxy.AuthMethodAWSCredentialsFile:
		log.Println("S3 Credentials: using AWS credentials file.")
		return credentials.NewFileAWSCredentials(s3c.AWSSharedCredentialsFile, s3c.AWSProfile), nil
	case s3proxy.AuthMethodAccessKey:
		if s3c.AccessKeyID == "" {
			return nil, fmt.Errorf("missing s3.access_key_id for s3.auth_method = '%s'", s3proxy.AuthMethodAccessKey)
		}
		if s3c.SecretAccessKey == "" {
			return nil, fmt.Errorf("missing s3.secret_access_key for s3.auth_method = '%s'", s3proxy.AuthMethodAccessKey)
		}
		log.Println("S3 Credentials: using access/secret access key.")
		signatureType := parseSignatureType(s3c.SignatureType)
		log.Printf("S3 Sign: using %s sign\n", signatureType.String())
		return credentials.NewStatic(s3c.AccessKeyID, s3c.SecretAccessKey, s3c.SessionToken, signatureType), nil
	case s3proxy.AuthMethodIAMRole:
		// Fall back to getting credentials from IAM
		log.Println("S3 Credentials: using IAM.")
		return credentials.NewIAM(s3c.IAMRoleEndpoint), nil
	}

	return nil, fmt.Errorf("invalid s3.auth_method: %s", s3c.AuthMethod)
}

func parseSignatureType(str string) credentials.SignatureType {
	// str has been verified in config.go/validateConfig, must be one of these keys
	valMap := map[string]credentials.SignatureType{
		"":            credentials.SignatureV4,
		"v2":          credentials.SignatureV2,
		"v4":          credentials.SignatureV4,
		"v4streaming": credentials.SignatureV4Streaming,
		"anonymous":   credentials.SignatureAnonymous,
	}
	return valMap[str]
}
