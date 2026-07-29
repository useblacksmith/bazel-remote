package config

import (
	"strings"
	"testing"
	"time"
)

// A complete multi-backend config: two allowlisted backends keyed by the
// tenant-facing endpoint URL, secrets inherited from the top-level fields
// where unset.
const multiBackendYaml = `host: localhost
port: 8080
dir: /opt/cache-dir
max_size: 100
s3_proxy:
  auth_method: access_key
  access_key_id: SHARED_ACCESS_KEY
  secret_access_key: SHARED_SECRET_KEY
  bucket: shared-bucket
  conn_recycle_interval: 1m
  backends:
    http://minio-a.example.com:9000:
      default: true
    https://minio-b.example.com:9000:
      endpoint: 10.4.6.99:9000
      disable_ssl: true
      bucket: bucket-b
      extra_buckets: [bucket-b-pre-rename]
      access_key_id: B_ACCESS_KEY
      secret_access_key: B_SECRET_KEY
`

func TestS3BackendsMapConfig(t *testing.T) {
	config, err := NewFromYaml([]byte(multiBackendYaml))
	if err != nil {
		t.Fatal(err)
	}

	s3 := config.S3CloudStorage
	if s3 == nil {
		t.Fatal("expected S3CloudStorage config")
	}
	if len(s3.Backends) != 2 {
		t.Fatalf("expected 2 backends, got %d", len(s3.Backends))
	}
	if s3.ConnRecycleInterval != time.Minute {
		t.Fatalf("ConnRecycleInterval = %v, want 1m", s3.ConnRecycleInterval)
	}

	// The allowlist pairs each selector with its bucket set: the entry's
	// (possibly inherited) default bucket plus any extra_buckets.
	allowed, err := s3.AllowedBackends()
	if err != nil {
		t.Fatal(err)
	}
	wantBuckets := map[string][]string{
		"http://minio-a.example.com:9000":  {"shared-bucket"},
		"https://minio-b.example.com:9000": {"bucket-b", "bucket-b-pre-rename"},
	}
	for key, buckets := range wantBuckets {
		got, ok := allowed[key]
		if !ok {
			t.Fatalf("expected %q in allowed backends %v", key, allowed)
		}
		if len(got) != len(buckets) {
			t.Fatalf("backend %q allowed buckets = %v, want %v", key, got, buckets)
		}
		for _, bucket := range buckets {
			if !got[bucket] {
				t.Fatalf("backend %q allowed buckets = %v, missing %q", key, got, bucket)
			}
		}
	}

	specs, err := s3.backendSpecs()
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]bool{}
	for _, spec := range specs {
		byKey[spec.Key] = true
		switch spec.Key {
		case "http://minio-a.example.com:9000":
			// Endpoint derived from the key URL; http scheme implies
			// disable_ssl; bucket and creds inherited from the top level.
			if spec.Endpoint != "minio-a.example.com:9000" {
				t.Errorf("backend a endpoint = %q", spec.Endpoint)
			}
			if !spec.DisableSSL {
				t.Error("backend a should have SSL disabled (http key)")
			}
			if spec.Bucket != "shared-bucket" {
				t.Errorf("backend a bucket = %q, want inherited shared-bucket", spec.Bucket)
			}
			if !spec.Default {
				t.Error("backend a should be the default")
			}
		case "https://minio-b.example.com:9000":
			// Explicit dial endpoint overrides the key URL (private-VLAN
			// dialing), with its own creds, bucket and ssl setting.
			if spec.Endpoint != "10.4.6.99:9000" {
				t.Errorf("backend b endpoint = %q", spec.Endpoint)
			}
			if !spec.DisableSSL {
				t.Error("backend b should have SSL disabled (explicit)")
			}
			if spec.Bucket != "bucket-b" {
				t.Errorf("backend b bucket = %q", spec.Bucket)
			}
			if spec.Default {
				t.Error("backend b should not be the default")
			}
		default:
			t.Errorf("unexpected backend key %q", spec.Key)
		}
		if spec.Credentials == nil {
			t.Errorf("backend %q has no credentials", spec.Key)
		}
	}
	if len(byKey) != 2 {
		t.Fatalf("expected specs for 2 distinct keys, got %v", byKey)
	}
}

func TestConnRecycleIntervalResolvedAtConfigLoad(t *testing.T) {
	base := `host: localhost
port: 8080
dir: /opt/cache-dir
max_size: 100
s3_proxy:
  endpoint: minio.example.com:9000
  bucket: test-bucket
  auth_method: access_key
  access_key_id: EXAMPLE_ACCESS_KEY
  secret_access_key: EXAMPLE_SECRET_KEY
`

	t.Run("unset resolves to the default", func(t *testing.T) {
		config, err := NewFromYaml([]byte(base))
		if err != nil {
			t.Fatal(err)
		}
		if config.S3CloudStorage.ConnRecycleInterval != defaultConnRecycleInterval {
			t.Fatalf("ConnRecycleInterval = %v, want default %v",
				config.S3CloudStorage.ConnRecycleInterval, defaultConnRecycleInterval)
		}
	})

	t.Run("negative disables and is preserved", func(t *testing.T) {
		config, err := NewFromYaml([]byte(base + "  conn_recycle_interval: -1s\n"))
		if err != nil {
			t.Fatal(err)
		}
		if config.S3CloudStorage.ConnRecycleInterval != -time.Second {
			t.Fatalf("ConnRecycleInterval = %v, want -1s", config.S3CloudStorage.ConnRecycleInterval)
		}
	})
}

// Multi-backend mode lowers the per-backend upload-pool defaults (queues
// preallocate per backend); explicit top-level settings are inherited
// per backend unchanged.
func TestPerBackendUploadLimits(t *testing.T) {
	defaults := &Config{NumUploaders: defaultNumUploaders, MaxQueuedUploads: defaultMaxQueuedUploads}
	numUploaders, maxQueued := defaults.perBackendUploadLimits()
	if numUploaders != multiBackendNumUploaders || maxQueued != multiBackendMaxQueuedUploads {
		t.Fatalf("default limits resolved to (%d, %d), want (%d, %d)",
			numUploaders, maxQueued, multiBackendNumUploaders, multiBackendMaxQueuedUploads)
	}

	explicit := &Config{NumUploaders: 40, MaxQueuedUploads: 250000}
	numUploaders, maxQueued = explicit.perBackendUploadLimits()
	if numUploaders != 40 || maxQueued != 250000 {
		t.Fatalf("explicit limits resolved to (%d, %d), want (40, 250000)", numUploaders, maxQueued)
	}
}

func TestS3BackendsWithoutMapIsBackwardCompatible(t *testing.T) {
	yaml := `host: localhost
port: 8080
dir: /opt/cache-dir
max_size: 100
s3_proxy:
  endpoint: minio.example.com:9000
  bucket: test-bucket
  auth_method: access_key
  access_key_id: EXAMPLE_ACCESS_KEY
  secret_access_key: EXAMPLE_SECRET_KEY
`
	config, err := NewFromYaml([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if len(config.S3CloudStorage.Backends) != 0 {
		t.Fatalf("expected no backends map, got %v", config.S3CloudStorage.Backends)
	}
}

func TestS3BackendsValidation(t *testing.T) {
	base := `host: localhost
port: 8080
dir: /opt/cache-dir
max_size: 100
s3_proxy:
  auth_method: access_key
  access_key_id: SHARED_ACCESS_KEY
  secret_access_key: SHARED_SECRET_KEY
  bucket: shared-bucket
  backends:
`
	testCases := []struct {
		name     string
		backends string
		wantErr  string
	}{
		{
			name: "no default backend",
			backends: `    http://minio-a.example.com:9000: {}
    http://minio-b.example.com:9000: {}
`,
			wantErr: "exactly one entry with 'default: true'",
		},
		{
			name: "two default backends",
			backends: `    http://minio-a.example.com:9000:
      default: true
    http://minio-b.example.com:9000:
      default: true
`,
			wantErr: "exactly one entry with 'default: true'",
		},
		{
			name: "non-URL key without explicit endpoint",
			backends: `    minio-a.example.com:9000:
      default: true
`,
			wantErr: "is not an http(s) URL",
		},
		{
			name: "extra bucket duplicating the default bucket",
			backends: `    http://minio-a.example.com:9000:
      default: true
      bucket: bucket-a
      extra_buckets: [bucket-a]
`,
			wantErr: "lists bucket \"bucket-a\" more than once",
		},
		{
			name: "extra bucket duplicating the inherited default bucket",
			backends: `    http://minio-a.example.com:9000:
      default: true
      extra_buckets: [shared-bucket]
`,
			wantErr: "lists bucket \"shared-bucket\" more than once",
		},
		{
			name: "duplicate within extra_buckets",
			backends: `    http://minio-a.example.com:9000:
      default: true
      extra_buckets: [bucket-x, bucket-x]
`,
			wantErr: "lists bucket \"bucket-x\" more than once",
		},
		{
			name: "empty extra_buckets value",
			backends: `    http://minio-a.example.com:9000:
      default: true
      extra_buckets: [""]
`,
			wantErr: "empty extra_buckets value",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFromYaml([]byte(base + tc.backends))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestS3BackendsMissingBucketRejected(t *testing.T) {
	yaml := `host: localhost
port: 8080
dir: /opt/cache-dir
max_size: 100
s3_proxy:
  auth_method: access_key
  access_key_id: SHARED_ACCESS_KEY
  secret_access_key: SHARED_SECRET_KEY
  backends:
    http://minio-a.example.com:9000:
      default: true
`
	_, err := NewFromYaml([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "has no bucket") {
		t.Fatalf("expected 'has no bucket' error, got %v", err)
	}
}
