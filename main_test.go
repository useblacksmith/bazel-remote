package main

import (
	"strings"
	"testing"

	"github.com/buchgr/bazel-remote/v2/config"
)

func multiBackendConfig() *config.Config {
	return &config.Config{
		S3CloudStorage: &config.S3CloudStorageConfig{
			Backends: map[string]config.S3BackendConfig{
				"http://minio-a.example.com:9000": {Default: true},
			},
		},
	}
}

func TestValidateMultiBackendTrust(t *testing.T) {
	testCases := []struct {
		name               string
		c                  *config.Config
		trustStoragePrefix bool
		authSecret         string
		wantErr            bool
	}{
		{"backends map with both toggles", multiBackendConfig(), true, "hunter2", false},
		{"backends map without prefix trust", multiBackendConfig(), false, "hunter2", true},
		{"backends map without auth secret", multiBackendConfig(), true, "", true},
		{"backends map with neither toggle", multiBackendConfig(), false, "", true},
		{"no s3 config needs no toggles", &config.Config{}, false, "", false},
		{"single-backend s3 needs no toggles",
			&config.Config{S3CloudStorage: &config.S3CloudStorageConfig{}}, false, "", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMultiBackendTrust(tc.c, tc.trustStoragePrefix, tc.authSecret)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateMultiBackendTrust = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil {
				return
			}
			// The refusal must name all three toggles so the operator can
			// fix the combination without reading source.
			for _, needle := range []string{
				"backends",
				"BAZEL_REMOTE_TRUST_STORAGE_PREFIX_HEADER",
				"BAZEL_REMOTE_L1_AUTH_SECRET",
			} {
				if !strings.Contains(err.Error(), needle) {
					t.Fatalf("error %q does not mention %q", err.Error(), needle)
				}
			}
		})
	}
}
