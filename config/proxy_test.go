package config

import "testing"

// The legacy Go switch and the general list must union rather than override:
// during a roll the two can be set by different generations of the rendered
// env file, and losing either one silently re-attaches that tool to MinIO.
func TestBackendDisconnectTools(t *testing.T) {
	for _, tc := range []struct {
		name  string
		goEnv string
		list  string
		want  []string
	}{
		{name: "unset", want: nil},
		{name: "legacy go switch only", goEnv: "1", want: []string{"go"}},
		{name: "legacy switch off", goEnv: "0", want: nil},
		{name: "list only", list: "turbo", want: []string{"turbo"}},
		{name: "list of several", list: "go,turbo", want: []string{"go", "turbo"}},
		{name: "union of both sources", goEnv: "1", list: "turbo", want: []string{"go", "turbo"}},
		{name: "overlap deduplicates", goEnv: "1", list: "go,turbo", want: []string{"go", "turbo"}},
		{name: "whitespace and empties ignored", list: " go , , turbo ", want: []string{"go", "turbo"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BAZEL_REMOTE_GO_BACKEND_DISCONNECT", tc.goEnv)
			t.Setenv("BAZEL_REMOTE_BACKEND_DISCONNECT_TOOLS", tc.list)
			got := backendDisconnectTools()
			if len(got) != len(tc.want) {
				t.Fatalf("backendDisconnectTools() = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("backendDisconnectTools() = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
