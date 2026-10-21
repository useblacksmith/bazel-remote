# Rotate the L1 shared authentication secret

Use this runbook for the Blacksmith L1 credential in
`BAZEL_REMOTE_L1_AUTH_SECRET`. It does not rotate HTTP Basic Auth, S3 credentials,
or TLS certificates. The production operator sequence is server overlap,
client transition, graceful drain, and finally old-key rejection.

Complete staging before repeating for production. Runner hosts must be rolled
to load the new client credential; keep both keys accepted until outgoing
agents and their jobs have finished.

## What accepts which value

| Location | Steady state | Overlap | Final state |
|---|---|---|---|
| L1 `BAZEL_REMOTE_L1_AUTH_SECRET` | old | `new,old` | new |
| Runner `BAZELRE_CACHE_L1_SECRET` | old | new after **all** L1s accept it | new |
| gRPC `x-blacksmith-cache-auth` | one key | one key | one key |

The server parses the comma-separated configuration into a set at startup.
Requests use a single hash lookup. Spaces around configured entries are trimmed;
empty entries never match. A non-empty configuration containing no usable keys
rejects protected RPCs. Duplicate request metadata and a request containing
`new,old` as one credential are not an overlap mechanism.

The check runs when `BAZEL_REMOTE_TRUST_STORAGE_PREFIX_HEADER=1`. Do not disable
that setting, clear the secret, or set `BAZEL_REMOTE_L1_UNSAFE_NO_AUTH_SECRET=1`
to get through a rotation. Health and capabilities RPCs are exempt; their
success is not authentication evidence. An env-file edit does not reload the
accepted set until the process restarts.

## 1. Prepare an immutable target and a rollback

1. Start in staging. Record environment, owner, operation ID, current server
   binary SHA256/source, target SHA/tag, and the live endpoint set.
2. Fetch the service repository. Operational Blacksmith code and release tags
   are based on **`patchset`**, not the upstream-style default branch `main`.
   Confirm the target contains the multiple-key implementation (PR #59).
3. Prefer the approved published release whose artifact matches the required
   architecture. Check the release assets before stopping a node. Do not
   combine this operation with an unreviewed backend/cache behavior change.
4. In fa, fetch current `origin/main`, read its instructions, and use its current
   Ansible role and fleet tooling. Record tooling SHA independently of the
   service release. The L1 deployment pin and the agent's fork dependency need
   the consistency review described in the fa role README.
5. Save a private backup of each node's binary, `/etc/bazel-remote/env`, relevant
   config/unit, and the encrypted vault source. Record hashes and file modes.
   Protect backups containing credentials; do not attach them to the incident.

Reconcile live etcd membership under `/services/bazel-l1/nodes/<environment>/`
with fa's `infra/ansible/{staging,production}_bazel_l1_<region>.ini` files.
Include every region and both architectures. Inventory identity, SSH address,
and the public gRPC endpoint may differ. Probe the actual endpoint clients use.
Never assume an inventory alone is the full serving set.

## 2. Prepare keys and the persistent server configuration

Run the examples in Bash. Create a private working directory, and reuse it for
retries so the replacement key stays the same:

```bash
set +x
umask 077
ROTATION_DIR=$(mktemp -d "${TMPDIR:-/tmp}/credential-rotation.XXXXXX")
export ROTATION_DIR
```

Generate an independent cryptographically random new key. Save it as
`$ROTATION_DIR/bazel.new` and preserve the current key as `$ROTATION_DIR/bazel.old`.
Compare the old key with the runner Doppler values and the live server
configuration without printing it. Unexpected variation is a reason to map
consumers before proceeding.

Save only the raw credential values in `bazel.old` and `bazel.new` inside
that directory, with mode `0600`. Keep these files out of Git and terminal logs.

fa owns the persistent server value:

| Environment | File | YAML variable |
|---|---|---|
| staging | `infra/ansible/secrets/bazel_l1.yml` | `bazel_l1_auth_secret_stg` |
| production | same file | `bazel_l1_auth_secret_prd` |

This file contains individual `!vault` values; it is not one whole encrypted
document. Use `ansible-vault encrypt_string` to create the replacement block.
Example, in Bash on the approved operator machine:

```bash
set +x
umask 077
export AUTH_VAR=bazel_l1_auth_secret_stg  # choose prd only for production
python3 - <<'PY' | ansible-vault encrypt_string \
  --vault-password-file '<vault-password-file>' --stdin-name "$AUTH_VAR" \
  > "$ROTATION_DIR/bazel-overlap.vault.yml"
import os
from pathlib import Path
w = Path(os.environ['ROTATION_DIR'])
print((w/'bazel.new').read_text().strip()+','+(w/'bazel.old').read_text().strip(), end='')
PY
```

Replace **only that variable's encrypted block** in the vault file. Verify by
private decryption that it contains exactly the intended accepted keys and
that the other environment/TLS/KMS values are unchanged. Review and commit the
encrypted source so future configuration convergence preserves overlap. Do not
paste the plaintext list into a PR or an Ansible `-e` command argument.

## 3. Roll a canary, then each L1 cache node

The commands below run from fa's `infra/ansible` directory. Fill in a reviewed
inventory, inventory group, public-IP inventory identity, and approved release.
The group is explicit because the playbook defaults to the us-west group.

```bash
ansible-playbook -i '<environment>_bazel_l1_<region>.ini' bazel_l1_roll.yaml \
  -e bazel_l1_target_group='<inventory-group>' \
  -e release_tag='<approved-vX.Y.Z-blacksmith.N>' \
  --limit '<canary-inventory-identity>' \
  --vault-password-file '<vault-password-file>'
```

`bazel_l1_roll.yaml` stops, installs the binary, renders env/unit, starts, and
checks health with `serial: 1` and `any_errors_fatal: true`. It has no drain
endpoint: active requests can be interrupted and the ring must retain healthy
capacity. Review a single-member staging ring's impact before restarting it.
Wait for the canary's old/new/invalid probes to pass before the remaining nodes.

If every node already runs the approved multi-key binary, use the configuration
convergence playbook rather than changing the binary:

```bash
ansible-playbook -i '<environment>_bazel_l1_<region>.ini' bazel_l1_map_converge.yaml \
  -e bazel_l1_target_group='<inventory-group>' \
  --limit '<canary-inventory-identity>' \
  --vault-password-file '<vault-password-file>'
```

It renders env/config/unit and restarts changed nodes serially. Review all
rendered settings first: it converges the complete templates, not only auth.
It avoids the TLS/NVMe provisioning touched by full setup; missing historical
TLS entries in a vault should not be “fixed” by running full hydration for a
credential-only change. For bastion-only public SSH, use the documented
management address override for **one selected node**, never a global address
override on a multi-node invocation.

For every node, record unit health, actual executable hash, accepted-set match,
and probe results. Run `systemctl is-active bazel-remote`; capture environment
comparisons privately instead of printing `systemctl show ... Environment`.
Stop on any mismatch and leave runner credentials unchanged.

## 4. Probe the protected RPC boundary

The following diagnostic intentionally omits storage-prefix metadata. A valid
key passes auth and receives the specific **missing storage prefix** error;
an invalid key receives `Unauthenticated`. It never reads/writes a cache object
or needs a tenant identity. It checks both unary and streaming interceptors.

This distinction is deliberate: a random `InvalidArgument` is not a pass. The
probe requires its exact post-auth error. In earlier probes with a valid prefix
but no backend selector, the analogous expected error was
`missing x-blacksmith-s3-endpoint metadata`; that is also not a successful cache
operation. Neither probe replaces the end-to-end cache canary after cutover.

Save the following as `$ROTATION_DIR/bazel-auth-probe.go`, then build it from
this repository's approved `patchset` checkout in the normal build environment:

```go
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	reapi "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type probeCase struct {
	Label, KeyFile string
	Accept         bool
}
type input struct {
	Target, CAFile string
	Cases          []probeCase
}

func run() error {
	var in input
	if err := json.NewDecoder(os.Stdin).Decode(&in); err != nil {
		return fmt.Errorf("read probe description: %w", err)
	}
	pem, err := os.ReadFile(in.CAFile)
	if err != nil {
		return fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return fmt.Errorf("invalid CA PEM")
	}
	conn, err := grpc.NewClient(in.Target, grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer func() { _ = conn.Close() }()
	for _, c := range in.Cases {
		raw, err := os.ReadFile(c.KeyFile)
		if err != nil {
			return fmt.Errorf("read key file: %w", err)
		}
		key := strings.TrimSpace(string(raw))
		if key == "" {
			return fmt.Errorf("empty key file")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(cache.AuthSecretGRPCMetadataKey, key))
		_, unaryErr := reapi.NewContentAddressableStorageClient(conn).FindMissingBlobs(
			ctx, &reapi.FindMissingBlobsRequest{})
		stream, streamErr := bytestream.NewByteStreamClient(conn).Read(ctx, &bytestream.ReadRequest{})
		if streamErr == nil {
			_, streamErr = stream.Recv()
		}
		cancel()
		for _, result := range []struct {
			name string
			err  error
		}{{"unary", unaryErr}, {"stream", streamErr}} {
			s := status.Convert(result.err)
			pass := s.Code() == codes.Unauthenticated
			if c.Accept {
				pass = s.Code() == codes.InvalidArgument &&
					s.Message() == "missing x-blacksmith-storage-prefix metadata"
			}
			fmt.Printf("%s %s code=%s pass=%t\n", c.Label, result.name, s.Code(), pass)
			if !pass {
				return fmt.Errorf("unexpected auth result for %s/%s", c.Label, result.name)
			}
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

```bash
go build -o "$ROTATION_DIR/bazel-auth-probe" "$ROTATION_DIR/bazel-auth-probe.go"
export L1_TARGET='<public-ip-or-dns>:9092'
export L1_CA_FILE='<path-to-L1-CA.pem>'
export PROBE_PHASE=overlap  # change to retired only after retirement
python3 - <<'PY' | "$ROTATION_DIR/bazel-auth-probe"
import json, os
from pathlib import Path
w = Path(os.environ['ROTATION_DIR'])
phase = os.environ['PROBE_PHASE']; assert phase in ['overlap', 'retired']
(w/'bazel.invalid').write_text('invalid-credential-rotation-probe')
print(json.dumps({'Target': os.environ['L1_TARGET'], 'CAFile': os.environ['L1_CA_FILE'],
 'Cases': [{'Label': label, 'KeyFile': str(w/('bazel.'+label)), 'Accept': accept}
           for label, accept in [('new', True), ('old', phase == 'overlap'), ('invalid', False)]]}))
PY
```

Run from an allowed network source and use the L1 CA with the public endpoint
whose certificate SAN matches. A timeout, TLS failure, or refused connection is
not an authentication verdict. Do not weaken certificate verification to make
the remote probe pass. Save labeled results and endpoint/timestamp, not keys.
Check every discovered endpoint again immediately before updating clients.

## 5. Update clients, roll, and wait for old jobs

Only after complete server coverage, update `BAZELRE_CACHE_L1_SECRET` to the
single new key in every applicable Doppler `gha-agent` config for the selected
environment. Include regional, Linux ARM, and Mac configs; preserve intentionally
absent variables. Read back each write privately to verify it. Server overlap
can remain in place while waiting for a separately scheduled agent roll.

Gracefully roll live/reporting runner hosts, including both Linux architectures
and Mac regions. Force a restart when the code SHA is unchanged. Keep old
accepted while outgoing processes finish their jobs; the new agent's readiness
does not prove they are gone. Inspect actual process credentials and local jobs
by PID. Track offline/excluded consumers and their return-to-service plan.

Monitor `bazel_remote_auth_secret_rejected_total{reason="missing|duplicate|mismatch"}`
(these are the possible label values, not a literal PromQL alternation), cache
health and latency, and a real staging cache workload. Account for deliberate
negative probes when interpreting counter increments. Do not interpret a cache
miss/fallback as proof that authentication worked.

## 6. Retire old and close the operation

1. Confirm runner hosts have been rolled: every live serving client uses new,
   no old-key jobs/processes remain, and offline consumers cannot return with old.
2. Generate a new `!vault` block containing only `bazel.new`; replace only the
   selected environment's variable. Commit/review the encrypted change.
3. Apply the same canary-first, serial configuration convergence to all L1s.
4. Run the probe with `PROBE_PHASE=retired`: new must reach the expected
   post-auth prefix error; old and invalid must be `Unauthenticated` in both
   RPC modes. Recheck discovery membership so newly added nodes are included.
5. Verify a real cache workload and the error trend; record completion and
   remove transient secret files/probes according to the recovery policy.

## Recovery

Before any client uses new, restoring a server's old binary/config is safe for
credentials; verify old afterward and stop the rollout. Once any client uses
new, restore **dual acceptance**, not old-only. The generic Ansible rescue
starts the binary/config on disk; it does not necessarily restore the backup.
Inspect and restore explicitly when required.

If a server still accepts old after retirement, check its actual PID/start time,
env-file source, binary revision, and inventory/discovery identity. If new is
rejected after retirement, restore overlap while investigating stale callers.
Never solve either case by disabling authentication or altering tenant-prefix
or backend-selector validation.
