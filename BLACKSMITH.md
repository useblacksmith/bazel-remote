# Blacksmith bazel-remote fork

This repository is the Blacksmith-owned fork of upstream
`github.com/buchgr/bazel-remote/v2` used by the FA agent's embedded Buck2
cache.

Repository location: `github.com/useblacksmith/bazel-remote`.
Go module path: `github.com/buchgr/bazel-remote/v2`.

## Branch ownership

- `main` owns release automation and documents the fork's branch and release
  policy. Going forward, runtime source changes land only on `patchset`; the
  source tree on `main` is not a release source.
- `patchset` is the canonical executable source branch. It contains an exact
  upstream release plus the Blacksmith-owned source patches applied on top.
  Pull requests that change bazel-remote code target `patchset`.
- Upstream upgrades rebase `patchset` onto an exact, verified upstream release
  tag. Update the remote branch with `--force-with-lease`; do not merge
  `patchset` into `main`.
- Release tags use `vX.Y.Z-blacksmith.N` and are cut from `patchset`. The release
  workflow on `main` validates that the published tag is reachable from the
  current `patchset`, resolves it to one immutable commit, and gives that same
  commit to every platform build.

## Patchset upstream base

- Module: `github.com/buchgr/bazel-remote/v2`
- Version: `v2.6.1`
- Upstream tag: `refs/tags/v2.6.1`
- Upstream commit: `f46bc2030d3f30604d79ef4bf040e3a9c7a4ff89`
- Module sum: `h1:vTMw3VmzjHfmR9jHcnqzQLLuHXRIFkROOcp5Pjke59c=`
- Go module sum: `h1:vC7tD62wunH9S286SJ8naNJpKQNUgzlK3VlW816sI1E=`

## Local use

FA replaces `github.com/buchgr/bazel-remote/v2` with a commit or release tag cut
from `patchset` and fetched from `github.com/useblacksmith/bazel-remote/v2`.
Existing FA imports intentionally keep the upstream import path.

## Build cache storage prefixing

BLA-4006 keeps the default upstream behavior unless FA attaches an explicit
request-scoped storage prefix to the cache operation context.

The existing configured S3 prefix remains the default path for Buck2 and any
other callers that do not opt in to request-scoped routing. For Bazel, FA should
resolve the authorized VM/job namespace to the full physical prefix:

```text
<MINIO_PREFIX>/<environment>/<model_installation_id>/<repository_id>/<generation>/<tool>
```

and attach it with `cache.WithStoragePrefix`. The S3 proxy then uses that
request-scoped prefix when constructing Action Cache and CAS object keys. Action
Cache also remains isolated by bazel-remote's existing instance-name key
remapping, so the physical prefix is additive and gives cache-clear/delete
operations a visible repo/generation boundary. The local disk cache AC/CAS keys
also include the request-scoped prefix, so a new repo/generation namespace does
not hit stale local entries before reaching the S3 backend. This lets a single
shared bazel-remote process route AC/CAS puts/gets to the correct
repo/generation namespace while preserving existing Buck2 behavior.

Local disk cache entries store the full request prefix as a stable hash so the
LRU can distinguish identical AC/CAS digests from different repo/generation
namespaces without using S3-style slash-heavy prefixes in local paths. MinIO/S3
object keys use the real request-scoped prefix directly, so broad remote
deletion still targets `<MINIO_PREFIX>/<environment>/<model_installation_id>/<repository_id>/<generation>/`.

For Bazel requests, FA should also mark the request with
`cache.WithRequiredStoragePrefix`. If a request reaches the S3 proxy with that
marker but without a request-scoped prefix, bazel-remote logs that it is falling
back to the configured process-wide prefix. Buck2 should not set this marker.

## Security and upstream patch tracking

Track upstream security fixes by monitoring the upstream repository's releases,
tags, and security advisories for `bazel-remote`. To upgrade the upstream base:

1. Fetch and verify the exact upstream release tag.
2. Identify the upstream commit on which the current `patchset` is based.
3. Rebase with `git rebase --onto <target-tag> <old-base> patchset`.
4. Resolve conflicts only in the Blacksmith patch stack, keeping those commits
   separate from the upstream history.
5. Update this file and any generated build metadata, then run the Go, race,
   vet, Gazelle, and Bazel checks plus the FA cache integration tests.
6. Update the remote with `git push --force-with-lease origin patchset`.
7. Cut release tags from `patchset`; `main` only validates and builds them.

BLA-4006 should make CAS namespacing changes in this repository.

## Build cache operation observation

BLA-4010 adds optional cache operation observation for FA-owned customer
metrics. Callers may attach opaque identity labels with
`cache.WithMetricsLabels`. bazel-remote stores and forwards those labels but
does not interpret tenant, repository, VM, or job identity.

The disk cache accepts an optional `cache.OperationObserver` and invokes it next
to the existing endpoint metrics decorator for semantic cache outcomes:

- `action_cache_get`: `hit`, `miss`, or `error`
- `cas_lookup`: `hit`, `miss`, or `error`

The S3 proxy accepts the same observer and records backend async upload health
only:

- `backend_upload`: `error` or `dropped`

Client transfer bytes are intentionally not inferred inside bazel-remote; FA
observes gRPC request/response payloads and emits `client_upload` and
`client_download` rows with byte counts.

Nil observers preserve existing behavior. Observer panics are swallowed through
the cache package helper so metrics collection cannot change cache request
outcomes. The fork still has no Laravel/Web dependency; FA owns aggregation and
ClickHouse delivery.
