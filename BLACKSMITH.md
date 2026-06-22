# Blacksmith bazel-remote fork

This repository is the Blacksmith-owned fork of upstream
`github.com/buchgr/bazel-remote/v2` used by the FA agent's embedded Buck2
cache.

Repository location: `github.com/useblacksmith/bazel-remote`.
Go module path: `github.com/buchgr/bazel-remote/v2`.

## Upstream base

- Module: `github.com/buchgr/bazel-remote/v2`
- Version: `v2.4.4`
- Upstream tag: `refs/tags/v2.4.4`
- Upstream commit: `54d1782d72b291937988edad32c9752abe269d8e`
- Module sum: `h1:fYqg5C4COpTO1OTHUHjYvVAOb2rEe2Xt+oYu4JTOlbc=`
- Go module sum: `h1:Z7rZqDuLXfCyJ9HCu3ZYQsRx/yKHA+XH0eZq6jm80Gk=`

## Local use

FA replaces `github.com/buchgr/bazel-remote/v2` with a tagged version fetched
from `github.com/useblacksmith/bazel-remote/v2`. Existing FA imports
intentionally keep the upstream import path so this fork remains
behavior-preserving until Blacksmith-specific changes are needed.

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
tags, and security advisories for `bazel-remote`. To apply an upstream patch:

1. Identify the upstream commit or release containing the fix.
2. Apply or cherry-pick the relevant changes into this repository.
3. Keep Blacksmith-local changes separate from upstream patch commits when
   possible.
4. Update this file with the new upstream base or applied patch commit.
5. Run the FA agent build and Buck2 cache tests before merging.

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
