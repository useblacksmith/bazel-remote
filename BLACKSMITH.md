# Blacksmith bazel-remote fork

This repository is the Blacksmith-owned fork of
`github.com/buchgr/bazel-remote/v2` used by the FA agent's embedded Buck2
cache.

## Upstream base

- Module: `github.com/buchgr/bazel-remote/v2`
- Version: `v2.4.4`
- Upstream tag: `refs/tags/v2.4.4`
- Upstream commit: `54d1782d72b291937988edad32c9752abe269d8e`
- Module sum: `h1:fYqg5C4COpTO1OTHUHjYvVAOb2rEe2Xt+oYu4JTOlbc=`
- Go module sum: `h1:Z7rZqDuLXfCyJ9HCu3ZYQsRx/yKHA+XH0eZq6jm80Gk=`

## Local use

FA replaces `github.com/buchgr/bazel-remote/v2` with this fork via a local
submodule checkout. Existing FA imports intentionally keep the upstream import
path so this fork remains behavior-preserving until Blacksmith-specific changes
are needed.

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
