# Hadron Artifact Adapter

This package implements `workflow/values.ArtifactStore` for Hadron-owned
run/project artifacts and routes reads to explicitly approved external stores.
It is not wired into legacy executors or runtime dispatch by this package.

The caller supplies a dedicated root and an `ArtifactAuthorizer`. Missing
authorization fails closed. Every operation validates the immutable authority
and reference before a pre-resolution policy check. Local operations then
verify the stored manifest and perform an owner-aware policy check. External
stores receive their URI unchanged and must implement the same authorization
contract themselves.

Local references contain only the fixed scope plus hashes of the owner and
immutable artifact metadata. Raw owner IDs and absolute paths never enter a
URI. Directories use mode `0700`; payloads and manifests use `0600`. Writes are
bounded and hashed while streaming into a same-filesystem staging directory,
fsynced, and atomically renamed. Failed writes remove staging data; explicit
partial cleanup handles crash remnants after a caller-provided cutoff.

Reads reject symlinks and nonregular files, compare the pre-open and opened file
identities, enforce manifest/payload size bounds, and verify the complete local
payload digest before returning any bytes. The configured root, `objects`, and
`staging` identities are revalidated for every local operation so replacing a
directory after construction fails closed.

No database migration is required. The existing workflow value-set storage
already persists the immutable `ArtifactRef`; payloads and adapter manifests
stay outside SQLite.
