# Artifact Contract

`ArtifactStore` is the extraction-ready boundary for bytes that do not belong
inside workflow state. A producer streams bytes through `Put`; workflow state
stores only the returned `ArtifactRef`. `Open` and `Stat` require an explicit
`ArtifactAccess`, and concrete adapters authorize before resolving local paths
or contacting delegates. An owner-aware adapter authorizes again after it has
verified immutable storage metadata.

`ArtifactRef.Store` is the resolution authority. The authority owns its URI
grammar: Hadron-local references use a closed `artifact://hadron-local/...`
form, while approved external delegates receive their opaque URI unchanged.
`Producer` is the value provenance carrier, and redaction/retention remain part
of the immutable reference identity.

`ArtifactReadCloser` verifies SHA-256 at EOF. Closing before verified EOF
returns a stable verification error, and repeated `Close` calls return the same
result. A concrete local adapter may verify the complete file before returning
the reader as an additional defense against local tampering.

Contexts are required and may not already be canceled. Streaming capture and
the Hadron adapter check cancellation before and after source reads; a source
that can block indefinitely should itself provide a cancellation-aware Reader.
Adapters snapshot pointer-backed expected-size input before consuming bytes.

## Capture policy

`CaptureValue` uses a caller-selected positive inline limit. The exported
`DefaultInlineLimit` is 64 KiB and the maximum configurable inline limit is 1
MiB; callers must still set the policy field explicitly, because zero is
rejected. This is not an artifact size limit: `ArtifactPutRequest.MaxBytes` is
a separately required, stream-enforced cap and may be much larger.

JSON and text capture buffers at most `InlineLimit+1` bytes. A complete small
JSON value is decoded with exact `json.Number` values; malformed small JSON
fails. Oversized JSON is not claimed to be parsed or valid: its original bytes
are replayed into `Put` and the result becomes `TypeArtifact`. Invalid UTF-8
text is likewise promoted. Secret-classified capture always promotes, even
when small, so secret material can never become an inline Value.

## Retention and cleanup

Only `run` and `project` are locally durable ownership classes. Local `Put`
rejects `none` because it is non-durable and rejects `external` because it is
not locally owned. Cleanup reports a closed, aggregate result:

- `deleted` with a positive count for removed local artifacts or partials;
- `already_absent` for an idempotent local retry;
- `not_stored` for `none` or an empty expiry/partial sweep; and
- `preserved_external` for approved external content.

Cleanup results contain no references, URIs, digests, owner identifiers, or
payloads. External delegates are never called for deletion.

`ArtifactError.Error` and its structured diagnostic use closed vocabulary and
never render the wrapped operating-system cause or artifact reference. The raw
cause and a defensive reference copy remain available for process-local logic;
presentation must still use the normal value redaction policy.
