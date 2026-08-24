# Value Classification And Secret References

Workflow state stores typed envelopes, not credential material. A credential
is represented by a `secret_ref` Value containing a canonical
`secret://authority/path[#field]` reference and ordinary producer provenance.
Authorities are lowercase and URI path/fragment escaping must be canonical, so
aliases cannot produce different value digests for the same URI identity.
Only the host adapter that needs a credential resolves it. Resolved bytes are
injected, registered with a `Redactor`, masked from observations, and forgotten.

Secret-classified inline Values are invalid. Secret-classified ArtifactRefs are
valid because workflow state stores only immutable reference metadata; artifact
authorization, byte access, and deletion belong to the artifact adapter. Exact
expression passthrough preserves any secret-classified envelope, but computed,
dynamic, and interpolated use is rejected so it cannot lose classification.
The streaming contract, capture caps, and aggregate cleanup outcomes are
specified in [ARTIFACTS.md](ARTIFACTS.md).

Rendering defaults to masking private payloads. A caller must explicitly select
the closed `reveal` policy to display private data. SecretRef and secret Artifact
payload metadata are always rendered as `[REDACTED]`.

Retention classes have these core meanings:

- `none`: ephemeral only; durable ValueSet persistence is rejected.
- `run`: eligible for host cleanup at run completion.
- `project`: retained under project policy across runs.
- `external`: the payload lifecycle remains owned outside the workflow store.

`runtime.SaveValuesWithRetention` reports stable, sorted per-class name groups
to an optional host hook before and after the immutable write. A nil hook is
valid. Mixed run/project/external sets are valid. If the post-write hook fails,
the typed error carries the saved ValueSetRef so the host can identify the
unreferenced immutable row.
