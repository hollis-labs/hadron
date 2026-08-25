# Integration Commit Table

| Order | Task | Source commit | Integration commit | Repository | Result |
| --- | --- | --- | --- | --- | --- |
| 1 | W00-T02 | `497bdbb` | `2b9f287` | Hadron | accepted |
| 2 | W00-T01 | `bb8a16f` | `65a7aeb` | Hadron | accepted |
| 3 | W00-T03 | `54c7c85` | `1fc2dbc` | Hadron | accepted after guard hardening |
| 4 | W00-T07 | `51ebe8a` | `51ebe8a` | go-scheduler | fast-forwarded local `main`; version pin deferred to W05-T04 |
| 5 | W00-T04 | `a2e02ca` | `8a2ec65` | Hadron | accepted |
| 6 | W01-T01 | `3a0c0c4` | `7b6e712` | Hadron | accepted after complete nested source-ref validation and deterministic map traversal |
| 7 | W00-T05 | `f56ce04` | `f0ceef1` | Hadron | accepted after source-coordinate validation hardening |
| 8 | W01-T02 | `3babe00` | `0fd8245` | Hadron | accepted after closed-enum and structural rejection coverage |
| 9 | W02-T01 | `9776847` | `10e0068` | Hadron | accepted after invalid UTF-8 transport hardening |
| 10 | W01-T03 | `353c45d` | `e598e5f` | Hadron | accepted; SOURCE-001..004 and LEGACY-001 reserved |
| 11 | W00-T06 | `c8349c1` | `0983086` | Hadron | accepted after diagnostic-code allocation and recursive schema hardening |
| 12 | W02-T02 | `5fee6ec` | `985243b` | Hadron | accepted after heterogeneous-array cache-schema hardening |
| 13 | W01-T04 | `785121b` | `ee38867` | Hadron | accepted after exact-number digest and literal-interpolation hardening |
| 14 | W01-T05 | `29121b6` | `7eea197` | Hadron | accepted with dependency-graph-only allowance for the adopted schema validator's transitive `x/text` closure |
| 15 | W02-T04 | `1bc4de0` | `2caeb82` | Hadron | accepted after immutable run-plan/wait-invocation and semantic time-equality hardening |
| 16 | W02-T07 | `b579713` | `f990807` | Hadron | accepted after scoped root-map and same-endpoint control/data edge hardening |
| 17 | W03-T01 | `fe9de95` | `67f97c8` | Hadron | accepted after lease-releasing wait and claimed-resume lifecycle hardening |
| 18 | W01-T07 | `eb6d4e9` | `1285639` | Hadron | accepted after activation vocabulary, file-path, and operational-state hardening |
| 19 | W03-T02 | `f0a8abb` | `cb96ee1` | Hadron | accepted after durable replay, claim-generation fencing, and leased-ready recovery hardening |
| 20 | W01-T06 | `e5d3c38` | `88fb4cf` | Hadron | accepted with production compiler, inference, validation, source-map, and diagnostic fixture coverage |
| 21 | W02-T05 | `c9497ae` | `b811d23` | Hadron | accepted after transactional integrity, append-only event, reopen, and two-handle contention hardening |
| 22 | W03-T03 | `3321fed` | `a588285` | Hadron | accepted after SQLite skipped-explanation and blocked-refresh parity hardening |
| 23 | W02-T03 | `9dce984` | `257a584` | Hadron | accepted after lossless artifact input, declaration-schema, local-ref artifact, and schema-literal hardening |
| 24 | W02-T06 | `c8a951f` | `4648e0b` | Hadron | accepted after secret ArtifactRef taint preservation, canonical SecretRef identity, and SQLite prewrite enforcement |
| 25 | W03-T05 | `53748d1` | `5d9fbfc` | Hadron | accepted after URL credential, deterministic metadata-validation, and authorization-order hardening |
| 26 | W02-T08 | `ee94487` | `177e0e1` | Hadron | accepted after fail-closed authorization, bounded capture, filesystem identity, and cancellation hardening |
| 27 | W04-T01 | `c19faf7` | `f1ad5fa` | Hadron | accepted after same-attempt wait binding, recoverable external-operation lifecycle, exact-number persistence, and transient hook-error hardening |
| 28 | W04-T02 | `1f5b966` | `b8ee57d` | Hadron | accepted with deterministic scoped expression contexts, exact typed outputs, and runtime-owned dynamic output-schema enforcement |
| 29 | W04-T05 | `41a5057` | `5f0c1aa` | Hadron | accepted after fail-closed annotation mapping, complete result redaction, bounded artifact capture, and idempotency-gated workflow reconnect |
| 30 | W04-T03 | `e508945` | `439fafe` | Hadron | accepted after structured launch policy, raw-byte-bounded redaction, truthful sandbox attestation, and typed stream capture |
| 31 | W04-T04 | `2e233ed` | `4ad4e14` | Hadron | accepted after all-answer destination authorization, pinned transport, percent-decoded redirect-secret rejection, full-operation deadlines, and expanded-secret bounds |
| 32 | W03-T04 | `34f1ee3` | `f103b3d` | Hadron | accepted after cancellation-wins terminal fencing, trusted retry-policy enforcement, collision-free durable identities, restart-durable activations, and atomic fan-out persistence |
| 33 | W04-T07 | `c680634` | `2229ed8` | Hadron | accepted after deterministic timer-wake ordering, typed timeout failure, retry-stable callback and gate payload contracts, authorized message bridging, restart recovery, and SQLite parity |
| 34 | W07-T02 | `25dabcf` | `9e52e0a` | Hadron | accepted after fail-closed Goja sandbox hardening, deterministic structural limits, exact-number admission, typed private outputs, cancellation, and concurrent execution coverage |
| 35 | W04-T06 | `d7c312f` | `8ac8dbe` | Hadron | accepted after immutable call-site resolution pinning, exact replay/conflict fencing, typed partial/default/local input binding, authoritative lineage validation, and nested inline/run runtime coverage |
| 36 | W05-T01 | `a5eb3bc` | `2d14c13` | Hadron | accepted after exact authenticated start replay, append-only policy facts, convergent lifecycle recovery, bounded cancellation CAS, and atomic call/child-run SQLite binding |
| 37 | W03-T08 | `d771e44` | `61ea279` | Hadron | accepted after recursive cancellation-tree, semantic replay, pending-child recovery ordering, terminal-intent context, and admission-fence hardening |
| 38 | W05-T03 | `c40bf7d` | `c950291` | Hadron | accepted after canonical exact-selector, whole-stream package bound, registry response bound, cancellation-aware child materialization, and overflow hardening |
| 39 | W07-T03 | `e5a9dc4` | `2d95b0c` | Hadron | accepted with graph-native agent sugar, durable session-host contract, container-scoped generated-bundle authorization, and restart-safe child resolution |
| 40 | W03-T07 | `cb3e80c` | `6c59dbf` | Hadron | accepted after durable-projection integrity, zero-finalizer fail-fast fencing, and in-memory/SQLite admission rollback parity hardening |
| 41 | W04-T08 | `1d670de` | `92147ad` | Hadron | accepted after frozen catalog identity, schema/evidence fail-closed validation, suspension fencing, and atomic external terminal verification hardening |
| 42 | W03-T06 | `4110fd9` | `8fb3ab0` | Hadron | accepted after exact pinned-plan recovery, compiler-scoped input binding, deterministic crash policy, replay/fan-out provenance, and readiness-fixpoint hardening |
| 43 | W03-T09 | `1ffda24` | `ca2a7d2` | Hadron | accepted after append-order memo parity and untrusted value-record integrity hardening |
| 44 | W05-T07 | `4a89c14` | `d717a58` | Hadron | accepted after caller-ownership, bounded-report, post-attestation, and exact namespace-authorization hardening |
| 45 | W07-T01 | `93667ef` | `377490c` | Hadron | accepted after exact provider-identity, provenance, audit-budget, response-shape, and tool-batch hardening |
| 46 | W03-T08-H1 | `04ba64b` | `77b65c0` | Hadron | accepted after stale intent/run read-skew, failure-side-effect, exact output-ref, reopen/replay, and contract-runner hardening |
| 47 | W05-T02 | `65e1bc0` | `300257b` | Hadron | accepted after graph-requirement, public-metadata, defensive-ownership, and exact durable scope/target hardening |
| 48 | W07-T07 | `aa01d1d` | `9431754` | Hadron | accepted after shared-profile, authority/responder, receipt, observation, restart, and conformance hardening |
| 49 | W07-T05 | `6348a06` | `4d9c2c7` | Hadron | accepted after manifest-integrity, exact node-profile, safe narrowing, reconstructible remote bridge, runtime-parity, and atomic reproducibility hardening |
| 50 | W05-T04 | `bb2a55a` | `c68a38a` | Hadron | accepted after exact registration identity, stable-fire replay, leased-claim recovery, activation-local binding, overlap/reuse, callback idempotency, observer redaction, and two-handle CAS hardening |
| 51 | W07-T09 | `fbc877d` | `94f03bb` | Hadron | accepted after deterministic lowering, fail-fast admission fencing, launch-before-provider durability, monotonic service recovery, exact generated-child authorization, and honest optional-gate hardening |
| 52 | W05-T06 | `c8b57bc` | `ddc2030` | Hadron | accepted after bounded durable-state projection, pinned-plan/source explanation, shared redaction, exact activation-attempt joins, and credential-shaped lease-owner hardening |
| 53 | W05-T08 | `dcfb868` | `0a37129` | Hadron | accepted after qualified-registration ingress, exact current-alias removal, source/materialization digest fencing, reconciliation replay, and schedule-projection rollback hardening |
| 54 | W06-T01 | `6ae32b2` | `93ab469` | Hadron | accepted after authenticated shared-service routing, safe transport projections, truthful dry-run handling, exact run authority, and terminal pre-admission pin rejection |
| 55 | W06-T05 | `d4cbb35` | `0fbea80` | Hadron | accepted with bounded graph/edge diagnostics, redacted value-flow associations, safe start-policy facts, fan-out-safe inspection, and graph-native desktop controls; rendered browser QA unavailable in the execution environment |
| 56 | W06-T02 | `5bfc8c2` | `cea2992` | Hadron | accepted after exact route matching, bounded duplicate-safe JSON, authenticated authority rebinding, hidden-resource equivalence, private-display authorization, safe errors, and transport-level idempotency enforcement |
| 57 | W06-T02-H1 | root integration review | `99de732` | Hadron | accepted after escaped-segment routing preserved opaque run IDs containing path separators without weakening exact action matching |
| 58 | W07-T04 | `e52e041` | `3a88a51` | Hadron | accepted after bounded local-schema expansion, runtime-bound unsafe idempotency, secret-backed bearer/basic/header credentials, credential-channel rejection, conservative effects, and immutable generated catalog hardening |
