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
