# Task And Dependency Ledger

Status values are `blocked`, `eligible`, `dispatched`, `reviewing`,
`integrated`, or `owner-gated`.

| Task | Dependencies | Status | Integrated commit |
| --- | --- | --- | --- |
| W00-T01 | none | integrated | `65a7aeb` |
| W00-T02 | none | integrated | `2b9f287` |
| W00-T03 | W00-T02 | integrated | `1fc2dbc` |
| W00-T04 | W00-T02 | integrated | `8a2ec65` |
| W00-T05 | W00-T02, W01-T01 | integrated | `f0ceef1` |
| W00-T06 | W00-T02, W00-T05, W01-T01 | integrated | `0983086` |
| W00-T07 | none | integrated | `go-scheduler@51ebe8a` |
| W01-T01 | W00-T02 | integrated | `7b6e712` |
| W01-T02 | W01-T01 | integrated | `0fd8245` |
| W01-T03 | W01-T01, W00-T05 | integrated | `e598e5f` |
| W01-T04 | W01-T01, W01-T02, W01-T03 | integrated | `ee38867` |
| W01-T05 | W01-T04, W00-T06 | integrated | `7eea197` |
| W01-T06 | W01-T04 | integrated | `88fb4cf` |
| W01-T07 | W01-T03, W01-T04 | integrated | `1285639` |
| W02-T01 | W01-T01 | integrated | `10e0068` |
| W02-T02 | W02-T01, W00-T05 | integrated | `985243b` |
| W02-T03 | W02-T01, W02-T02, W02-T04 | integrated | `257a584` |
| W02-T04 | W01-T04, W02-T01 | integrated | `2caeb82` |
| W02-T05 | W02-T04 | integrated | `b811d23` |
| W02-T06 | W02-T01, W02-T04 | integrated | `4648e0b` |
| W02-T07 | W01-T04, W01-T05, W02-T02 | integrated | `f990807` |
| W02-T08 | W02-T01, W02-T04, W02-T06 | integrated | `177e0e1` |
| W03-T01 | W02-T04 | integrated | `67f97c8` |
| W03-T02 | W03-T01 | integrated | `cb96ee1` |
| W03-T03 | W02-T02, W03-T01 | integrated | `a588285` |
| W03-T04 | W02-T02, W02-T07, W03-T01, W03-T02, W03-T05 | integrated | `f103b3d` |
| W03-T05 | W03-T01, W02-T05 | integrated | `5d9fbfc` |
| W03-T06 | W03-T02, W03-T05, W03-T08 | integrated | `8fb3ab0` |
| W03-T07 | W03-T02, W03-T04 | integrated | `6c59dbf` |
| W03-T08 | W02-T07, W03-T03, W03-T04 | integrated | `61ea279` |
| W03-T08-H1 | W03-T08, W03-T09, W05-T07 | integrated | `77b65c0` |
| W03-T09 | W02-T04, W03-T06, W03-T08 | integrated | `ca2a7d2` |
| W04-T01 | W00-T06, W03-T01 | integrated | `f1ad5fa` |
| W04-T02 | W02-T02, W04-T01 | integrated | `b8ee57d` |
| W04-T03 | W02-T01, W04-T01 | integrated | `439fafe` |
| W04-T04 | W02-T01, W04-T01 | integrated | `4ad4e14` |
| W04-T05 | W02-T01, W04-T01 | integrated | `5f0c1aa` |
| W04-T06 | W01-T04, W03-T02, W04-T01 | integrated | `8ac8dbe` |
| W04-T07 | W03-T05, W04-T01 | integrated | `2229ed8` |
| W04-T08 | W02-T02, W03-T08, W04-T01 | integrated | `92147ad` |
| W05-T01 | W02-T05, W02-T08, W03-T02, W04-T01, W04-T02, W04-T03, W04-T04, W04-T05, W04-T06, W04-T07 | integrated | `2d14c13` |
| W05-T02 | W05-T01 | integrated | `300257b` |
| W05-T03 | W01-T04, W05-T01 | integrated | `c950291` |
| W05-T04 | W00-T07, W03-T05, W05-T03 | integrated | `c68a38a` |
| W05-T05 | W01-T04, W05-T03 | integrated | `f0df790` |
| W05-T06 | W03-T01, W03-T07, W05-T01 | integrated | `ddc2030` |
| W05-T07 | W03-T06, W04-T02, W04-T03, W04-T04, W04-T05, W04-T06, W04-T07, W05-T01, W05-T03 | integrated | `d717a58` |
| W05-T08 | W01-T07, W05-T03, W05-T04 | integrated | `0a37129` |
| W06-T01 | W05-T03, W05-T06 | integrated | `93ab469` |
| W06-T02 | W05-T01, W05-T06 | integrated | `cea2992`, `99de732` |
| W06-T03 | W05-T03, W05-T07, W06-T02 | integrated | `6a4f55f` |
| W06-T04 | W05-T01, W06-T02 | integrated | `05a97b5` |
| W06-T05 | W01-T04, W05-T06 | integrated | `0fbea80` |
| W06-T06 | W06-T01, W06-T02, W06-T03, W06-T04, W06-T05 | eligible | |
| W06-T07 | W06-T06, W06-T08, W06-T09, W06-T10 | blocked | |
| W06-T08 | W05-T07, W06-T01, W06-T03, W06-T04 | eligible | |
| W06-T09 | W03-T04, W03-T07, W03-T08, W04-T02, W04-T05, W05-T01, W05-T03, W05-T07, W06-T03 | integrated | `c2a7dec`, `7a4895c` |
| W06-T10 | W06-T02, W06-T05 | integrated | `2fdb665`, `e2ee5ee`, `225d113`, `bc3b589` |
| W07-T01 | W04-T08, W05-T01 | integrated | `377490c` |
| W07-T02 | W04-T01, W02-T02 | integrated | `9e52e0a` |
| W07-T03 | W03-T05, W04-T06 | integrated | `2d95b0c` |
| W07-T04 | W04-T01, W06-T02 | integrated | `3a88a51` |
| W07-T05 | W01-T02, W04-T02, W04-T03, W04-T04, W04-T05, W04-T06, W04-T07 | integrated | `4d9c2c7` |
| W07-T06 | W00-T04, W06-T07 | blocked | |
| W07-T07 | W04-T01, W04-T07, W05-T01 | integrated | `9431754` |
| W07-T08 | W00-T07, W03-T06, W04-T07, W05-T04 | integrated | `53217b3` |
| W07-T09 | W02-T07, W03-T08, W04-T07 | integrated | `94f03bb` |
| W07-T10 | W03-T08, W04-T06, owner-approved compensation ADR | owner-gated | |
| W07-T11 | W01-T02, W05-T03, W06-T01, W06-T02 | integrated | `d66cc39` |

W07-T09's durable service-store migration integrated as `0024` after W05-T04's
`0023` activation migration.
