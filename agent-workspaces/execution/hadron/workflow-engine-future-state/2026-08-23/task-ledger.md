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
| W01-T06 | W01-T04 | eligible | |
| W01-T07 | W01-T03, W01-T04 | dispatched | |
| W02-T01 | W01-T01 | integrated | `10e0068` |
| W02-T02 | W02-T01, W00-T05 | integrated | `985243b` |
| W02-T03 | W02-T01, W02-T02, W02-T04 | eligible | |
| W02-T04 | W01-T04, W02-T01 | integrated | `2caeb82` |
| W02-T05 | W02-T04 | dispatched | |
| W02-T06 | W02-T01, W02-T04 | eligible | |
| W02-T07 | W01-T04, W01-T05, W02-T02 | integrated | `f990807` |
| W02-T08 | W02-T01, W02-T04, W02-T06 | blocked | |
| W03-T01 | W02-T04 | integrated | `67f97c8` |
| W03-T02 | W03-T01 | eligible | |
| W03-T03 | W02-T02, W03-T01 | eligible | |
| W03-T04 | W02-T02, W02-T07, W03-T01, W03-T02, W03-T05 | blocked | |
| W03-T05 | W03-T01, W02-T05 | blocked | |
| W03-T06 | W03-T02, W03-T05 | blocked | |
| W03-T07 | W03-T02, W03-T04 | blocked | |
| W03-T08 | W02-T07, W03-T03, W03-T04 | blocked | |
| W03-T09 | W02-T04, W03-T06, W03-T08 | blocked | |
| W04-T01 | W00-T06, W03-T01 | eligible | |
| W04-T02 | W02-T02, W04-T01 | blocked | |
| W04-T03 | W02-T01, W04-T01 | blocked | |
| W04-T04 | W02-T01, W04-T01 | blocked | |
| W04-T05 | W02-T01, W04-T01 | blocked | |
| W04-T06 | W01-T04, W03-T02, W04-T01 | blocked | |
| W04-T07 | W03-T05, W04-T01 | blocked | |
| W04-T08 | W02-T02, W03-T08, W04-T01 | blocked | |
| W05-T01 | W02-T05, W02-T08, W03-T02, W04-T01, W04-T02, W04-T03, W04-T04, W04-T05, W04-T06, W04-T07 | blocked | |
| W05-T02 | W05-T01 | blocked | |
| W05-T03 | W01-T04, W05-T01 | blocked | |
| W05-T04 | W00-T07, W03-T05, W05-T03 | blocked | |
| W05-T05 | W01-T04, W05-T03 | blocked | |
| W05-T06 | W03-T01, W05-T01 | blocked | |
| W05-T07 | W04-T02, W04-T03, W04-T04, W04-T05, W04-T06, W04-T07, W05-T01, W05-T03 | blocked | |
| W05-T08 | W01-T07, W05-T03, W05-T04 | blocked | |
| W06-T01 | W05-T03, W05-T06 | blocked | |
| W06-T02 | W05-T01, W05-T06 | blocked | |
| W06-T03 | W05-T03, W05-T07, W06-T02 | blocked | |
| W06-T04 | W05-T01, W06-T02 | blocked | |
| W06-T05 | W01-T04, W05-T06 | blocked | |
| W06-T06 | W06-T01, W06-T02, W06-T03, W06-T04, W06-T05 | blocked | |
| W06-T07 | W06-T06, W06-T08, W06-T09, W06-T10 | blocked | |
| W06-T08 | W05-T07, W06-T01, W06-T03, W06-T04 | blocked | |
| W06-T09 | W03-T04, W03-T07, W03-T08, W04-T02, W04-T05, W05-T01, W05-T03, W05-T07, W06-T03 | blocked | |
| W06-T10 | W06-T02, W06-T05 | blocked | |
| W07-T01 | W04-T08, W05-T01 | blocked | |
| W07-T02 | W04-T01, W02-T02 | blocked | |
| W07-T03 | W03-T05, W04-T06 | blocked | |
| W07-T04 | W04-T01, W06-T02 | blocked | |
| W07-T05 | W01-T02, W04-T02, W04-T03, W04-T04, W04-T05, W04-T06, W04-T07 | blocked | |
| W07-T06 | W00-T04, W06-T07 | blocked | |
| W07-T07 | W04-T01, W04-T07, W05-T01 | blocked | |
| W07-T08 | W00-T07, W03-T06, W04-T07, W05-T04 | blocked | |
| W07-T09 | W02-T07, W03-T08, W04-T07 | blocked | |
| W07-T10 | W03-T08, W04-T06, owner-approved compensation ADR | owner-gated | |
| W07-T11 | W01-T02, W05-T03, W06-T01, W06-T02 | blocked | |
