# Deferred Findings Register

No execution findings have been deferred.

## Known limitations

- W00-T07 intentionally leaves stale-claim recovery policy outside
  `go-scheduler`: a process failure after `ClaimFire` and before a terminal or
  retry transition can leave a fire in `claimed`. Hadron's later scheduler and
  recovery binding must define lease/reclaim policy without weakening stable
  fire identity or per-attempt CAS guarantees. This does not block W00-T07's
  accepted contract, but must be reconciled before W05-T04/W03-T06 integration
  is considered durable.
