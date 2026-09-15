-- queue_entries belonged to the obsolete legacy execution queue. Runtime
-- admission is graph-native now, so this intentionally removes any stale rows
-- in that table while preserving workflow run, scheduler, and resource state.
DROP TABLE IF EXISTS queue_entries;
