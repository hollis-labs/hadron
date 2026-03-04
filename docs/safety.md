# Hadron Safety Settings

Hadron reads `settings.json` from the data directory (`~/.hadron/settings.json`
by default) to control what blueprints are allowed to do.

## `settings.json` Location and Format

```
~/.hadron/settings.json
```

If the file does not exist, Hadron uses safe defaults (see below).

Full example:

```json
{
  "execution": {
    "allowedCommands": [],
    "deniedCommands": ["rm -rf /", "dd", "mkfs", "format", "shutdown", "reboot"],
    "allowedDirs": [],
    "deniedDirs": ["/", "/System", "/Library", "/bin", "/sbin", "/usr", "/etc"],
    "maxConcurrentJobs": 3,
    "defaultTimeout": 300,
    "workers": 3
  },
  "safety": {
    "requireConfirmation": true,
    "dryRunByDefault": false,
    "blockSudo": false,
    "sandboxMode": false
  },
  "telemetry": {
    "enabled": true,
    "retainDays": 30
  }
}
```

---

## Execution Settings

| Key | Type | Default | Description |
|---|---|---|---|
| `allowedCommands` | string[] | `[]` | If non-empty, only commands containing one of these strings are allowed |
| `deniedCommands` | string[] | `["rm -rf /", ...]` | Commands matching any of these substrings are blocked |
| `allowedDirs` | string[] | `[]` | If non-empty, `dir:` must be within one of these directories |
| `deniedDirs` | string[] | `["/", "/System", ...]` | Task `dir:` values that resolve inside these paths are blocked |
| `maxConcurrentJobs` | int | 3 | Maximum parallel runs (informational; workers is the enforced limit) |
| `defaultTimeout` | int | 300 | Default task timeout in seconds (0 = no timeout) |
| `workers` | int | 3 | Number of parallel worker goroutines |

---

## Safety Settings

| Key | Type | Default | Description |
|---|---|---|---|
| `requireConfirmation` | bool | true | Reserved for interactive confirmation flows |
| `dryRunByDefault` | bool | false | All runs are dry-runs unless `--dry-run=false` is passed |
| `blockSudo` | bool | false | Block any command containing `sudo` |
| `sandboxMode` | bool | false | Reserved for future process sandboxing |

---

## Telemetry Settings

| Key | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | true | Write JSONL telemetry to logs dir |
| `retainDays` | int | 30 | Auto-delete JSONL files older than N days |

When `enabled` is false, no `.jsonl` files are written (SQLite run events still work).

---

## Recommended Settings by Trust Level

### Personal Development (default — trusting)

```json
{
  "safety": { "blockSudo": false, "dryRunByDefault": false },
  "execution": { "deniedCommands": ["rm -rf /", "dd", "mkfs"] }
}
```

### Team / Shared Machine (moderate)

```json
{
  "safety": { "blockSudo": true, "dryRunByDefault": false },
  "execution": {
    "deniedCommands": ["rm -rf /", "dd", "mkfs", "shutdown", "reboot"],
    "deniedDirs": ["/", "/System", "/Library", "/bin", "/usr", "/etc", "/home"],
    "defaultTimeout": 120
  }
}
```

### CI / Automated (strict)

```json
{
  "safety": { "blockSudo": true, "dryRunByDefault": false, "sandboxMode": true },
  "execution": {
    "allowedCommands": ["make", "go ", "npm ", "yarn ", "pnpm "],
    "deniedDirs": ["/", "/System", "/Library", "/bin", "/usr", "/etc"],
    "defaultTimeout": 60,
    "workers": 1
  }
}
```
