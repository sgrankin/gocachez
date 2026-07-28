# gocachez

`gocachez` is a [`GOCACHEPROG`](https://pkg.go.dev/cmd/go/internal/cacheprog)
helper for the Go build cache. It stores cache artifacts as zstd-compressed
files, materializes uncompressed files for the Go command on demand, and evicts
old compressed entries automatically.

This project was inspired by the discussion on
[`golang/go#76337`](https://github.com/golang/go/issues/76337).

This is a fork of
[`jakebailey/gocachez`](https://github.com/jakebailey/gocachez). It adds a
separate expiry for retained `go list` files, and changes maintenance so that a
build never waits for it: the scan is rate-limited, skips rather than queues
when another process holds the cache, and does its analysis without the lock
that a starting build needs.

Go `std`:

| Mode             |       Cold time |       Warm time | Disk usage |
| ---------------- | --------------: | --------------: | ---------: |
| No `GOCACHEPROG` |          36.31s |           4.06s |     1.60GB |
| `gocachez`       |          40.97s |           5.65s |      403MB |
|                  | +4.66s (+12.8%) | +1.59s (+39.1%) |     -74.8% |

[`typescript-go`](https://github.com/microsoft/typescript-go):

| Mode             |      Cold time |       Warm time | Disk usage |
| ---------------- | -------------: | --------------: | ---------: |
| No `GOCACHEPROG` |         89.20s |           4.76s |     4.95GB |
| `gocachez`       |         96.84s |           6.97s |      453MB |
|                  | +7.64s (+8.6%) | +2.21s (+46.4%) |     -90.9% |

## Installation

Requires Go 1.25 or newer.

```console
$ go install github.com/sgrankin/gocachez@latest
$ go env -w GOCACHEPROG=gocachez
```

Undo:

```console
$ go env -u GOCACHEPROG
```

## Commands

Show help:

```console
$ gocachez -h
```

Run as a `GOCACHEPROG` helper:

```console
$ gocachez
```

Remove inactive cache state:

```console
$ gocachez clean
```

`clean` removes blobs, live files, and catalog state that no active `gocachez`
process is using. State for active live runs is preserved.

Run maintenance now:

```console
$ gocachez prune
```

`prune` does the work that otherwise happens as a build's helper exits, without
waiting for the interval. It enforces `maxSize` and `maxAge` and reclaims unused
live run directories; it never removes anything a build might still be using.
Unlike `clean`, it does not empty the cache.

Show cache state:

```console
$ gocachez status
```

`status` reports the current cache configuration and state, including when each
maintenance pass last completed. It reads only the catalog and the directory
entries, so its cost does not depend on how much is stored.

Add `-types` for a breakdown of what the cache holds by kind:

```console
$ gocachez status -types
```

That one opens every blob it has no cached classification for, so on a large
cache it reads the whole thing. The classifications are cached in the catalog,
so a second run is much cheaper than the first.

## How it works

`gocachez` implements the Go command's external cache protocol over stdin and
stdout. The Go command sends `put` requests when it wants to store an artifact
and `get` requests when it wants to retrieve one.

On `put`, `gocachez`:

1. streams the artifact body into a zstd encoder,
2. writes the compressed bytes into `blobs/`,
3. writes the uncompressed body into the current run's `live/` directory, and
4. returns that live file path to the Go command as `DiskPath`.

The compressed blob is the durable cache entry. The live file is only the
temporary uncompressed file the Go command needs in order to read the artifact
from disk.

On `get`, `gocachez` looks up the action ID in SQLite. On a hit, it opens the
compressed blob, decompresses it into the current run's `live/` directory, and
returns the materialized path as `DiskPath`.

Live files are removed when the `GOCACHEPROG` process closes. If `gocachez`
exits abnormally, a later run can safely clean up abandoned live files: each run
holds an OS file lock, and another process only reclaims a run directory after
it can acquire that lock.

Some live files can escape the Go command process through `go list` output. For
example, `go list -export` reports an `Export` path that tools such as
`go/packages` and `golangci-lint` may open after the Go command has closed its
`GOCACHEPROG` helper. `go list -compiled` can also report generated cgo source
paths in `CompiledGoFiles`, and `go list -test` can report a generated
`_testmain.go` path in the synthetic test main package's `GoFiles`.

To support those tools without keeping large uncompressed archives around,
`gocachez` treats these escaped files specially on close. Package archives are
replaced with small archives containing only their `__.PKGDEF` export data, and
generated Go source files that can appear in list output are retained as-is.
These retained files are stored under `retained/`, keyed by output ID, and are
cleaned up once no catalog entry references that output.

## Configuration

By default, `gocachez` reads its config from:

```text
os.UserConfigDir()/gocachez/config.json
```

and stores cache data in:

```text
os.UserCacheDir()/gocachez
```

See [`os.UserConfigDir`](https://pkg.go.dev/os#UserConfigDir) and
[`os.UserCacheDir`](https://pkg.go.dev/os#UserCacheDir) for the
platform-specific base directories.

Example config:

```json
{
    "cacheDir": "/path/to/gocachez",
    "maxSize": "20GiB",
    "maxAge": "5d",
    "verbose": false
}
```

Config can also be selected explicitly:

```console
$ go env -w GOCACHEPROG="gocachez -config /path/to/config.json"
```

Supported options:

**Cache directory**

- Config: `cacheDir`
- Environment: `GOCACHEZ_DIR`
- Flag: `-dir`
- Default: `os.UserCacheDir()/gocachez`

**Maximum compressed blob size**

- Config: `maxSize`
- Environment: `GOCACHEZ_MAX_SIZE`
- Flag: `-max-size`
- Default: `20GiB`; `0` disables size-based pruning.

This budgets the compressed blobs only — the figure `gocachez status` reports as
"Blob max usage". Retained `go list` files are **not** counted against it; they
are reclaimed by `maxAge`, and by following their blob when it is evicted. They
are stored uncompressed, so on a cache of mostly Go package archives they can
approach the size of the blobs themselves. For what is actually on disk, check
the "Total stored" figure reported by `gocachez status`.

**Maximum age of unused entries**

- Config: `maxAge`
- Environment: `GOCACHEZ_MAX_AGE`
- Flag: `-max-age`
- Default: `5d`, matching `cmd/go`'s `GOCACHE`; `0` disables age-based pruning.
  Accepts a duration such as `5d`, `36h`, or `90m`.

**Maximum age of unused retained files**

- Config: `maxRetainedAge`
- Environment: `GOCACHEZ_MAX_RETAINED_AGE`
- Flag: `-max-retained-age`
- Default: whatever `maxAge` is. Unlike `maxAge`, `0` means "follow `maxAge`"
  rather than "disabled".

Retained files are not cache entries — they exist only so that paths which
escaped a finished build stay openable (see above). Losing one costs a re-strip
the next time its output is used, whereas losing a blob costs a rebuild, so they
are usually worth expiring far sooner than the cache itself. On a cache with
high key churn every rebuild of a package mints another one, and by default they
all linger for `maxAge`; that tail can rival the blobs in size.

Set this to roughly how long a tool might hold a path from `go list` after the
build that produced it finished. For CI, that is the length of a job. Locally it
is the length of an editor session, because `go/packages` consumers such as
`gopls` and `golangci-lint` can hold an `Export` path for as long as they run —
which is why the default leaves the previous behaviour alone. A file still in
use is safe: its mtime is refreshed as builds touch it, and the cutoff allows a
further `mtimeInterval` (1h) of slack on top of the configured age, so `1h`
expires at about two.

Age-based pruning is independent of `maxSize`: entries and retained files that
have not been used within `maxAge` are trimmed even while the cache is under its
size limit.

Size-based pruning evicts least-recently-used blobs, ranking an output by its
most recent access across every action that maps to it. The ranking is
approximate: rather than ordering the whole catalog, it walks the access-time
index oldest-first in bounded steps and stops once it has freed enough. On a
cache with high key churn most candidates are equally cold, so paying to rank
them exactly buys nothing.

Both limits are enforced by a maintenance scan that runs at most once an hour,
and only when no build is using the cache — the scan is proportional to the size
of the cache, and the `go` command waits for `gocachez` to exit, so running it
on every build would add that cost to every build. `maxSize` is therefore a
target rather than a hard cap: the cache can exceed it between scans. A single
build that writes a large fraction of `maxSize` scans on its way out instead of
waiting for the next interval, but many smaller builds can still drift past the
limit until the next scan brings the cache back under it. `gocachez prune` runs
the scan on request, which is how to keep even that cost off a build.

Reclaiming live run directories is separate, and waits only for the hourly
interval. Each one is guarded by its own lock rather than by the whole cache
being idle, so unlike the scan it still happens on a machine where builds
overlap continuously. Every clean exit that retains a `go list` path leaves its
directory behind on purpose, so they arrive at the rate of `go` invocations, and
the tree holds roughly `maxRetainedAge` plus the hour of mtime slack plus one
interval worth of them.

**Verbose maintenance logs**

- Config: `verbose`
- Environment: `GOCACHEZ_VERBOSE`
- Flag: `-v`
- Default: `false`

**Config file path**

- Environment: `GOCACHEZ_CONFIG`
- Flag: `-config`
- Default: `os.UserConfigDir()/gocachez/config.json`
- Missing file is an error when explicitly set.

## Continuous integration

CI differs from local use in three ways that change the right settings: nothing
holds a `go list` path for longer than a job, the cache is usually archived and
restored between runs, and several `go` commands may share one cache directory
at once.

### Installing the helper

Pin it as a tool of the project it builds, so the version lives in `go.mod`
alongside everything else:

```console
$ go get -tool github.com/sgrankin/gocachez
$ go install github.com/sgrankin/gocachez
```

Inside a module `go install` takes no `@version`: it uses that module's
`go.mod`, which is what makes the pin meaningful. The tradeoff is that
gocachez's dependencies land in your `go.sum` as indirect requirements. To keep
them out, install a pinned version standalone instead — from anywhere:

```console
$ go install github.com/sgrankin/gocachez@v0.0.0-...
```

Then point the `go` command at the installed binary. In CI prefer the
environment variable over `go env -w`, which persists to the user's environment
file:

```console
$ export GOCACHEPROG="$(go env GOPATH)/bin/gocachez -config /path/to/gocachez.json"
```

`GOCACHEPROG` is a command line, not just a path, so flags can go here instead
of a config file. Two things to get right:

- Install before exporting `GOCACHEPROG`, or the build that installs the helper
  goes looking for a helper that does not exist yet. `GOBIN` is empty unless you
  set it, in which case `go install` writes to `$(go env GOPATH)/bin`.
- Do not use `go tool gocachez` as the value. `go tool` consults `GOCACHEPROG`
  itself, so each invocation would spawn another.

Confirm the binary is recent enough for the settings below:

```console
$ gocachez -h | grep max-retained-age
```

### Recommended configuration

```json
{
    "maxSize": "250GiB",
    "maxAge": "5d",
    "maxRetainedAge": "1h"
}
```

- `maxSize` budgets the compressed blobs only. Size the volume for this plus the
  retained tree, or exclude the retained tree from what you archive (below).
- `maxAge` is what actually reclaims space on a cache with high key churn, where
  most entries are never read twice. Lower it if runs are frequent enough that
  five days of history is not buying hits.
- `maxRetainedAge` should cover the longest a job might hold a path printed by
  `go list`, which is bounded by the job itself. An hour is generous for most.
  **Do not carry this setting into local development**: `gopls` and other
  `go/packages` consumers hold `Export` paths for as long as they run, which is
  why the default leaves them alone.

Add `"verbose": true` while validating a rollout. Maintenance logs go to stderr
and do not interfere with the protocol, which runs over stdin and stdout.

### Archiving the cache between runs

Archive `v1/blobs/` and `v1/cache.db*`. Exclude these:

- `v1/retained/` — regenerated on use. Costs one re-strip per package the first
  time each is touched, and saves archiving an uncompressed copy of every
  package's export data.
- `v1/live/` — scratch space for one `GOCACHEPROG` process, keyed by a run
  directory no later run can use. After a clean exit it holds only the hard
  links that keep escaped `go list` paths alive, sharing inodes with
  `v1/retained/`, so an archiver that does not preserve hard links stores that
  data twice. After a killed process it can also hold full uncompressed
  artifacts.

Both are recreated as needed, so restoring without them is safe — verified by
archiving only `v1/blobs/` and `v1/cache.db`, restoring, and getting hits on
every entry with the retained files rebuilt on use.

### Do not run `clean` as a tidy-up step

`gocachez clean` is not compaction. When no `gocachez` process is using the
cache — the normal state between CI steps — it removes the blobs, the retained
tree, and the catalog. That is the whole cache. Use it to reset a cache, never
before archiving one.

`gocachez prune` is the command for that job. It enforces the limits and
reclaims unused live run directories, and leaves the cache usable.

### Optionally, prune as its own step

Nothing is required between runs — maintenance happens as processes exit. But
the `go` command waits for its helper to exit, so that work is charged to
whichever build happens to trigger it. Running it deliberately moves the cost
somewhere it does not matter:

```console
$ gocachez prune -config /path/to/gocachez.json
```

Best placed after the last `go` command in a job and before archiving, so the
archive is of a cache already under its limits. It reports on stdout if builds
are still registered, in which case it reclaims live run directories and leaves
the blobs alone; add `-v` to log what it removed. It exits 0 either way, so it
will not fail a job for being unable to do the full pass.

### Concurrency

Several `go` commands can share one cache directory. Each `gocachez` process
registers a run, and blobs and catalog entries are only ever deleted while none
are registered, so parallel jobs neither corrupt the cache nor block each other
— the expensive part of a maintenance scan runs without the lock that a starting
process needs. Live run directories are held by a lock of their own instead, so
reclaiming them does not wait for the cache to fall idle, which on a busy
machine it may never do.
