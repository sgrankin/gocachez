package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
	_ "modernc.org/sqlite"
)

// cacheSchemaVersion names the on-disk layout, and appears in the cache path, so
// binaries expecting different layouts use different directories and never open
// each other's catalog. Bumping it therefore starts cold rather than migrating:
// a build cache refills, and rewriting a multi-GiB catalog in place while six
// helpers may start at any moment buys nothing.
const cacheSchemaVersion = 2

// catalogSchema is complete: every version-2 catalog is created from it, so there
// is no migration path to keep. Adding to it later has to be additive, because
// the version in the path lets an older binary of the same version open the
// result; anything else bumps cacheSchemaVersion.
//
// outputs is the inventory of what is on disk: one row per blob, holding the facts
// that belong to the content rather than to any action that produced it. entries
// maps actions onto it.
//
// The reference is deliberately soft — no foreign key, and no index from an output
// back to the actions naming it. A dangling entry is just a miss, which get
// already handles for a blob that went missing, and that costs far less than the
// reverse index would: eviction and classification would otherwise fan out over
// the 29 actions a production cache maps onto each output, and answering "how big
// is the cache" meant a GROUP BY over every entry in it.
//
// entries.size is denormalised from outputs so a cache hit stays one primary-key
// seek with no join.
//
// IDs are stored as the raw digest rather than the 64-character hex that Go uses
// for paths. Hex doubles every copy of every key, and a production catalog spent
// 650MiB on the key index alone. WITHOUT ROWID removes that index entirely by
// making each table its own primary-key b-tree.
//
// STRICT is what keeps that decision honest. Without it a hex string bound to a
// BLOB column is stored as TEXT and never compares equal to the key it was meant
// to be, so the row is invisible rather than wrong and nothing reports a problem.
// STRICT turns that into "cannot store TEXT value in BLOB column".
const catalogSchema = `
CREATE TABLE IF NOT EXISTS outputs (
	output_id BLOB PRIMARY KEY,
	size INTEGER NOT NULL,
	compressed_size INTEGER NOT NULL,
	accessed_at INTEGER NOT NULL,
	blob_type INTEGER,
	blob_type_version INTEGER,
	retained_type INTEGER,
	retained_type_version INTEGER
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS outputs_accessed_at ON outputs(accessed_at, compressed_size);

CREATE TABLE IF NOT EXISTS entries (
	action_id BLOB PRIMARY KEY,
	output_id BLOB NOT NULL,
	size INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	accessed_at INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS entries_accessed_at ON entries(accessed_at);

CREATE TABLE IF NOT EXISTS runs (
	run_id TEXT PRIMARY KEY,
	path TEXT NOT NULL,
	lock_path TEXT NOT NULL,
	created_at INTEGER NOT NULL
) STRICT;
`

type entry struct {
	ActionID string
	OutputID string
	Size     int64
	// CompressedSize and AccessedAt are inputs to a put, recorded against the
	// output rather than the action. A lookup does not read them back — a cache hit
	// has no use for either, and fetching them would mean joining outputs — so they
	// are zero on anything lookupEntry returns.
	CompressedSize int64
	CreatedAt      time.Time
	AccessedAt     time.Time
}

type store struct {
	config

	db                *sql.DB
	q                 *catalog
	versionDir        string
	blobsDir          string
	liveRoot          string
	lifecycleLockPath string
	runID             string
	runDir            string
	runLock           *flock.Flock
	mu                sync.Mutex
	encoderPool       sync.Pool
	decoderPool       sync.Pool
	liveWriterPool    sync.Pool
	materialized      map[string]string
	accessed          map[string]int64
	accessedOutputs   map[string]int64
	accessFlushed     time.Time
	// installed counts compressed bytes this run put, so a build large enough
	// to overshoot maxSize can force a maintenance scan on the way out instead
	// of waiting for pruneInterval. It counts puts rather than growth — a put
	// of an output already in the cache adds nothing — which only ever errs
	// toward scanning sooner.
	installed atomic.Int64
}

const retainedDirName = "retained"

func newStore(cfg config) (*store, error) {
	versionDir, blobsDir, liveRoot, lifecycleLockPath := cachePaths(cfg)
	if err := os.MkdirAll(versionDir, 0o777); err != nil {
		return nil, fmt.Errorf("create version dir: %w", err)
	}

	var st *store
	err := withFileLock(lifecycleLockPath, func() error {
		var err error
		st, err = newStoreLocked(cfg, versionDir, blobsDir, liveRoot, lifecycleLockPath)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := st.cleanupAbandonedRuns(); err != nil {
		log.Printf("gocachez: cleanup abandoned runs failed: %v", err)
	}
	return st, nil
}

func cachePaths(cfg config) (string, string, string, string) {
	versionDir := cacheVersionDir(cfg)
	return versionDir,
		filepath.Join(versionDir, "blobs"),
		filepath.Join(versionDir, "live"),
		filepath.Join(versionDir, "lifecycle.lock")
}

func cacheVersionDir(cfg config) string {
	return filepath.Join(cfg.dir, fmt.Sprintf("v%d", cacheSchemaVersion))
}

func retainedRoot(versionDir string) string {
	return filepath.Join(versionDir, retainedDirName)
}

// pruneStampPath names the file whose mtime records the last completed
// maintenance scan. Only the mtime matters; the contents are always empty.
func pruneStampPath(versionDir string) string {
	return filepath.Join(versionDir, "prune.stamp")
}

// sweepStampPath names the file whose mtime records the last completed live-run
// sweep. The sweep has a stamp of its own because prune.stamp is only written
// when a scan actually deletes, which needs an idle cache: sharing one stamp
// would either make the sweep re-walk on every exit of a busy cache, or spend
// the scan's hourly budget on hours when it could not have pruned anyway.
func sweepStampPath(versionDir string) string {
	return filepath.Join(versionDir, "sweep.stamp")
}

const runDirPrefix = "run-"

// liveShardName spreads run directories over 100 shards. os.MkdirTemp names them
// with a decimal random suffix, whose low digits are uniform, so the last two
// characters need no hashing. Names are digits rather than the hex that blobs and
// retained files use, which also makes the layout a run directory came from
// obvious at a glance.
func liveShardName(runID string) string {
	if len(runID) < 2 {
		return "00"
	}
	return runID[len(runID)-2:]
}

// liveRunDirs lists every live run directory under liveRoot.
//
// Run directories live one shard deep — live/<xx>/run-* — for the same reason
// blobs and retained files do: nothing bounds how many accumulate if reclaim
// stops, and recovering from that should not mean churning a directory with tens
// of thousands of entries. Caches written before this keep them directly under
// live/, and those are exactly the ones with a backlog to clear, so both layouts
// are walked.
func liveRunDirs(liveRoot string) ([]string, error) {
	entries, err := os.ReadDir(liveRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read live dir: %w", err)
	}

	var dirs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), runDirPrefix) {
			dirs = append(dirs, filepath.Join(liveRoot, entry.Name()))
			continue
		}
		shard := filepath.Join(liveRoot, entry.Name())
		shardEntries, err := os.ReadDir(shard)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read live shard: %w", err)
		}
		for _, sub := range shardEntries {
			if sub.IsDir() && strings.HasPrefix(sub.Name(), runDirPrefix) {
				dirs = append(dirs, filepath.Join(shard, sub.Name()))
			}
		}
	}
	return dirs, nil
}

// maintenanceLockPath names the lock that admits one scanner at a time. It is
// separate from the lifecycle lock so that serialising scans against each other
// never makes a build wait to open the store.
func maintenanceLockPath(versionDir string) string {
	return filepath.Join(versionDir, "maintenance.lock")
}

func newStoreLocked(cfg config, versionDir, blobsDir, liveRoot, lifecycleLockPath string) (*store, error) {
	if err := os.MkdirAll(blobsDir, 0o777); err != nil {
		return nil, fmt.Errorf("create blobs dir: %w", err)
	}
	if err := os.MkdirAll(liveRoot, 0o777); err != nil {
		return nil, fmt.Errorf("create live dir: %w", err)
	}
	runDir, err := os.MkdirTemp(liveRoot, runDirPrefix)
	if err != nil {
		return nil, fmt.Errorf("create live run dir: %w", err)
	}
	// Move it into a shard. os.MkdirTemp picks the unique name, and deriving the
	// shard from that name keeps the choice in one place; failing the move leaves
	// the run flat, which liveRunDirs still finds.
	runID := filepath.Base(runDir)
	shard := filepath.Join(liveRoot, liveShardName(runID))
	if err := os.MkdirAll(shard, 0o777); err != nil {
		_ = os.RemoveAll(runDir)
		return nil, fmt.Errorf("create live shard dir: %w", err)
	}
	if sharded := filepath.Join(shard, runID); os.Rename(runDir, sharded) == nil {
		runDir = sharded
	}
	runLock := flock.New(filepath.Join(runDir, "run.lock"))
	if err := runLock.Lock(); err != nil {
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, fmt.Errorf("lock live run: %w", err)
	}

	db, err := openDB(filepath.Join(versionDir, "cache.db"))
	if err != nil {
		_ = runLock.Unlock()
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	st := &store{
		config:            cfg,
		db:                db,
		q:                 newCatalog(db),
		versionDir:        versionDir,
		blobsDir:          blobsDir,
		liveRoot:          liveRoot,
		lifecycleLockPath: lifecycleLockPath,
		runID:             runID,
		runDir:            runDir,
		runLock:           runLock,
		materialized:      make(map[string]string),
		accessed:          make(map[string]int64),
		accessedOutputs:   make(map[string]int64),
		accessFlushed:     time.Now(),
	}
	if err := st.q.prepare(context.Background()); err != nil {
		_ = st.q.close()
		_ = db.Close()
		_ = runLock.Unlock()
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	if err := st.registerRun(); err != nil {
		_ = st.q.close()
		_ = db.Close()
		_ = runLock.Unlock()
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	return st, nil
}

func (st *store) withLifecycleLock(fn func() error) error {
	return withFileLock(st.lifecycleLockPath, fn)
}

func withFileLock(path string, fn func() error) error {
	lock := flock.New(path)
	if err := lock.Lock(); err != nil {
		_ = lock.Close()
		return fmt.Errorf("lock cache lifecycle: %w", err)
	}
	return runUnlocking(lock, fn)
}

// withFileLockIfFree runs fn under path's lock, or reports false and runs
// nothing if another process holds it.
func withFileLockIfFree(path string, fn func() error) (bool, error) {
	lock := flock.New(path)
	locked, err := lock.TryLock()
	if err != nil {
		_ = lock.Close()
		return false, fmt.Errorf("lock cache lifecycle: %w", err)
	}
	if !locked {
		if closeErr := lock.Close(); closeErr != nil {
			return false, fmt.Errorf("close cache lifecycle lock: %w", closeErr)
		}
		return false, nil
	}
	return true, runUnlocking(lock, fn)
}

func runUnlocking(lock *flock.Flock, fn func() error) error {
	var err error
	err = errors.Join(err, fn())
	if unlockErr := lock.Unlock(); unlockErr != nil {
		err = errors.Join(err, fmt.Errorf("unlock cache lifecycle: %w", unlockErr))
	}
	if closeErr := lock.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close cache lifecycle lock: %w", closeErr))
	}
	return err
}

// readTuning maps the database rather than reading it through pread.
//
// Every cache hit is one seek into a b-tree whose index is 64-character hex, and on
// a catalog of a few GiB each seek is several page reads that miss SQLite's own
// cache. Mapping them costs no syscall and no copy into a buffer, which a profile of
// a busy host showed as the top cost by a distance: _copy_to_iter, xas_load and
// filemap_get_read_batch above every SQLite symbol, with the per-syscall overhead of
// seccomp and apparmor stacked on each one.
//
// Measured on a catalog of 1.5M entries with scattered lookups: 7.4-7.7us each
// without, 5.4-5.6us with, and more than that where syscalls are taxed. Raising
// cache_size instead was tried and is *worse* — 8.2-9.9us — because a large private
// cache costs more to manage than the shared page cache it duplicates. SQLite clamps
// the request to its compile-time maximum, and to the size of the file, so this is
// an upper bound rather than an allocation.
//
// Unlinking a mapped file is safe; it is truncation that raises SIGBUS, and nothing
// here truncates the catalog — clean removes it.
const readTuning = "&_pragma=mmap_size(2147483648)"

// walTuning bounds the write-ahead log on disk. SQLite resets the log and writes
// from the start again, but never shortens the file, so its high-water mark is
// permanent: one host was carrying 611MiB of dead log against 16MiB in use, which
// the 32KiB wal-index gives away.
const walTuning = "&_pragma=journal_size_limit(67108864)"

func openDB(path string) (*sql.DB, error) {
	dsn := "file:" + url.PathEscape(filepath.ToSlash(path)) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)" + readTuning + walTuning
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	conns := min(max(runtime.GOMAXPROCS(0), 1), 8)
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	if err := initDB(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func openExistingDB(path string) (*sql.DB, error) {
	dsn := "file:" + url.PathEscape(filepath.ToSlash(path)) + "?mode=ro&_pragma=busy_timeout(5000)" + readTuning
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("read catalog version: %w", err)
	}
	if version != cacheSchemaVersion {
		_ = db.Close()
		return nil, fmt.Errorf("unsupported catalog version %d, want %d", version, cacheSchemaVersion)
	}
	return db, nil
}

func initDB(db *sql.DB) error {
	ctx := context.Background()
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read catalog version: %w", err)
	}
	if version != 0 && version != cacheSchemaVersion {
		return fmt.Errorf("unsupported catalog version %d, want %d", version, cacheSchemaVersion)
	}
	if _, err := db.ExecContext(ctx, catalogSchema); err != nil {
		return fmt.Errorf("initialize catalog: %w", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, cacheSchemaVersion)); err != nil {
		return fmt.Errorf("write catalog version: %w", err)
	}
	return nil
}

func (st *store) close() {
	if err := st.flushAccessTimes(); err != nil && st.verbose {
		log.Printf("gocachez: flush access times failed: %v", err)
	}
	if err := st.unregisterRun(); err != nil && st.verbose {
		log.Printf("gocachez: unregister run failed: %v", err)
	}
	if err := st.prune(); err != nil && st.verbose {
		log.Printf("gocachez: prune failed: %v", err)
	}
	if err := st.q.close(); err != nil && st.verbose {
		log.Printf("gocachez: close prepared statements failed: %v", err)
	}
	if err := st.db.Close(); err != nil && st.verbose {
		log.Printf("gocachez: close catalog failed: %v", err)
	}
}

func (st *store) registerRun() error {
	now := unixMillis(time.Now())
	if err := st.q.registerRun(context.Background(), st.runID, st.runDir, st.runLock.Path(), now); err != nil {
		return fmt.Errorf("register run: %w", err)
	}
	return nil
}

func (st *store) unregisterRun() error {
	var err error
	if deleteErr := st.q.deleteRun(context.Background(), st.runID); deleteErr != nil {
		err = errors.Join(err, fmt.Errorf("delete run record: %w", deleteErr))
	}
	retainedLiveFiles, prepareErr := st.prepareLiveRunForClose()
	if prepareErr != nil {
		err = errors.Join(err, prepareErr)
	}
	if retainedLiveFiles && prepareErr == nil {
		now := time.Now()
		if touchErr := os.Chtimes(st.runLock.Path(), now, now); touchErr != nil {
			err = errors.Join(err, fmt.Errorf("timestamp retained live run: %w", touchErr))
		}
	}
	if unlockErr := st.runLock.Unlock(); unlockErr != nil {
		err = errors.Join(err, fmt.Errorf("unlock live run: %w", unlockErr))
	}
	if closeErr := st.runLock.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close live run lock: %w", closeErr))
	}
	if !retainedLiveFiles {
		if removeErr := os.RemoveAll(st.runDir); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remove live run dir: %w", removeErr))
		}
	}
	return err
}

// prepareLiveRunForClose keeps the live files whose paths can escape this build and
// removes the rest. Live files sit one shard deep (see createLiveFile), so this
// walks rather than reading a single directory; empty shards go at the end, since a
// retained run directory outlives the build and should not keep 256 of them.
func (st *store) prepareLiveRunForClose() (bool, error) {
	retained := false
	err := filepath.WalkDir(st.runDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("read live run dir: %w", err)
		}
		if d.IsDir() || d.Name() == "run.lock" {
			return nil
		}
		if !d.Type().IsRegular() {
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("remove live path: %w", err)
			}
			return nil
		}
		stripped, err := st.stripLivePackageArchiveToExport(path)
		if err != nil {
			return err
		}
		if stripped {
			retained = true
			return nil
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove live file: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return retained, removeEmptyDirs(st.runDir)
}

func (st *store) stripLivePackageArchiveToExport(path string) (bool, error) {
	outputID := liveOutputID(path)
	if outputID == "" {
		return stripPackageArchiveToExport(path, "")
	}
	retained, err := stripPackageArchiveToExport(path, st.retainedPath(outputID, ".a"))
	if err != nil {
		return false, err
	}
	if retained {
		if err := st.q.updateRetainedType(context.Background(), outputID, retainedTypeExportArchive); err != nil {
			if st.verbose {
				log.Printf("gocachez: cache retained file type failed: %v", err)
			}
		}
		return true, nil
	}
	kind, retained, err := retainEscapedGeneratedGoSource(path, st.retainedPath(outputID, ".go"))
	if err != nil || !retained {
		return retained, err
	}
	if err := st.q.updateRetainedType(context.Background(), outputID, kind); err != nil {
		if st.verbose {
			log.Printf("gocachez: cache retained file type failed: %v", err)
		}
	}
	return true, nil
}

func liveOutputID(path string) string {
	base := filepath.Base(path)
	outputID, _, ok := strings.Cut(base, "-")
	if !ok {
		return ""
	}
	return outputID
}

func (st *store) retainedDir(outputHex string) string {
	return filepath.Join(retainedRoot(st.versionDir), outputShard(outputHex))
}

func (st *store) retainedPath(outputHex, ext string) string {
	return filepath.Join(st.retainedDir(outputHex), outputHex+ext)
}

func (st *store) cleanupAbandonedRuns() error {
	runs, err := st.q.listOtherRuns(context.Background(), st.runID)
	if err != nil {
		return fmt.Errorf("query runs: %w", err)
	}

	// One run that cannot be reclaimed must not strand the rest. Every pass sees
	// the same rows in the same order, so returning here left a single bad row
	// shadowing every row behind it, in a loop whose error both hot callers drop
	// — reclaim would stop for good, silently, while live/ grew without bound.
	var errs error
	for _, run := range runs {
		reclaimed, err := st.tryReclaimRun(run.runID, run.path, run.lockPath)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		if reclaimed && st.verbose {
			log.Printf("gocachez: reclaimed abandoned live run %s", run.runID)
		}
	}
	return errs
}

func (st *store) tryReclaimRun(runID, runDir, lockPath string) (bool, error) {
	if _, err := os.Stat(runDir); errors.Is(err, os.ErrNotExist) {
		if err := st.q.deleteRun(context.Background(), runID); err != nil {
			return false, fmt.Errorf("delete missing-run record: %w", err)
		}
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("stat live run dir: %w", err)
	}

	runLock := flock.New(lockPath)
	locked, err := runLock.TryLock()
	if err != nil {
		_ = runLock.Close()
		return false, fmt.Errorf("try lock live run %s: %w", runID, err)
	}
	if !locked {
		_ = runLock.Close()
		return false, nil
	}

	if err := st.q.deleteRun(context.Background(), runID); err != nil {
		_ = runLock.Unlock()
		_ = runLock.Close()
		return false, fmt.Errorf("delete abandoned run record: %w", err)
	}
	if err := runLock.Unlock(); err != nil {
		_ = runLock.Close()
		return false, fmt.Errorf("unlock abandoned live run: %w", err)
	}
	if err := runLock.Close(); err != nil {
		return false, fmt.Errorf("close abandoned live run lock: %w", err)
	}
	if err := os.RemoveAll(runDir); err != nil {
		return false, fmt.Errorf("remove abandoned live run dir: %w", err)
	}
	return true, nil
}
