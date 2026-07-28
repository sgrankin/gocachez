package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/klauspost/compress/zstd"
)

var errInvalidCacheEntry = errors.New("invalid cache entry")

const (
	// mtimeInterval mirrors cmd/go's DiskCache: retained-file mtimes are updated
	// at most once per interval to avoid churn. The age cutoff itself is the
	// configurable maxAge (see config); the default matches GOCACHE's 5 days.
	mtimeInterval = time.Hour

	// pruneInterval bounds how often the maintenance scan runs. The scan is
	// O(cache) — it walks the retained tree, aggregates the catalog, and
	// reconciles every blob shard against it — and the go command waits for
	// gocachez to exit before it finishes, so running it on every build adds
	// that cost to every build. Its results barely change between builds, so
	// gating it costs nothing but latency. cmd/go gates its own cache trim the
	// same way, with a 24h interval; gocachez uses a shorter one because it
	// also enforces a size budget.
	//
	// The interval is a lower bound on the gap between scans, not an upper
	// bound: a scan also requires the cache to be momentarily idle (see
	// applyPrune), so a machine that always has a build running defers it.
	pruneInterval = time.Hour

	// pruneOvershootDivisor caps how far a *single* run can push the cache past
	// maxSize before it scans on the way out instead of waiting for
	// pruneInterval. It does not bound aggregate drift: several runs each
	// installing less than maxSize/pruneOvershootDivisor still add up between
	// interval-driven scans.
	pruneOvershootDivisor = 16

	// evictionSampleSize sets how far one eviction step reaches into
	// entries_accessed_at. Steps repeat until the overshoot is covered, trading a
	// few bounded queries for never ordering the whole catalog at once.
	//
	// It is a target rather than a hard cap: a step takes every entry sharing its
	// watermark timestamp, since stopping mid-timestamp would skip entries a
	// later step can no longer reach. Access times are milliseconds, so a step
	// only grows past this when many entries were touched in the same
	// millisecond.
	evictionSampleSize = 2048

	// accessFlushInterval bounds how long a cache hit stays invisible to the
	// catalog. Hits are recorded in memory and were flushed only on close, so a
	// build's reads did not exist as far as anything else was concerned until it
	// exited — and a killed helper lost them outright, which on a host that kills
	// helpers makes the entries most in use look like the coldest in the cache.
	//
	// The total rows written does not depend on this interval, only the number of
	// transactions, so it is set by how stale eviction data may be rather than by
	// write volume. Thirty seconds against an hourly maintenance scan.
	accessFlushInterval = 30 * time.Second

	// liveWriteBuffer batches writes to the live file, whose source hands over at
	// most 768 bytes at a time (see streamPutBody). Puts are serialised on the
	// protocol read loop, and the writers are pooled, so this is one buffer for
	// the process rather than one per put.
	liveWriteBuffer = 1 << 20
)

// encoderLevel trades put throughput for cache size. Measured over 900MiB of
// real go build output, relative to SpeedFastest: 10% smaller for 34% less
// single-core compression throughput, which end to end through the protocol is
// only 8% (129 -> 119 MiB/s) because puts are dominated by decoding the request
// body, not by zstd. Decompression — the cache-hit path, and far more frequent
// than puts — gets 9% faster (800 -> 872 MiB/s), since there is less to read.
// SpeedBestCompression is a cliff: 5 points more ratio for a third of the put
// throughput.
//
// Blobs already on disk keep whatever level wrote them; zstd frames are
// self-describing, so changing this needs no migration.
const encoderLevel = zstd.SpeedBetterCompression

var decoderOptions = []zstd.DOption{
	zstd.WithDecoderConcurrency(1),
	zstd.WithDecoderLowmem(true),
}

func (st *store) put(req request, br *bufio.Reader) (response, error) {
	actionHex, err := idHex("ActionID", req.ActionID)
	if err != nil {
		return response{}, err
	}
	outputHex, err := idHex("OutputID", req.OutputID)
	if err != nil {
		return response{}, err
	}
	body, err := bodyReader(br, req.BodySize)
	if err != nil {
		return response{}, err
	}
	bodyDrained := false
	defer func() {
		if !bodyDrained {
			_ = drainBody(body)
		}
	}()

	blobDir := st.blobDir(outputHex)
	if err := os.MkdirAll(blobDir, 0o777); err != nil {
		return response{}, fmt.Errorf("create blob dir: %w", err)
	}

	bodyPath, err := st.createLiveFile(outputHex)
	if err != nil {
		return response{}, err
	}
	keepBody := false
	defer func() {
		if !keepBody {
			_ = os.Remove(bodyPath)
		}
	}()

	blobTmp, err := os.CreateTemp(blobDir, outputHex+"-pending-*.zst")
	if err != nil {
		return response{}, fmt.Errorf("create compressed file: %w", err)
	}
	blobTmpPath := blobTmp.Name()
	defer func() {
		_ = os.Remove(blobTmpPath)
	}()

	bodyFile, err := os.Create(bodyPath)
	if err != nil {
		_ = blobTmp.Close()
		return response{}, fmt.Errorf("create live file: %w", err)
	}
	zw, err := st.getEncoder(blobTmp)
	if err != nil {
		_ = bodyFile.Close()
		_ = blobTmp.Close()
		return response{}, fmt.Errorf("create zstd encoder: %w", err)
	}

	written, copyErr := st.streamPutBody(bodyFile, zw, body)
	closeErr := zw.Close()
	st.putEncoder(zw)
	bodyCloseErr := bodyFile.Close()
	blobCloseErr := blobTmp.Close()
	if copyErr != nil {
		return response{}, copyErr
	}
	bodyDrained = true
	if closeErr != nil {
		return response{}, fmt.Errorf("finish zstd stream: %w", closeErr)
	}
	if bodyCloseErr != nil {
		return response{}, fmt.Errorf("close live file: %w", bodyCloseErr)
	}
	if blobCloseErr != nil {
		return response{}, fmt.Errorf("close compressed file: %w", blobCloseErr)
	}
	if written != req.BodySize {
		return response{}, fmt.Errorf("put body size mismatch: got %d bytes, expected %d", written, req.BodySize)
	}

	compressedSize, err := st.installBlob(blobTmpPath, outputHex)
	if err != nil {
		return response{}, err
	}
	st.installed.Add(compressedSize)

	now := time.Now()
	ent := entry{
		ActionID:       actionHex,
		OutputID:       outputHex,
		Size:           written,
		CompressedSize: compressedSize,
		CreatedAt:      now,
		AccessedAt:     now,
	}
	if err := st.upsertEntry(ent); err != nil {
		return response{}, err
	}
	keepBody = true
	st.setMaterialized(outputHex, bodyPath)

	return response{
		ID:       req.ID,
		DiskPath: bodyPath,
	}, nil
}

func (st *store) get(req request) (response, error) {
	actionHex, err := idHex("ActionID", req.ActionID)
	if err != nil {
		return response{}, err
	}
	ent, err := st.lookupEntry(actionHex)
	if errors.Is(err, sql.ErrNoRows) {
		return response{ID: req.ID, Miss: true}, nil
	}
	if err != nil {
		return response{}, err
	}

	path := st.getMaterialized(ent.OutputID)
	if path == "" || !regularFile(path) {
		path, err = st.materialize(ent)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, errInvalidCacheEntry) {
				if deleteErr := st.deleteOutput(ent.OutputID); deleteErr != nil && st.verbose {
					log.Printf("gocachez: delete bad cache output failed: %v", deleteErr)
				}
				return response{ID: req.ID, Miss: true}, nil
			}
			return response{}, err
		}
		st.setMaterialized(ent.OutputID, path)
	}

	st.markEntryAccess(actionHex)

	outputID, err := hex.DecodeString(ent.OutputID)
	if err != nil {
		return response{}, fmt.Errorf("decode output ID: %w", err)
	}
	return response{
		ID:       req.ID,
		OutputID: outputID,
		Size:     ent.Size,
		Time:     &ent.CreatedAt,
		DiskPath: path,
	}, nil
}

func (st *store) getMaterialized(outputID string) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.materialized[outputID]
}

func (st *store) setMaterialized(outputID, path string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.materialized[outputID] = path
}

func (st *store) deleteMaterialized(outputID string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.materialized, outputID)
}

func (st *store) markEntryAccess(actionID string) {
	now := time.Now()
	st.mu.Lock()
	st.accessed[actionID] = unixMillis(now)
	// Claim the flush while holding the lock, so concurrent gets do not each open
	// a transaction for the same batch.
	due := now.Sub(st.accessFlushed) >= accessFlushInterval
	if due {
		st.accessFlushed = now
	}
	st.mu.Unlock()

	if !due {
		return
	}
	// Deliberately on the caller's goroutine: gets already run on a bounded pool,
	// so one of them waiting out the flush is the backpressure.
	if err := st.flushAccessTimes(); err != nil && st.verbose {
		log.Printf("gocachez: flush access times failed: %v", err)
	}
}

func (st *store) flushAccessTimes() error {
	st.mu.Lock()
	accessed := st.accessed
	st.accessed = make(map[string]int64)
	st.mu.Unlock()
	if len(accessed) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin access-time transaction: %w", err)
	}
	if err := st.q.touchEntries(ctx, tx, accessed); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit access-time transaction: %w", err)
	}
	return nil
}

func (st *store) materialize(ent entry) (string, error) {
	bodyPath, err := st.createLiveFile(ent.OutputID)
	if err != nil {
		return "", err
	}
	keepBody := false
	defer func() {
		if !keepBody {
			_ = os.Remove(bodyPath)
		}
	}()

	blob, err := os.Open(st.blobPath(ent.OutputID))
	if err != nil {
		return "", err
	}
	defer blob.Close() //nolint:errcheck

	zr, err := st.getDecoder(blob)
	if err != nil {
		return "", fmt.Errorf("%w: create zstd decoder: %w", errInvalidCacheEntry, err)
	}
	defer st.putDecoder(zr)

	bodyFile, err := os.Create(bodyPath)
	if err != nil {
		return "", fmt.Errorf("create live file: %w", err)
	}

	written, copyErr := io.Copy(bodyFile, zr)
	closeErr := bodyFile.Close()
	if copyErr != nil {
		return "", fmt.Errorf("%w: decompress cache entry: %w", errInvalidCacheEntry, copyErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close live file: %w", closeErr)
	}
	if written != ent.Size {
		return "", fmt.Errorf("%w: decompressed size mismatch: got %d bytes, expected %d", errInvalidCacheEntry, written, ent.Size)
	}

	keepBody = true
	return bodyPath, nil
}

// streamPutBody writes the artifact to the live file and the compressed blob in
// one pass over the request body.
//
// The live file has to be buffered. body is a base64 decoder, and its Read
// returns at most 768 bytes however large the destination — outbuf is
// [1024 / 4 * 3]byte, bounded by the 1024-byte input buffer — so io.Copy's 32KiB
// buffer buys nothing, and writing straight to the *os.File turned a 32MiB
// artifact into 43,691 write syscalls at 767 bytes each. The zstd encoder
// buffers internally, so only this side needed it.
func (st *store) streamPutBody(live io.Writer, blob io.Writer, body io.Reader) (int64, error) {
	bw := st.getLiveWriter(live)
	defer st.putLiveWriter(bw)

	written, err := io.Copy(io.MultiWriter(bw, blob), body)
	if err != nil {
		return written, fmt.Errorf("read put body: %w", err)
	}
	if err := bw.Flush(); err != nil {
		return written, fmt.Errorf("write live file: %w", err)
	}
	return written, nil
}

func (st *store) getLiveWriter(w io.Writer) *bufio.Writer {
	if bw, ok := st.liveWriterPool.Get().(*bufio.Writer); ok {
		bw.Reset(w)
		return bw
	}
	return bufio.NewWriterSize(w, liveWriteBuffer)
}

func (st *store) putLiveWriter(bw *bufio.Writer) {
	// Reset before pooling, or the buffer keeps the live file reachable.
	bw.Reset(io.Discard)
	st.liveWriterPool.Put(bw)
}

func (st *store) getEncoder(w io.Writer) (*zstd.Encoder, error) {
	if enc, ok := st.encoderPool.Get().(*zstd.Encoder); ok {
		enc.Reset(w)
		return enc, nil
	}
	return zstd.NewWriter(w, zstd.WithEncoderLevel(encoderLevel), zstd.WithEncoderCRC(true))
}

func (st *store) putEncoder(enc *zstd.Encoder) {
	enc.Reset(io.Discard)
	st.encoderPool.Put(enc)
}

func (st *store) getDecoder(r io.Reader) (*zstd.Decoder, error) {
	if dec, ok := st.decoderPool.Get().(*zstd.Decoder); ok {
		if err := dec.Reset(r); err != nil {
			dec.Close()
			return nil, err
		}
		return dec, nil
	}
	return zstd.NewReader(r, decoderOptions...)
}

func (st *store) putDecoder(dec *zstd.Decoder) {
	_ = dec.Reset(bytes.NewReader(nil))
	st.decoderPool.Put(dec)
}

// createLiveFile makes the uncompressed file whose path is handed back to the go
// command. It is sharded within the run directory for the same reason blobs are:
// a build materialises one of these per output it touches, and a large one reached
// 28,000 in a single directory, where create and unlink churn costs filesystem
// metadata and journal IO rather than just lookups.
func (st *store) createLiveFile(outputHex string) (string, error) {
	dir := filepath.Join(st.runDir, outputShard(outputHex))
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return "", fmt.Errorf("create live shard dir: %w", err)
	}
	file, err := os.CreateTemp(dir, outputHex+"-*")
	if err != nil {
		return "", fmt.Errorf("create live file: %w", err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close live file placeholder: %w", err)
	}
	return path, nil
}

func (st *store) installBlob(tmpPath, outputHex string) (int64, error) {
	dst := st.blobPath(outputHex)
	if regularFile(dst) {
		return fileSize(dst)
	}
	err := os.Rename(tmpPath, dst)
	if err == nil {
		return fileSize(dst)
	}
	if regularFile(dst) {
		return fileSize(dst)
	}
	return 0, fmt.Errorf("install compressed file: %w", err)
}

func (st *store) upsertEntry(ent entry) error {
	if err := st.q.upsertEntry(context.Background(), ent); err != nil {
		return fmt.Errorf("upsert entry: %w", err)
	}
	return nil
}

func (st *store) lookupEntry(actionID string) (entry, error) {
	return st.q.lookupEntry(context.Background(), actionID)
}

func (st *store) deleteOutput(outputID string) error {
	if err := os.Remove(st.blobPath(outputID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove bad blob: %w", err)
	}
	if _, err := st.q.deleteEntriesByOutputID(context.Background(), outputID); err != nil {
		return fmt.Errorf("delete bad output entries: %w", err)
	}
	st.deleteMaterialized(outputID)
	return nil
}

// prune runs the maintenance scan when one is due.
//
// Deciding what to remove is the expensive part — it walks the retained tree,
// aggregates the catalog, and reconciles all 256 blob shards against it — and
// it runs without the lifecycle lock. newStore takes that lock, so holding it
// across the analysis is what stalled a starting build for the length of a
// whole scan. Only the deletions take it.
//
// What the lock guarantees is unchanged: nothing is removed unless the cache is
// idle. put and get never take it, so "no run is registered" is what makes
// removal safe, and applyPrune re-confirms that under the lock. Because the
// analysis ran unlocked, applyPrune also re-verifies what it is about to
// delete — see prunePlan.
//
// That invariant has one gap, and it predates the split: unregisterRun deletes
// its row before writing retained files and stamping run.lock, so a closing
// store can still touch the cache while counting as idle. It holds its own
// run.lock throughout, and entries for anything it retains are freshly
// accessed, so the reachable outcome is a lost retained file rather than a lost
// blob or a dangling row.
// pruneCache runs maintenance now rather than when the interval next allows it.
//
// Maintenance otherwise happens as a helper exits, and the go command waits for
// that, so its cost lands on a build. A CI job can run this between steps
// instead, where the same work costs nobody's build latency.
//
// It cannot delete blobs or entries while another build is registered — that is
// what makes deletion safe and is not something an explicit request can waive —
// so it says as much rather than reporting nothing and looking successful.
func pruneCache(cfg config, stdout io.Writer) error {
	versionDir, blobsDir, liveRoot, lifecycleLockPath := cachePaths(cfg)
	dbPath := filepath.Join(versionDir, "cache.db")
	if !regularFile(dbPath) {
		return nil
	}

	// Briefly under the lifecycle lock, as newStore opens it: initDB may still
	// have schema work to do, and nothing else serialises that.
	var db *sql.DB
	if err := withFileLock(lifecycleLockPath, func() error {
		var err error
		db, err = openDB(dbPath)
		return err
	}); err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck // read-mostly; the deletions below are already committed

	// Deliberately no registered run. countRuns counts every row including the
	// caller's, so a prune that registered itself would always find the cache
	// busy and decline.
	st := &store{
		config:            cfg,
		db:                db,
		q:                 newCatalog(db),
		versionDir:        versionDir,
		blobsDir:          blobsDir,
		liveRoot:          liveRoot,
		lifecycleLockPath: lifecycleLockPath,
	}
	// Reclaim before counting, for both halves of the answer. This is the only
	// path that frees a killed helper's directory without waiting for it to age
	// out, and it is what empties a live/ tree full of unstripped artifacts; it
	// needs no lifecycle lock, since each directory is taken on its own. Counting
	// after means the figure below is live builds rather than dead rows.
	if err := st.cleanupAbandonedRuns(); err != nil {
		log.Printf("gocachez: cleanup abandoned runs failed: %v", err)
	}
	active, err := st.q.countRuns(context.Background())
	if err != nil {
		return fmt.Errorf("count active runs: %w", err)
	}
	if active > 0 {
		if _, err := fmt.Fprintf(stdout, "gocachez: %d build(s) still using the cache; only live-run cleanup can run\n", active); err != nil {
			return fmt.Errorf("report active runs: %w", err)
		}
	}
	if err := clearMaintenanceStamps(versionDir); err != nil {
		return err
	}
	return st.prune()
}

func (st *store) prune() error {
	// Check the stamps before anything else: skipping is the common case.
	now := time.Now()
	sweepDue, pruneDue := st.sweepDue(now), st.pruneDue(now)
	if !sweepDue && !pruneDue {
		return nil
	}
	// Serialise maintenance on a lock of its own. Analysis is expensive and its
	// result is discarded if someone else stamps first, so a second scanner
	// should skip rather than duplicate the work. This is deliberately not the
	// lifecycle lock: a build opening a store must never wait on maintenance.
	ran, err := withFileLockIfFree(maintenanceLockPath(st.versionDir), func() error {
		if sweepDue {
			if err := st.sweepLiveRuns(now); err != nil {
				return err
			}
			if err := st.markSwept(); err != nil {
				return err
			}
		}
		if !pruneDue {
			return nil
		}
		return st.scan()
	})
	if err == nil && !ran && st.verbose {
		log.Print("gocachez: skipped maintenance, another process is already scanning")
	}
	return err
}

// sweepLiveRuns removes live run directories nothing is using any more.
//
// This is deliberately not part of the scan below. A run directory is guarded by
// its own run.lock, which liveRunExpiredIfUnlocked takes before removing
// anything — a more precise guard than "no run is registered anywhere", which is
// what blobs and entries need because they have no per-object lock. Sweeping
// under the scan's idleness check meant that on a machine where builds overlap
// continuously the check never passed, so a pass that needs no such check never
// ran and run directories accumulated indefinitely.
//
// Two very different things end up here. A clean exit that retains a go-list file
// leaves its directory behind on purpose, holding only hard links to retained/,
// so it costs almost nothing until its age is up. A killed helper leaves one that
// nothing stripped, holding full uncompressed artifacts — those are what fills
// live/, and cleanupAbandonedRuns is what should reclaim them promptly. This is
// the backstop for the ones it cannot see, whose runs row was already deleted.
func (st *store) sweepLiveRuns(now time.Time) error {
	if st.retainedAge() <= 0 {
		return nil
	}
	runDirs, err := liveRunDirs(st.liveRoot)
	if err != nil {
		return err
	}
	// A live run dir matters as much as the retained/ copy it links to: the path a
	// tool actually opened is the live one, so the inode outlives an expired
	// retained/ entry but not an expired live dir.
	cutoff := trimCutoff(st.retainedAge(), now)
	removed := 0
	for _, runDir := range runDirs {
		expired, err := liveRunExpiredIfUnlocked(runDir, cutoff)
		if err != nil {
			return err
		}
		if !expired {
			continue
		}
		if err := os.RemoveAll(runDir); err != nil {
			return fmt.Errorf("remove old retained live run: %w", err)
		}
		removed++
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d old retained live runs", removed)
	}
	return nil
}

func (st *store) sweepDue(now time.Time) bool {
	info, err := os.Stat(sweepStampPath(st.versionDir))
	if err != nil {
		return true
	}
	age := now.Sub(info.ModTime())
	return age >= pruneInterval || age < 0
}

func (st *store) markSwept() error {
	if err := os.WriteFile(sweepStampPath(st.versionDir), nil, 0o666); err != nil {
		return fmt.Errorf("write sweep stamp: %w", err)
	}
	return nil
}

// clearMaintenanceStamps makes every pass due again. Removing the stamps is how
// the prune command forces maintenance: the interval is checked both before the
// maintenance lock is taken and again inside applyPrune, and an absent stamp
// reads as due in both places.
func clearMaintenanceStamps(versionDir string) error {
	for _, path := range []string{pruneStampPath(versionDir), sweepStampPath(versionDir)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("clear maintenance stamp: %w", err)
		}
	}
	return nil
}

func (st *store) scan() error {
	// Analysing a cache that other builds are still using is wasted work, and
	// applyPrune would decline to delete anyway. A run row left behind by a
	// killed helper defers the scan to the next exit, which reclaims it in
	// newStore; the authoritative check happens under the lifecycle lock.
	active, err := st.q.countRuns(context.Background())
	if err != nil {
		return fmt.Errorf("count active runs: %w", err)
	}
	if active > 0 {
		return nil
	}

	plan, err := st.planPrune(time.Now())
	if err != nil {
		return err
	}
	return st.applyPrune(plan)
}

// pruneDue reports whether the maintenance scan should run. A stamp dated in
// the future — clock skew, a restored backup — counts as due, so a bad
// timestamp can never lock maintenance out permanently.
func (st *store) pruneDue(now time.Time) bool {
	if st.maxSize > 0 && st.installed.Load() > st.maxSize/pruneOvershootDivisor {
		return true
	}
	info, err := os.Stat(pruneStampPath(st.versionDir))
	if err != nil {
		return true
	}
	age := now.Sub(info.ModTime())
	return age >= pruneInterval || age < 0
}

func (st *store) markPruned() error {
	if err := os.WriteFile(pruneStampPath(st.versionDir), nil, 0o666); err != nil {
		return fmt.Errorf("write prune stamp: %w", err)
	}
	st.installed.Store(0)
	return nil
}

// prunePlan is what a scan decided to remove. It is computed without the
// lifecycle lock, so a build can have started, written, and finished while it
// was being built. applyPrune therefore re-verifies each group under the lock
// rather than trusting the plan:
//
//   - entries: the DELETE re-evaluates accessed_at, so an entry a build touched
//     no longer matches.
//   - retained files and live runs: their mtimes are re-read, so anything a
//     build refreshed is kept.
//   - blobs: the cache size is re-read, so eviction stops as soon as the cache
//     is back under budget.
//   - orphans: the shards holding candidates are re-queried, so a file that has
//     since gained a catalog entry is kept. This is the one that would otherwise
//     matter — a put writes its blob before inserting the row, so a blob can be
//     unreferenced during the walk and referenced by the time we delete.
type prunePlan struct {
	// retainedCutoff is the age boundary the retained groups were built against.
	// Deletion re-reads mtimes but compares them to this, so the re-check can
	// only ever keep more than the plan chose, never less.
	retainedCutoff time.Time
	entryCutoff    int64            // delete entries unused since this; 0 for none
	retained       []string         // expired retained files
	blobs          []pruneCandidate // over-budget blobs, least recently used first
	orphans        map[int][]string // shard index -> files with no catalog entry
}

func (p prunePlan) empty() bool {
	return p.entryCutoff == 0 &&
		len(p.retained) == 0 &&
		len(p.blobs) == 0 &&
		len(p.orphans) == 0
}

func (st *store) planPrune(now time.Time) (prunePlan, error) {
	plan := prunePlan{retainedCutoff: trimCutoff(st.retainedAge(), now)}
	var err error
	if plan.entryCutoff, err = st.planOldEntries(now); err != nil {
		return prunePlan{}, err
	}
	if plan.retained, err = st.planOldRetainedFiles(now); err != nil {
		return prunePlan{}, err
	}
	if plan.blobs, err = st.planToMaxSize(plan.entryCutoff); err != nil {
		return prunePlan{}, err
	}
	// Entries this scan is about to delete must not count as references, or the
	// blobs they were the last holder of would survive until the next scan.
	if plan.orphans, err = st.planOrphans(true, plan.entryCutoff); err != nil {
		return prunePlan{}, err
	}
	return plan, nil
}

// applyPrune performs the plan's deletions under the lifecycle lock, or does
// nothing if the cache turned out to be in use or another process got there
// first. The stamp is only written when a scan actually completed, so a declined
// attempt is retried by the next process to exit past the interval.
func (st *store) applyPrune(plan prunePlan) error {
	applied, err := withFileLockIfFree(st.lifecycleLockPath, func() error {
		if err := st.cleanupAbandonedRuns(); err != nil {
			log.Printf("gocachez: cleanup abandoned runs failed: %v", err)
		}
		active, err := st.q.countRuns(context.Background())
		if err != nil {
			return fmt.Errorf("count active runs: %w", err)
		}
		if active > 0 {
			return nil
		}
		// Another process may have finished a scan while this one was planning.
		if !st.pruneDue(time.Now()) {
			return nil
		}
		if err := st.deletePlanned(plan); err != nil {
			return err
		}
		return st.markPruned()
	})
	if err == nil && !applied && st.verbose {
		log.Print("gocachez: skipped maintenance, another process holds the cache lock")
	}
	return err
}

func (st *store) deletePlanned(plan prunePlan) error {
	if plan.empty() {
		return nil
	}
	if err := st.pruneOldEntries(plan.entryCutoff); err != nil {
		return err
	}
	if err := st.removeExpiredRetainedFiles(plan.retained, plan.retainedCutoff); err != nil {
		return err
	}
	evicted, err := st.evictToMaxSize(plan.blobs)
	if err != nil {
		return err
	}
	// The plan's orphan list was built before eviction, so an evicted output was
	// still referenced then and its retained files were not candidates. Offer
	// them now; removeOrphans re-queries the shard, so anything still referenced
	// is kept.
	st.addRetainedCandidates(plan.orphans, evicted)
	if err := st.removeOrphans(plan.orphans); err != nil {
		return err
	}
	if err := removeEmptyDirs(st.blobsDir); err != nil {
		return err
	}
	return removeEmptyDirs(retainedRoot(st.versionDir))
}

// planToMaxSize lists blobs to evict, least recently used first, or nothing if
// the cache is within budget.
//
// It walks entries_accessed_at oldest-first in bounded steps and stops once it
// has enough candidates to cover the overshoot, rather than ordering the whole
// catalog. Exact LRU cost a full GROUP BY plus a sort — 344ms and 200k rows at
// 200k outputs — to rank candidates that in a cache with high key churn are
// almost all equally dead. Overshooting the estimate is harmless: evictToMaxSize
// re-reads the real size and stops at the budget.
func (st *store) planToMaxSize(entryCutoff int64) ([]pruneCandidate, error) {
	if st.maxSize <= 0 {
		return nil, nil
	}
	total, err := st.compressedSize()
	if err != nil {
		return nil, err
	}
	if total <= st.maxSize {
		return nil, nil
	}

	// Start at the age cutoff: everything older is being deleted anyway, so
	// listing it for eviction is wasted work. need is therefore an
	// overestimate, since the age delete frees bytes this does not count.
	ctx := context.Background()
	cursor := entryCutoff
	need := total - st.maxSize
	var candidates []pruneCandidate
	for need > 0 {
		watermark, ok, err := st.q.oldestAccessWatermark(ctx, cursor, evictionSampleSize)
		if err != nil {
			return nil, fmt.Errorf("find eviction watermark: %w", err)
		}
		if !ok {
			break
		}
		batch, err := st.q.evictionCandidates(ctx, cursor, watermark)
		if err != nil {
			return nil, fmt.Errorf("query eviction candidates: %w", err)
		}
		for _, candidate := range batch {
			candidates = append(candidates, candidate)
			need -= candidate.size
			if need <= 0 {
				break
			}
		}
		// Past the watermark, so a step whose outputs all had fresher entries
		// still makes progress instead of re-reading the same region.
		cursor = watermark + 1
	}
	return candidates, nil
}

// evictToMaxSize removes least-recently-used blobs until the cache is within
// budget, and returns the outputs it evicted.
func (st *store) evictToMaxSize(candidates []pruneCandidate) ([]string, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	// Re-read the size rather than trusting the planned total: a build that ran
	// during the analysis may have freed or added blobs, and stopping at the
	// real budget keeps this from over-evicting.
	total, err := st.compressedSize()
	if err != nil {
		return nil, err
	}
	var evicted []string
	for _, candidate := range candidates {
		if total <= st.maxSize {
			break
		}
		// Candidates were chosen from the catalog as it was before this pass
		// deleted expired entries, so some of them are already gone. Their bytes
		// are not in the total that was just read, and crediting them anyway
		// would end the loop early and leave the cache over budget — which is
		// the whole point of eviction. The blobs are not leaked: an output with
		// no surviving entries is in plan.orphans.
		rows, err := st.q.deleteEntriesByOutputID(context.Background(), candidate.outputID)
		if err != nil {
			return nil, fmt.Errorf("delete pruned entries: %w", err)
		}
		if rows == 0 {
			continue
		}
		if err := os.Remove(st.blobPath(candidate.outputID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("remove compressed entry: %w", err)
		}
		total -= candidate.size
		evicted = append(evicted, candidate.outputID)
	}
	if st.verbose && len(evicted) > 0 {
		log.Printf("gocachez: pruned %d blobs, compressed size now %s", len(evicted), formatSize(total))
	}
	return evicted, nil
}

// keepEveryEntry disables the access-time filter in referencedInShard. Zero
// would not do: accessed_at can be negative if the clock was ever behind 1970,
// and pruneDue already anticipates a skewed clock.
const keepEveryEntry = math.MinInt64

type pruneCandidate struct {
	outputID string
	size     int64
}

func (st *store) compressedSize() (int64, error) {
	total, err := st.q.compressedSize(context.Background())
	if err != nil {
		return 0, fmt.Errorf("calculate compressed size: %w", err)
	}
	return total, nil
}

// planOldEntries returns the cutoff to delete by, or 0 when no entry is old
// enough to be worth taking the lock for. MIN(accessed_at) is index-backed, so
// answering that costs one seek rather than the DELETE's scan.
func (st *store) planOldEntries(now time.Time) (int64, error) {
	if st.maxAge <= 0 {
		return 0, nil
	}
	cutoff := unixMillis(trimCutoff(st.maxAge, now))
	oldest, ok, err := st.q.oldestAccess(context.Background())
	if err != nil {
		return 0, fmt.Errorf("find oldest entry access: %w", err)
	}
	if !ok || oldest >= cutoff {
		return 0, nil
	}
	return cutoff, nil
}

func (st *store) pruneOldEntries(cutoff int64) error {
	if cutoff == 0 {
		return nil
	}
	removed, err := st.q.deleteEntriesAccessedBefore(context.Background(), cutoff)
	if err != nil {
		return fmt.Errorf("prune old entries: %w", err)
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d entries not used in %s", removed, st.maxAge)
	}
	return nil
}

func (st *store) planOldRetainedFiles(now time.Time) ([]string, error) {
	if st.retainedAge() <= 0 {
		return nil, nil
	}
	root := retainedRoot(st.versionDir)
	cutoff := trimCutoff(st.retainedAge(), now)
	var expired []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// The walk runs without the lifecycle lock, so another process's
			// scan can take a file or a shard from under it; the root may also
			// never have existed.
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || (!strings.HasSuffix(path, ".a") && !strings.HasSuffix(path, ".go")) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("stat retained file: %w", err)
		}
		if info.ModTime().Before(cutoff) {
			expired = append(expired, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return expired, nil
}

func (st *store) removeExpiredRetainedFiles(paths []string, cutoff time.Time) error {
	if len(paths) == 0 {
		return nil
	}
	removed := 0
	for _, path := range paths {
		// Re-read the mtime: a build that ran while this scan was planning may
		// have used the file, and markRetainedFileUsed would have refreshed it.
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("stat retained file: %w", err)
		}
		if !info.ModTime().Before(cutoff) {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove old retained file: %w", err)
		}
		removed++
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d retained files not used in %s", removed, st.retainedAge())
	}
	return nil
}

func liveRunExpiredIfUnlocked(runDir string, cutoff time.Time) (bool, error) {
	runLock := flock.New(filepath.Join(runDir, "run.lock"), flock.SetFlag(os.O_RDONLY))
	locked, err := runLock.TryLock()
	if err != nil {
		_ = runLock.Close()
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("try lock retained live run %s: %w", filepath.Base(runDir), err)
	}
	if !locked {
		_ = runLock.Close()
		return false, nil
	}
	expired, expireErr := retainedLiveRunExpired(runDir, cutoff)
	unlockErr := runLock.Unlock()
	closeErr := runLock.Close()
	if expireErr != nil {
		return false, expireErr
	}
	if unlockErr != nil {
		return false, fmt.Errorf("unlock retained live run: %w", unlockErr)
	}
	if closeErr != nil {
		return false, fmt.Errorf("close retained live run lock: %w", closeErr)
	}
	return expired, nil
}

func retainedLiveRunExpired(runDir string, cutoff time.Time) (bool, error) {
	info, err := os.Stat(filepath.Join(runDir, "run.lock"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat retained live run: %w", err)
	}
	// Retained files are hard-linked into live runs, so their mtimes cannot
	// represent the age of a particular run. The lock file is unique to the run
	// and timestamped when that run closes.
	return info.ModTime().Before(cutoff), nil
}

func trimCutoff(maxAge time.Duration, now time.Time) time.Time {
	return now.Add(-maxAge - mtimeInterval)
}

func markRetainedFileUsed(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	now := time.Now()
	if now.Sub(info.ModTime()) < mtimeInterval {
		return nil
	}
	return os.Chtimes(path, now, now)
}

// planOrphans lists output files with no catalog entry, keyed by shard so
// removeOrphans can re-check just the shards that turned something up.
//
// Output files are partitioned by the first byte of their hex ID, so one shard
// at a time keeps memory bounded without per-file SQL.
func (st *store) planOrphans(includeBlobs bool, entryCutoff int64) (map[int][]string, error) {
	referenced := make(map[string]struct{})
	orphans := make(map[int][]string)
	for shard := range 256 {
		if err := st.referencedInShard(shard, entryCutoff, referenced); err != nil {
			return nil, err
		}
		prefix := shardPrefix(shard)
		var found []string
		if includeBlobs {
			paths, err := orphanFilesInDir(filepath.Join(st.blobsDir, prefix), referenced, ".zst")
			if err != nil {
				return nil, fmt.Errorf("scan blobs in shard %s: %w", prefix, err)
			}
			found = append(found, paths...)
		}
		paths, err := orphanFilesInDir(filepath.Join(retainedRoot(st.versionDir), prefix), referenced, ".a", ".go")
		if err != nil {
			return nil, fmt.Errorf("scan retained files in shard %s: %w", prefix, err)
		}
		found = append(found, paths...)
		if len(found) > 0 {
			orphans[shard] = found
		}
	}
	return orphans, nil
}

func (st *store) removeOrphans(orphans map[int][]string) error {
	if len(orphans) == 0 {
		return nil
	}
	referenced := make(map[string]struct{})
	for _, shard := range slices.Sorted(maps.Keys(orphans)) {
		// A put writes its blob before inserting the catalog row, so a file that
		// looked unreferenced during the unlocked scan may be referenced by now.
		// Only shards with candidates are re-queried, which is why a healthy
		// cache does no work here at all.
		// pruneOldEntries has already run, so nothing is below the cutoff any
		// more and an unfiltered query is both simpler and the honest check.
		if err := st.referencedInShard(shard, keepEveryEntry, referenced); err != nil {
			return err
		}
		for _, path := range orphans[shard] {
			outputID := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
			if _, ok := referenced[outputID]; ok {
				continue
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove orphan file in shard %s: %w", shardPrefix(shard), err)
			}
		}
	}
	return nil
}

func shardPrefix(shard int) string {
	return fmt.Sprintf("%02x", shard)
}

// addRetainedCandidates offers the retained files of outputs that this pass
// deleted itself, which the plan could not have known about. Paths that do not
// exist are harmless — removeOrphans tolerates a missing file — and the shard is
// re-queried there, so anything still referenced is kept.
func (st *store) addRetainedCandidates(orphans map[int][]string, outputIDs []string) {
	for _, outputID := range outputIDs {
		shard, err := strconv.ParseInt(outputID[:min(2, len(outputID))], 16, 32)
		if err != nil {
			// Not one of ours: retainedPath would file it under "xx".
			continue
		}
		for _, ext := range []string{".a", ".go"} {
			orphans[int(shard)] = append(orphans[int(shard)], st.retainedPath(outputID, ext))
		}
	}
}

// referencedInShard replaces outputIDs with the output IDs that the given
// shard's entries reference. Entries accessed before minAccessedAt are ignored,
// which lets a scan plan around the entries it is about to delete; pass
// keepEveryEntry to count all of them.
func (st *store) referencedInShard(shard int, minAccessedAt int64, outputIDs map[string]struct{}) error {
	lower := shardPrefix(shard)
	upper := shardPrefix(shard + 1)
	if shard == 255 {
		// Hex digits all sort below 'g', so this is the open upper bound.
		upper = "g"
	}
	if err := st.q.referencedOutputIDs(context.Background(), lower, upper, minAccessedAt, outputIDs); err != nil {
		return fmt.Errorf("query referenced outputs in shard %s: %w", lower, err)
	}
	return nil
}

func orphanFilesInDir(root string, referenced map[string]struct{}, extensions ...string) ([]string, error) {
	var orphans []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		ext := filepath.Ext(path)
		if !slices.Contains(extensions, ext) {
			return nil
		}
		outputID := strings.TrimSuffix(filepath.Base(path), ext)
		if _, ok := referenced[outputID]; !ok {
			orphans = append(orphans, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return orphans, nil
}

func removeEmptyDirs(root string) error {
	var dirs []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() && path != root {
			dirs = append(dirs, path)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, dir := range slices.Backward(dirs) {
		if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
			if entries, readErr := os.ReadDir(dir); readErr != nil || len(entries) != 0 {
				continue
			}
		}
	}
	return nil
}

func (st *store) blobDir(outputHex string) string {
	return blobDir(st.blobsDir, outputHex)
}

func blobDir(blobsDir, outputHex string) string {
	return filepath.Join(blobsDir, outputShard(outputHex))
}

// outputShard spreads content-addressed files over 256 directories by the first
// byte of their output ID. Anything without one lands together under "xx", which
// is a name no hex prefix can take.
func outputShard(outputHex string) string {
	if len(outputHex) < 2 {
		return "xx"
	}
	return outputHex[:2]
}

func (st *store) blobPath(outputHex string) string {
	return blobPath(st.blobsDir, outputHex)
}

func blobPath(blobsDir, outputHex string) string {
	return filepath.Join(blobDir(blobsDir, outputHex), outputHex+".zst")
}
