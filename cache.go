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
	"os"
	"path/filepath"
	"slices"
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

	written, copyErr := io.Copy(io.MultiWriter(bodyFile, zw), body)
	closeErr := zw.Close()
	st.putEncoder(zw)
	bodyCloseErr := bodyFile.Close()
	blobCloseErr := blobTmp.Close()
	if copyErr != nil {
		return response{}, fmt.Errorf("read put body: %w", copyErr)
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
	st.mu.Lock()
	defer st.mu.Unlock()
	st.accessed[actionID] = unixMillis(time.Now())
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

func (st *store) createLiveFile(outputHex string) (string, error) {
	file, err := os.CreateTemp(st.runDir, outputHex+"-*")
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
	if err := st.q.deleteEntriesByOutputID(context.Background(), outputID); err != nil {
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
func (st *store) prune() error {
	// Check the stamp before anything else: skipping is the common case.
	if !st.pruneDue(time.Now()) {
		return nil
	}
	// Analysing a cache that other builds are still using is wasted work, and
	// applyPrune would decline to delete anyway. Purely an optimisation — the
	// authoritative check happens under the lock.
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
	entryCutoff int64            // delete entries unused since this; 0 for none
	retained    []string         // expired retained files
	liveRuns    []string         // expired retained live run dirs
	blobs       []pruneCandidate // over-budget blobs, least recently used first
	orphans     map[int][]string // shard index -> files with no catalog entry
}

func (p prunePlan) empty() bool {
	return p.entryCutoff == 0 &&
		len(p.retained) == 0 &&
		len(p.liveRuns) == 0 &&
		len(p.blobs) == 0 &&
		len(p.orphans) == 0
}

func (st *store) planPrune(now time.Time) (prunePlan, error) {
	var plan prunePlan
	var err error
	if plan.entryCutoff, err = st.planOldEntries(now); err != nil {
		return prunePlan{}, err
	}
	if plan.retained, err = st.planOldRetainedFiles(now); err != nil {
		return prunePlan{}, err
	}
	if plan.liveRuns, err = st.planOldRetainedLiveDirs(now); err != nil {
		return prunePlan{}, err
	}
	if plan.blobs, err = st.planToMaxSize(); err != nil {
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
		if err := st.cleanupAbandonedRuns(); err != nil && st.verbose {
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
	if err := st.removeExpiredRetainedFiles(plan.retained); err != nil {
		return err
	}
	if err := st.removeExpiredLiveRuns(plan.liveRuns); err != nil {
		return err
	}
	if err := st.evictToMaxSize(plan.blobs); err != nil {
		return err
	}
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
func (st *store) planToMaxSize() ([]pruneCandidate, error) {
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
	return st.pruneCandidates()
}

func (st *store) evictToMaxSize(candidates []pruneCandidate) error {
	if len(candidates) == 0 {
		return nil
	}
	// Re-read the size rather than trusting the planned total: a build that ran
	// during the analysis may have freed or added blobs, and stopping at the
	// real budget keeps this from over-evicting.
	total, err := st.compressedSize()
	if err != nil {
		return err
	}
	removed := 0
	for _, candidate := range candidates {
		if total <= st.maxSize {
			break
		}
		if err := os.Remove(st.blobPath(candidate.outputID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove compressed entry: %w", err)
		}
		if err := st.q.deleteEntriesByOutputID(context.Background(), candidate.outputID); err != nil {
			return fmt.Errorf("delete pruned entries: %w", err)
		}
		total -= candidate.size
		removed++
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d blobs, compressed size now %s", removed, formatSize(total))
	}
	return nil
}

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

func (st *store) pruneCandidates() ([]pruneCandidate, error) {
	candidates, err := st.q.pruneCandidates(context.Background())
	if err != nil {
		return nil, fmt.Errorf("query prune candidates: %w", err)
	}
	return candidates, nil
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
	if st.maxAge <= 0 {
		return nil, nil
	}
	root := retainedRoot(st.versionDir)
	cutoff := trimCutoff(st.maxAge, now)
	var expired []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
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

func (st *store) removeExpiredRetainedFiles(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	cutoff := trimCutoff(st.maxAge, time.Now())
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
		log.Printf("gocachez: pruned %d old retained files", removed)
	}
	return nil
}

func (st *store) planOldRetainedLiveDirs(now time.Time) ([]string, error) {
	if st.maxAge <= 0 {
		return nil, nil
	}
	entries, err := os.ReadDir(st.liveRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read live dir: %w", err)
	}
	cutoff := trimCutoff(st.maxAge, now)
	var expired []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runDir := filepath.Join(st.liveRoot, entry.Name())
		isExpired, err := liveRunExpiredIfUnlocked(runDir, cutoff)
		if err != nil {
			return nil, err
		}
		if isExpired {
			expired = append(expired, runDir)
		}
	}
	return expired, nil
}

func (st *store) removeExpiredLiveRuns(runDirs []string) error {
	if len(runDirs) == 0 {
		return nil
	}
	cutoff := trimCutoff(st.maxAge, time.Now())
	removed := 0
	for _, runDir := range runDirs {
		// Re-check under the lock: the run may have been reclaimed since, and a
		// dir is only ever removed while nothing holds its lock.
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

// liveRunExpiredIfUnlocked reports whether a live run is both idle and older
// than the cutoff. The lock is opened without O_CREATE so that merely asking
// cannot resurrect a run dir that is midway through being removed.
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
		// The stale entries are gone by now, so an unfiltered query is both
		// simpler and the honest check.
		if err := st.referencedInShard(shard, 0, referenced); err != nil {
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

// referencedInShard replaces outputIDs with the output IDs that the given
// shard's entries reference. Entries accessed before minAccessedAt are ignored,
// which lets a scan plan around the entries it is about to delete; pass 0 to
// count every entry.
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
	shard := "xx"
	if len(outputHex) >= 2 {
		shard = outputHex[:2]
	}
	return filepath.Join(blobsDir, shard)
}

func (st *store) blobPath(outputHex string) string {
	return blobPath(st.blobsDir, outputHex)
}

func blobPath(blobsDir, outputHex string) string {
	return filepath.Join(blobDir(blobsDir, outputHex), outputHex+".zst")
}
