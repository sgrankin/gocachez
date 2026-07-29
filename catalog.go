package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
)

type catalogDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// idKey converts one of the hex IDs that Go passes around into the raw digest the
// catalog stores. Every such string was produced by hex-encoding bytes we already
// held — from the protocol, or read back out of a BLOB column — so a decode
// failure is a broken invariant in this program rather than bad input, and there
// is no value it could return that would not corrupt the catalog.
func idKey(hexID string) []byte {
	key, err := hex.DecodeString(hexID)
	if err != nil {
		panic(fmt.Sprintf("catalog: %q is not a hex ID: %v", hexID, err))
	}
	return key
}

// lookupEntrySQL and upsertEntrySQL are the per-request hot-path queries. They
// are prepared once on the store's connection (see catalog.prepare) so modernc
// does not re-parse them on every get/put.
//
// action_id is deliberately absent from the select list: the caller passed it in,
// so reading it back is a column of copying per cache hit to learn nothing.
const lookupEntrySQL = `
SELECT output_id, size, compressed_size, created_at, accessed_at
FROM entries
WHERE action_id = ?`

const upsertEntrySQL = `
INSERT INTO entries(action_id, output_id, size, compressed_size, created_at, accessed_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(action_id) DO UPDATE SET
	output_id = excluded.output_id,
	size = excluded.size,
	compressed_size = excluded.compressed_size,
	created_at = excluded.created_at,
	accessed_at = excluded.accessed_at,
	blob_type = CASE WHEN entries.output_id = excluded.output_id THEN entries.blob_type END,
	blob_type_version = CASE WHEN entries.output_id = excluded.output_id THEN entries.blob_type_version END,
	retained_type = CASE WHEN entries.output_id = excluded.output_id THEN entries.retained_type END,
	retained_type_version = CASE WHEN entries.output_id = excluded.output_id THEN entries.retained_type_version END`

const touchEntrySQL = `
UPDATE entries
SET accessed_at = ?
WHERE action_id = ?`

type catalog struct {
	db         catalogDB
	lookupStmt *sql.Stmt
	upsertStmt *sql.Stmt
	touchStmt  *sql.Stmt
}

type catalogRun struct {
	runID    string
	path     string
	lockPath string
}

type catalogOutput struct {
	outputID       string
	size           int64
	compressedSize int64
	blobType       sql.NullInt64
	retainedType   sql.NullInt64
}

func newCatalog(db catalogDB) *catalog {
	return &catalog{db: db}
}

// prepare caches the hot-path statements on a persistent *sql.DB connection.
// It is a no-op for any other catalogDB, which falls back to parsing per call.
func (c *catalog) prepare(ctx context.Context) error {
	db, ok := c.db.(*sql.DB)
	if !ok {
		return nil
	}
	var err error
	if c.lookupStmt, err = db.PrepareContext(ctx, lookupEntrySQL); err != nil {
		return fmt.Errorf("prepare lookup statement: %w", err)
	}
	if c.upsertStmt, err = db.PrepareContext(ctx, upsertEntrySQL); err != nil {
		return fmt.Errorf("prepare upsert statement: %w", err)
	}
	if c.touchStmt, err = db.PrepareContext(ctx, touchEntrySQL); err != nil {
		return fmt.Errorf("prepare touch statement: %w", err)
	}
	return nil
}

func (c *catalog) close() error {
	var err error
	for _, stmt := range []*sql.Stmt{c.lookupStmt, c.upsertStmt, c.touchStmt} {
		if stmt != nil {
			err = errors.Join(err, stmt.Close())
		}
	}
	return err
}

func (c *catalog) registerRun(ctx context.Context, runID, path, lockPath string, createdAt int64) error {
	_, err := c.db.ExecContext(ctx, `
INSERT OR REPLACE INTO runs(run_id, path, lock_path, created_at)
VALUES (?, ?, ?, ?)`,
		runID, path, lockPath, createdAt,
	)
	return err
}

func (c *catalog) listOtherRuns(ctx context.Context, runID string) ([]catalogRun, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT run_id, path, lock_path
FROM runs
WHERE run_id != ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var runs []catalogRun
	for rows.Next() {
		var run catalogRun
		if err := rows.Scan(&run.runID, &run.path, &run.lockPath); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return runs, nil
}

func (c *catalog) countRuns(ctx context.Context) (int64, error) {
	var count int64
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&count)
	return count, err
}

func (c *catalog) deleteRun(ctx context.Context, runID string) error {
	_, err := c.db.ExecContext(ctx, `
DELETE FROM runs
WHERE run_id = ?`, runID)
	return err
}

func (c *catalog) upsertEntry(ctx context.Context, ent entry) error {
	args := []any{
		idKey(ent.ActionID),
		idKey(ent.OutputID),
		ent.Size,
		ent.CompressedSize,
		unixMillis(ent.CreatedAt),
		unixMillis(ent.AccessedAt),
	}
	var err error
	if c.upsertStmt != nil {
		_, err = c.upsertStmt.ExecContext(ctx, args...)
	} else {
		_, err = c.db.ExecContext(ctx, upsertEntrySQL, args...)
	}
	return err
}

func (c *catalog) lookupEntry(ctx context.Context, actionID string) (entry, error) {
	key := idKey(actionID)
	var row *sql.Row
	if c.lookupStmt != nil {
		row = c.lookupStmt.QueryRowContext(ctx, key)
	} else {
		row = c.db.QueryRowContext(ctx, lookupEntrySQL, key)
	}
	ent := entry{ActionID: actionID}
	var outputID []byte
	var createdAt, accessedAt int64
	err := row.Scan(
		&outputID,
		&ent.Size,
		&ent.CompressedSize,
		&createdAt,
		&accessedAt,
	)
	if err != nil {
		return entry{}, err
	}
	ent.OutputID = hex.EncodeToString(outputID)
	ent.CreatedAt = millisTime(createdAt)
	ent.AccessedAt = millisTime(accessedAt)
	return ent, nil
}

// touchEntries updates the access time of many entries in a single transaction,
// reusing the prepared statement (bound to tx) so the update is parsed once.
func (c *catalog) touchEntries(ctx context.Context, tx *sql.Tx, accessed map[string]int64) error {
	if c.touchStmt == nil {
		for actionID, accessedAt := range accessed {
			if _, err := tx.ExecContext(ctx, touchEntrySQL, accessedAt, idKey(actionID)); err != nil {
				return fmt.Errorf("touch entry: %w", err)
			}
		}
		return nil
	}
	stmt := tx.StmtContext(ctx, c.touchStmt)
	defer stmt.Close() //nolint:errcheck
	for actionID, accessedAt := range accessed {
		if _, err := stmt.ExecContext(ctx, accessedAt, idKey(actionID)); err != nil {
			return fmt.Errorf("touch entry: %w", err)
		}
	}
	return nil
}

// deleteEntriesByOutputID removes every entry for an output unconditionally. That
// is right for a blob found corrupt or missing, where the entries have to go
// whatever their age; eviction wants evictEntriesByOutputID instead.
func (c *catalog) deleteEntriesByOutputID(ctx context.Context, outputID string) error {
	_, err := c.db.ExecContext(ctx, `
DELETE FROM entries
WHERE output_id = ?`, idKey(outputID))
	return err
}

// evictEntriesByOutputID removes an output's entries only if nothing has read it
// since it was chosen. Candidate selection and deletion are separated by the whole
// planning walk, and a build that ran in between flushes its reads on the way out,
// so without this an output picked as the coldest in the cache is deleted even
// after becoming the hottest.
//
// It has to be all or nothing. Deleting the actions that are still cold while
// leaving a warm one behind would report rows affected, the caller would unlink the
// blob, and the surviving rows would point at nothing.
func (c *catalog) evictEntriesByOutputID(ctx context.Context, outputID string, accessedAt int64) (int64, error) {
	res, err := c.db.ExecContext(ctx, `
DELETE FROM entries
WHERE output_id = ?
  AND NOT EXISTS (
    SELECT 1 FROM entries newer
    WHERE newer.output_id = ? AND newer.accessed_at > ?
  )`, idKey(outputID), idKey(outputID), accessedAt)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (c *catalog) deleteEntriesAccessedBefore(ctx context.Context, cutoff int64) (int64, error) {
	res, err := c.db.ExecContext(ctx, `
DELETE FROM entries
WHERE accessed_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// oldestAccess returns the least recent access time in the catalog, or ok=false
// when there are no entries. entries_accessed_at makes this a seek, so callers
// can decide whether an age-based delete has anything to match before doing the
// scan it would cost.
func (c *catalog) oldestAccess(ctx context.Context) (int64, bool, error) {
	var oldest sql.NullInt64
	if err := c.db.QueryRowContext(ctx, `SELECT MIN(accessed_at) FROM entries`).Scan(&oldest); err != nil {
		return 0, false, err
	}
	return oldest.Int64, oldest.Valid, nil
}

func (c *catalog) compressedSize(ctx context.Context) (int64, error) {
	var size int64
	err := c.db.QueryRowContext(ctx, `
SELECT CAST(COALESCE(SUM(compressed_size), 0) AS INTEGER)
FROM (
	SELECT output_id, MAX(compressed_size) AS compressed_size
	FROM entries
	GROUP BY output_id
)`).Scan(&size)
	return size, err
}

// outputStats totals what status reports without materialising a row per output.
// It mirrors listOutputs' grouping — one row per output_id, taking MAX of each
// size — because several actions can map to one output and its bytes are on disk
// once. A cache with 267,900 outputs was loading all of them to add up two
// columns.
func (c *catalog) outputStats(ctx context.Context) (int64, int64, int64, error) {
	var count, size, compressedSize int64
	err := c.db.QueryRowContext(ctx, `
SELECT COUNT(*),
       CAST(COALESCE(SUM(size), 0) AS INTEGER),
       CAST(COALESCE(SUM(compressed_size), 0) AS INTEGER)
FROM (
	SELECT MAX(size) AS size, MAX(compressed_size) AS compressed_size
	FROM entries GROUP BY output_id
)`).Scan(&count, &size, &compressedSize)
	return count, size, compressedSize, err
}

// listOutputs returns one row per output with its uncompressed and compressed
// size and its cached classifications. A classification stamped by an older
// classifier is reported as absent, so status recomputes it rather than
// reporting a stale answer.
func (c *catalog) listOutputs(ctx context.Context) ([]catalogOutput, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT output_id,
       CAST(MAX(size) AS INTEGER),
       CAST(MAX(compressed_size) AS INTEGER),
       MAX(CASE WHEN blob_type_version = ? THEN blob_type END),
       MAX(CASE WHEN retained_type_version = ? THEN retained_type END)
FROM entries
GROUP BY output_id`, blobClassifierVersion, retainedClassifierVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var outputs []catalogOutput
	for rows.Next() {
		var output catalogOutput
		var outputID []byte
		if err := rows.Scan(&outputID, &output.size, &output.compressedSize,
			&output.blobType, &output.retainedType); err != nil {
			return nil, err
		}
		output.outputID = hex.EncodeToString(outputID)
		outputs = append(outputs, output)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return outputs, nil
}

const referencedOutputsSQL = `
SELECT DISTINCT output_id
FROM entries
WHERE output_id >= ? AND output_id < ? AND accessed_at >= ?`

// The last shard has no successor byte to stop before. BLOBs compare by content
// with the shorter one first, so every key beginning 0xff sorts at or above the
// single byte 0xff, and no other key does.
const referencedOutputsTailSQL = `
SELECT DISTINCT output_id
FROM entries
WHERE output_id >= ? AND accessed_at >= ?`

// referencedOutputIDs replaces outputIDs with the outputs referenced by entries
// whose ID starts with the shard byte. Entries accessed before minAccessedAt are
// ignored, which lets a scan plan around the entries it is about to delete; pass
// keepEveryEntry to count all of them.
func (c *catalog) referencedOutputIDs(ctx context.Context, shard int, minAccessedAt int64, outputIDs map[string]struct{}) error {
	query, args := referencedOutputsSQL, []any{
		[]byte{byte(shard)}, []byte{byte(shard + 1)}, minAccessedAt,
	}
	if shard == lastOutputShard {
		query, args = referencedOutputsTailSQL, []any{[]byte{byte(shard)}, minAccessedAt}
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close() //nolint:errcheck

	clear(outputIDs)
	for rows.Next() {
		var outputID []byte
		if err := rows.Scan(&outputID); err != nil {
			return err
		}
		outputIDs[hex.EncodeToString(outputID)] = struct{}{}
	}
	return rows.Err()
}

const updateBlobTypeSQL = `
UPDATE entries
SET blob_type = ?, blob_type_version = ?
WHERE output_id = ?`

const updateRetainedTypeSQL = `
UPDATE entries
SET retained_type = ?, retained_type_version = ?
WHERE output_id = ?`

func (c *catalog) updateBlobType(ctx context.Context, outputID string, kind blobTypeKind, classifierVersion int64) error {
	_, err := c.db.ExecContext(ctx, updateBlobTypeSQL, int64(kind), classifierVersion, idKey(outputID))
	return err
}

func (c *catalog) updateRetainedType(ctx context.Context, outputID string, kind retainedTypeKind) error {
	_, err := c.db.ExecContext(ctx, updateRetainedTypeSQL, int64(kind), retainedClassifierVersion, idKey(outputID))
	return err
}

// classifier pairs an update statement with the classifier version whose
// output it stores. Binding them in one value keeps a caller from writing blob
// kinds into retained_type, or stamping either column with the other's
// version; the type parameter ties the pair to the kind it accepts.
type classifier[K ~int] struct {
	// updateSQL must bind (kind, version, outputID) in that order.
	updateSQL string
	version   int64
}

var (
	blobClassifier     = classifier[blobTypeKind]{updateBlobTypeSQL, blobClassifierVersion}
	retainedClassifier = classifier[retainedTypeKind]{updateRetainedTypeSQL, retainedClassifierVersion}
)

// classificationBatch bounds how many rows one transaction updates. Status can
// classify the entire cache in a single pass, and SQLite has one write slot:
// committing that as a single transaction held it long enough to stall a
// concurrent put for 3.9s against its 5s busy timeout on a 200k-output cache,
// and past that threshold the put fails outright.
const classificationBatch = 1000

// persistClassifications caches classifications so later status runs can skip
// reclassifying. Batches commit independently, so a failure part way through
// still leaves the earlier ones cached and shrinks the next run's work.
//
// Failures are logged rather than returned: the report the caller is building
// is correct either way, only the next run's cost is affected. Logging is
// gated like the store's other maintenance chatter, but it must exist — a
// persist that keeps losing the write race to concurrent builds leaves status
// reclassifying the whole cache every time, and discarding the error made that
// undiagnosable.
func persistClassifications[K ~int](dbPath string, c classifier[K], classified map[string]K, verbose bool) {
	if err := writeClassifications(dbPath, c, classified); err != nil && verbose {
		log.Printf("gocachez: caching classifications failed, the next status will redo this work: %v", err)
	}
}

func writeClassifications[K ~int](dbPath string, c classifier[K], classified map[string]K) error {
	if len(classified) == 0 {
		return nil
	}
	db, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck

	ctx := context.Background()
	stmt, err := db.PrepareContext(ctx, c.updateSQL)
	if err != nil {
		return fmt.Errorf("prepare classification update: %w", err)
	}
	defer stmt.Close() //nolint:errcheck

	batch := make(map[string]K, classificationBatch)
	for outputID, kind := range classified {
		batch[outputID] = kind
		if len(batch) < classificationBatch {
			continue
		}
		if err := commitClassifications(ctx, db, stmt, c.version, batch); err != nil {
			return err
		}
		clear(batch)
	}
	return commitClassifications(ctx, db, stmt, c.version, batch)
}

func commitClassifications[K ~int](ctx context.Context, db *sql.DB, stmt *sql.Stmt, version int64, batch map[string]K) error {
	if len(batch) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin classification transaction: %w", err)
	}
	txStmt := tx.StmtContext(ctx, stmt)
	defer txStmt.Close() //nolint:errcheck
	for outputID, kind := range batch {
		if _, err := txStmt.ExecContext(ctx, int64(kind), version, idKey(outputID)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("cache classification: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit classifications: %w", err)
	}
	return nil
}

// oldestAccessWatermark reports the access time of the limit'th entry at or
// after from, which is how far a bounded eviction step advances. ok is false
// when fewer than one entry remains.
//
// entries_accessed_at makes this a range scan with no sort.
func (c *catalog) oldestAccessWatermark(ctx context.Context, from int64, limit int) (int64, bool, error) {
	var watermark sql.NullInt64
	err := c.db.QueryRowContext(ctx, `
SELECT MAX(accessed_at) FROM (
	SELECT accessed_at
	FROM entries
	WHERE accessed_at >= ?
	ORDER BY accessed_at
	LIMIT ?
)`, from, limit).Scan(&watermark)
	if err != nil {
		return 0, false, err
	}
	return watermark.Int64, watermark.Valid, nil
}

// evictionCandidates returns the outputs whose *newest* access falls in
// [from, watermark], least recently used first — the outputs entirely contained
// in the region an eviction step has walked. An output with a fresher entry
// elsewhere is deliberately absent: it is not actually old, and a later step
// reaches it when the walk gets to that entry.
func (c *catalog) evictionCandidates(ctx context.Context, from, watermark int64) ([]pruneCandidate, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT output_id, CAST(MAX(compressed_size) AS INTEGER), MAX(accessed_at)
FROM entries
WHERE output_id IN (
	SELECT output_id FROM entries WHERE accessed_at >= ? AND accessed_at <= ?
)
GROUP BY output_id
HAVING MAX(accessed_at) <= ?
ORDER BY MAX(accessed_at)`, from, watermark, watermark)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var candidates []pruneCandidate
	for rows.Next() {
		var candidate pruneCandidate
		var outputID []byte
		if err := rows.Scan(&outputID, &candidate.size, &candidate.accessedAt); err != nil {
			return nil, err
		}
		candidate.outputID = hex.EncodeToString(outputID)
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}
