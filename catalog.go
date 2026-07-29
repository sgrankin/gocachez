package main

import (
	"bytes"
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
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// idKey converts one of the hex IDs that Go passes around into the raw digest the
// catalog stores. Every such string was produced by hex-encoding bytes we already
// held — from the protocol, or read back out of a BLOB column — so a decode
// failure is a broken invariant in this program rather than bad input, and there
// is no value it could return that would not corrupt the catalog.
// outputTagLen is how much of an output digest an entry carries alongside the
// interned id it references. Four bytes cannot identify an output — 268k of them
// collide on 32 bits often enough — and it is not meant to. It is a witness that the
// id still points where it did when the entry was written, which is exactly the thing
// AUTOINCREMENT can only promise and not prove.
const outputTagLen = 4

// outputTag is the witness stored with an entry, and the value a resolved output must
// reproduce. Short IDs are taken whole; cmd/go's are 32 bytes.
func outputTag(outputKey []byte) []byte {
	return outputKey[:min(outputTagLen, len(outputKey))]
}

// errOutputTagMismatch means an entry resolved to an output it did not name: its
// interned id has been handed to different content. Callers treat it as a miss.
var errOutputTagMismatch = errors.New("entry resolved to the wrong output")

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
//
// The join resolves the interned output reference. An entry whose output has been
// evicted matches nothing and reads as a miss, which is the soft reference working.
//
// output_tag comes back so the caller can check the resolved output is the one the
// entry meant; see outputTagLen.
const lookupEntrySQL = `
SELECT o.output_id, o.size, e.created_at, e.output_tag
FROM entries e JOIN outputs o ON o.id = e.output
WHERE e.action_id = ?`

const upsertEntrySQL = `
INSERT INTO entries(action_id, output, output_tag, created_at, accessed_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(action_id) DO UPDATE SET
	output = excluded.output,
	output_tag = excluded.output_tag,
	created_at = excluded.created_at,
	accessed_at = excluded.accessed_at`

// Re-putting an output re-derives its compressed form, so the sizes are refreshed
// rather than left at whatever the first put recorded. The classifications are
// not: they describe the content, which is what the ID stands for.
const upsertOutputSQL = `
INSERT INTO outputs(output_id, size, compressed_size, accessed_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(output_id) DO UPDATE SET
	size = excluded.size,
	compressed_size = excluded.compressed_size,
	accessed_at = excluded.accessed_at
RETURNING id`

const touchEntrySQL = `
UPDATE entries
SET accessed_at = ?
WHERE action_id = ?`

const touchOutputSQL = `
UPDATE outputs
SET accessed_at = ?
WHERE output_id = ?`

type catalog struct {
	db               catalogDB
	lookupStmt       *sql.Stmt
	upsertStmt       *sql.Stmt
	upsertOutputStmt *sql.Stmt
	touchStmt        *sql.Stmt
	touchOutputStmt  *sql.Stmt
}

type catalogRun struct {
	runID string
	// path is relative to the version directory, forward-slashed.
	path string
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
	if c.upsertOutputStmt, err = db.PrepareContext(ctx, upsertOutputSQL); err != nil {
		return fmt.Errorf("prepare output upsert statement: %w", err)
	}
	if c.touchStmt, err = db.PrepareContext(ctx, touchEntrySQL); err != nil {
		return fmt.Errorf("prepare touch statement: %w", err)
	}
	if c.touchOutputStmt, err = db.PrepareContext(ctx, touchOutputSQL); err != nil {
		return fmt.Errorf("prepare output touch statement: %w", err)
	}
	return nil
}

func (c *catalog) close() error {
	var err error
	for _, stmt := range []*sql.Stmt{
		c.lookupStmt, c.upsertStmt, c.upsertOutputStmt, c.touchStmt, c.touchOutputStmt,
	} {
		if stmt != nil {
			err = errors.Join(err, stmt.Close())
		}
	}
	return err
}

func (c *catalog) registerRun(ctx context.Context, runID, path string, createdAt int64) error {
	_, err := c.db.ExecContext(ctx, `
INSERT OR REPLACE INTO runs(run_id, path, created_at)
VALUES (?, ?, ?)`,
		runID, path, createdAt,
	)
	return err
}

func (c *catalog) listOtherRuns(ctx context.Context, runID string) ([]catalogRun, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT run_id, path
FROM runs
WHERE run_id != ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var runs []catalogRun
	for rows.Next() {
		var run catalogRun
		if err := rows.Scan(&run.runID, &run.path); err != nil {
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

// upsertEntry records a put: the action's mapping, and the output it produced.
// Both or neither, so an action can never name an output the inventory does not
// know about — the direction that would make the cache claim bytes it is not
// accounting for.
func (c *catalog) upsertEntry(ctx context.Context, ent entry) error {
	action, output := idKey(ent.ActionID), idKey(ent.OutputID)
	accessedAt := unixMillis(ent.AccessedAt)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin put transaction: %w", err)
	}
	// RETURNING gives the interned id whether the output was new or already known,
	// so the entry can reference it without a second lookup.
	outputRef, err := txQueryInt64(ctx, tx, c.upsertOutputStmt, upsertOutputSQL,
		output, ent.Size, ent.CompressedSize, accessedAt)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("record output: %w", err)
	}
	if err := txExec(ctx, tx, c.upsertStmt, upsertEntrySQL,
		action, outputRef, outputTag(output), unixMillis(ent.CreatedAt), accessedAt); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("record entry: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit put transaction: %w", err)
	}
	return nil
}

// txQueryInt64 is txExec for a statement with a RETURNING clause. It scans here
// rather than handing back a *sql.Row, because the tx-bound statement has to
// outlive the scan and be closed after it.
func txQueryInt64(ctx context.Context, tx *sql.Tx, stmt *sql.Stmt, query string, args ...any) (int64, error) {
	var value int64
	if stmt == nil {
		return value, tx.QueryRowContext(ctx, query, args...).Scan(&value)
	}
	bound := tx.StmtContext(ctx, stmt)
	defer bound.Close() //nolint:errcheck
	return value, bound.QueryRowContext(ctx, args...).Scan(&value)
}

// txExec runs one statement in tx, using the catalog's prepared version when it has
// one. A catalog built for a one-shot read (see newCatalog callers that never call
// prepare) has none, and parses the SQL instead.
func txExec(ctx context.Context, tx *sql.Tx, stmt *sql.Stmt, query string, args ...any) error {
	if stmt == nil {
		_, err := tx.ExecContext(ctx, query, args...)
		return err
	}
	bound := tx.StmtContext(ctx, stmt)
	defer bound.Close() //nolint:errcheck
	_, err := bound.ExecContext(ctx, args...)
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
	var outputID, tag []byte
	var createdAt int64
	if err := row.Scan(&outputID, &ent.Size, &createdAt, &tag); err != nil {
		return entry{}, err
	}
	if !bytes.Equal(tag, outputTag(outputID)) {
		return entry{}, fmt.Errorf("%w: tag %x resolved to output %x", errOutputTagMismatch, tag, outputID)
	}
	ent.OutputID = hex.EncodeToString(outputID)
	ent.CreatedAt = millisTime(createdAt)
	return ent, nil
}

// deleteAction forgets one action and nothing else. The tag-mismatch path needs this
// rather than dropAction: the output it wrongly resolved to is somebody else's, and
// perfectly good.
func (c *catalog) deleteAction(ctx context.Context, actionID string) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM entries WHERE action_id = ?`, idKey(actionID))
	return err
}

// touchAccessed updates the access times a run has accumulated, in one
// transaction. Entry times drive the age reaper and output times drive eviction,
// so both have to move or one of the two stops seeing the cache being used.
func (c *catalog) touchAccessed(ctx context.Context, tx *sql.Tx, entries, outputs map[string]int64) error {
	if err := touchAll(ctx, tx, c.touchStmt, touchEntrySQL, entries); err != nil {
		return fmt.Errorf("touch entry: %w", err)
	}
	if err := touchAll(ctx, tx, c.touchOutputStmt, touchOutputSQL, outputs); err != nil {
		return fmt.Errorf("touch output: %w", err)
	}
	return nil
}

// touchAll binds the statement once for the whole batch rather than per row,
// which is the point of doing these in one transaction.
func touchAll(ctx context.Context, tx *sql.Tx, stmt *sql.Stmt, query string, accessed map[string]int64) error {
	if len(accessed) == 0 {
		return nil
	}
	if stmt == nil {
		for id, accessedAt := range accessed {
			if _, err := tx.ExecContext(ctx, query, accessedAt, idKey(id)); err != nil {
				return err
			}
		}
		return nil
	}
	bound := tx.StmtContext(ctx, stmt)
	defer bound.Close() //nolint:errcheck
	for id, accessedAt := range accessed {
		if _, err := bound.ExecContext(ctx, accessedAt, idKey(id)); err != nil {
			return err
		}
	}
	return nil
}

// dropAction forgets one action and the output it named, for a blob found corrupt
// or missing.
//
// Only this action is deleted, not every action sharing the output. There is no
// index from output back to actions — that is the point of the soft reference —
// so "every action for this output" would mean scanning the whole table on a
// cache miss. Dropping the output row is what matters: the blob stops being
// referenced, and any sibling action becomes a dangling entry that costs one
// primary-key delete when it is next asked for.
func (c *catalog) dropAction(ctx context.Context, actionID, outputID string) error {
	if _, err := c.db.ExecContext(ctx, `DELETE FROM outputs WHERE output_id = ?`, idKey(outputID)); err != nil {
		return err
	}
	_, err := c.db.ExecContext(ctx, `DELETE FROM entries WHERE action_id = ?`, idKey(actionID))
	return err
}

// evictOutput removes an output from the inventory, but only if nothing has read
// it since it was chosen. Candidate selection and deletion are separated by the
// whole planning walk, and a build that ran in between flushes its reads on the
// way out, so without the re-assert an output picked as the coldest in the cache
// is deleted after becoming the hottest.
//
// The caller may unlink the blob only when this reports true.
func (c *catalog) evictOutput(ctx context.Context, outputID string, accessedAt int64) (bool, error) {
	res, err := c.db.ExecContext(ctx, `
DELETE FROM outputs
WHERE output_id = ? AND accessed_at <= ?`, idKey(outputID), accessedAt)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

// ageBatch bounds how many rows one age-expiry transaction removes.
//
// SQLite has a single writer and gives a competing one a five-second busy timeout,
// after which its statement fails rather than waiting — and a put that fails reaches
// the go command as an error, not as a cache miss. Age expiry stopped waiting for an
// idle cache, so it now runs against live builds, and one DELETE across a
// multi-million-row catalog is a build failure waiting for a cache big enough.
// classificationBatch exists for this reason, measured on the same host: a single
// 200k-row transaction stalled a concurrent put for 3.9s of its 5s.
const ageBatch = 2000

// deleteAccessedBefore expires both tables. outputs is the one that frees disk:
// its rows are what keeps a blob from being an orphan, so an implementation that
// reaped only entries would leave maxAge quietly reclaiming nothing.
func (c *catalog) deleteAccessedBefore(ctx context.Context, cutoff int64) (reaped, error) {
	entries, err := c.deleteStaleEntries(ctx, cutoff)
	if err != nil {
		return reaped{}, err
	}
	outputs, err := c.deleteStaleOutputs(ctx, cutoff)
	if err != nil {
		return reaped{entries: entries}, err
	}
	return reaped{entries: entries, outputs: outputs}, nil
}

// deleteStaleEntries walks the table in primary-key order, carrying a cursor so the
// scan resumes where the last batch stopped. entries has no index on accessed_at —
// see catalogSchema for why — so every batch is a scan.
//
// The cursor bounds cost, not correctness: without it each batch restarts at the
// beginning and still finds the rows it should, because the ones it already removed
// are gone. What it costs is the tail, where few stale rows are left and each batch
// scans most of the surviving table to fill itself. Over 7.7M entries with an eighth
// of them stale that is roughly six passes rather than one. No test asserting what
// survives can tell the two apart, so this comment is the only thing recording why the
// cursor is here.
func (c *catalog) deleteStaleEntries(ctx context.Context, cutoff int64) (int64, error) {
	var total int64
	var cursor []byte
	for {
		ids, err := c.staleActionIDs(ctx, cutoff, cursor)
		if err != nil {
			return total, err
		}
		if len(ids) == 0 {
			return total, nil
		}
		removed, err := deleteStaleByKey(ctx, c.db, "entries", "action_id", cutoff, ids)
		total += removed
		if err != nil {
			return total, err
		}
		cursor = ids[len(ids)-1]
	}
}

func (c *catalog) staleActionIDs(ctx context.Context, cutoff int64, after []byte) ([][]byte, error) {
	if after == nil {
		// No BLOB sorts below the empty one, so this matches every key.
		after = []byte{}
	}
	rows, err := c.db.QueryContext(ctx, `
SELECT action_id FROM entries
WHERE accessed_at < ? AND action_id > ?
ORDER BY action_id
LIMIT ?`, cutoff, after, ageBatch)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	return scanKeys(rows)
}

// deleteStaleOutputs needs no cursor: outputs_accessed_at makes finding the next batch
// a seek rather than a scan, so removing one batch leaves the next at hand.
func (c *catalog) deleteStaleOutputs(ctx context.Context, cutoff int64) (int64, error) {
	var total int64
	for {
		ids, err := c.staleOutputIDs(ctx, cutoff)
		if err != nil {
			return total, err
		}
		if len(ids) == 0 {
			return total, nil
		}
		removed, err := deleteStaleByKey(ctx, c.db, "outputs", "output_id", cutoff, ids)
		total += removed
		if err != nil {
			return total, err
		}
	}
}

func (c *catalog) staleOutputIDs(ctx context.Context, cutoff int64) ([][]byte, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT output_id FROM outputs WHERE accessed_at < ? ORDER BY accessed_at LIMIT ?`, cutoff, ageBatch)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	return scanKeys(rows)
}

func scanKeys(rows *sql.Rows) ([][]byte, error) {
	var keys [][]byte
	for rows.Next() {
		var key []byte
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// deleteStaleByKey removes one batch in one transaction, by primary key, through a
// single prepared statement — the same shape as commitClassifications and for the same
// reason: the point is to bound how long the write lock is held, not to issue the
// fewest statements.
//
// It builds the statement rather than taking one, so the cutoff cannot be left out. The
// keys were chosen by an earlier statement and expiry runs against live builds, so a row
// can be read and its access flushed in between; deleting it by key alone would take an
// entry that is in use, and for an output would send the blob after it on the next
// orphan pass. Re-asserting is also what keeps the cursorless outputs loop terminating —
// a row that survives because it was refreshed is no longer stale, so the next batch
// does not return it. Table and column are internal constants, never input.
func deleteStaleByKey(ctx context.Context, db catalogDB, table, keyColumn string, cutoff int64, keys [][]byte) (int64, error) {
	query := fmt.Sprintf(`DELETE FROM %s WHERE %s = ? AND accessed_at < ?`, table, keyColumn)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin age-expiry transaction: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("prepare age-expiry delete: %w", err)
	}
	defer stmt.Close() //nolint:errcheck
	var removed int64
	for _, key := range keys {
		res, err := stmt.ExecContext(ctx, key, cutoff)
		if err != nil {
			_ = tx.Rollback()
			return 0, fmt.Errorf("age-expiry delete: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		removed += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit age-expiry transaction: %w", err)
	}
	return removed, nil
}

// reaped counts what an age pass removed from each table.
type reaped struct {
	entries int64
	outputs int64
}

func (c *catalog) compressedSize(ctx context.Context) (int64, error) {
	var size int64
	err := c.db.QueryRowContext(ctx,
		`SELECT CAST(COALESCE(SUM(compressed_size), 0) AS INTEGER) FROM outputs`).Scan(&size)
	return size, err
}

// outputStats totals what status reports. One row per blob is exactly what the
// inventory holds, so this is a table scan of a few hundred thousand rows rather
// than the GROUP BY over millions of entries it replaced.
func (c *catalog) outputStats(ctx context.Context) (int64, int64, int64, error) {
	var count, size, compressedSize int64
	err := c.db.QueryRowContext(ctx, `
SELECT COUNT(*),
       CAST(COALESCE(SUM(size), 0) AS INTEGER),
       CAST(COALESCE(SUM(compressed_size), 0) AS INTEGER)
FROM outputs`).Scan(&count, &size, &compressedSize)
	return count, size, compressedSize, err
}

// listOutputs returns one row per output with its uncompressed and compressed
// size and its cached classifications. A classification stamped by an older
// classifier is reported as absent, so status recomputes it rather than
// reporting a stale answer.
func (c *catalog) listOutputs(ctx context.Context) ([]catalogOutput, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT output_id, size, compressed_size,
       CASE WHEN blob_type_version = ? THEN blob_type END,
       CASE WHEN retained_type_version = ? THEN retained_type END
FROM outputs`, blobClassifierVersion, retainedClassifierVersion)
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
SELECT output_id
FROM outputs
WHERE output_id >= ? AND output_id < ? AND accessed_at >= ?`

// The last shard has no successor byte to stop before. BLOBs compare by content
// with the shorter one first, so every key beginning 0xff sorts at or above the
// single byte 0xff, and no other key does.
const referencedOutputsTailSQL = `
SELECT output_id
FROM outputs
WHERE output_id >= ? AND accessed_at >= ?`

// referencedOutputIDs replaces outputIDs with the inventoried outputs whose ID
// starts with the shard byte. Outputs accessed before minAccessedAt are ignored,
// which lets a scan plan around the rows it is about to reap; pass keepEveryOutput
// to count all of them.
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
UPDATE outputs
SET blob_type = ?, blob_type_version = ?
WHERE output_id = ?`

const updateRetainedTypeSQL = `
UPDATE outputs
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

// evictionCandidates returns the coldest inventoried outputs after the given cursor,
// least recently used first, at most limit of them.
//
// This is a range scan of outputs_accessed_at. It replaced a loop that walked
// entries in bounded steps, taking a GROUP BY and a sort per step, because there
// is no longer a set of aliases to collapse before the oldest output can be named.
//
// The cursor is the whole of that index — (accessed_at, compressed_size) plus the rowid
// SQLite appends to every index — because paging on accessed_at alone cannot resume
// inside a group that shares one. A page that ended mid-group had to either re-read it
// forever or step past it, and stepping past silently dropped every candidate it had not
// returned. That is not rare: the access flush stamps a whole batch with one millisecond,
// so thousands of outputs can share a timestamp, and skipping them leaves the cache over
// budget with the pass reporting itself done. Ordering by the full index keeps the seek
// and the limit index-driven, with no sort — see TestEvictionPagesThroughTiedAccessTimes.
func (c *catalog) evictionCandidates(ctx context.Context, after evictionCursor, limit int) ([]pruneCandidate, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT output_id, compressed_size, accessed_at, id
FROM outputs
WHERE (accessed_at, compressed_size, id) > (?, ?, ?)
ORDER BY accessed_at, compressed_size, id
LIMIT ?`, after.accessedAt, after.size, after.id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var candidates []pruneCandidate
	for rows.Next() {
		var candidate pruneCandidate
		var outputID []byte
		if err := rows.Scan(&outputID, &candidate.size, &candidate.accessedAt, &candidate.id); err != nil {
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
