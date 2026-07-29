package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"golang.org/x/tools/go/gcexportdata"
)

func TestStorePutGet(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	body := bytes.NewBufferString("hello from the cache")
	actionID := bytes.Repeat([]byte{1}, 32)
	outputID := bytes.Repeat([]byte{2}, 32)
	req := request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(body.Len()),
	}
	res, err := st.put(req, bufio.NewReader(encodedBody(body.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(res.DiskPath) {
		t.Fatalf("put DiskPath is not absolute: %q", res.DiskPath)
	}
	if rel, err := filepath.Rel(st.liveRoot, res.DiskPath); err != nil || rel == "." || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("put DiskPath = %q, not under live root %q", res.DiskPath, st.liveRoot)
	}
	gotBody, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBody) != "hello from the cache" {
		t.Fatalf("put body = %q", gotBody)
	}

	getRes, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if getRes.Miss {
		t.Fatal("get missed")
	}
	if !bytes.Equal(getRes.OutputID, outputID) {
		t.Fatalf("OutputID = %x, want %x", getRes.OutputID, outputID)
	}
	if getRes.Size != int64(len(gotBody)) {
		t.Fatalf("Size = %d, want %d", getRes.Size, len(gotBody))
	}
	gotBody, err = os.ReadFile(getRes.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBody) != "hello from the cache" {
		t.Fatalf("get body = %q", gotBody)
	}
}

func TestGetMiss(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{33}, 32)
	res, err := st.get(request{ID: 1, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if res.ID != 1 || !res.Miss {
		t.Fatalf("get response = %+v, want miss", res)
	}
}

func TestGetMaterializesAfterLiveFileRemoved(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{34}, 32)
	body := []byte("materialize this")
	outputID := sha256Sum(body)
	putRes, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body)))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(putRes.DiskPath); err != nil {
		t.Fatal(err)
	}

	getRes, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if getRes.Miss || getRes.DiskPath == putRes.DiskPath {
		t.Fatalf("get response = %+v, put path %q", getRes, putRes.DiskPath)
	}
	got, err := os.ReadFile(getRes.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("materialized body = %q, want %q", got, body)
	}
}

func TestGetDropsEntryWhoseBlobIsMissing(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{36}, 32)
	outputID := hexOf(bytes.Repeat([]byte{37}, 32))
	if err := st.upsertEntry(entry{
		ActionID:       hexOf(actionID),
		OutputID:       outputID,
		Size:           4,
		CompressedSize: 4,
		CreatedAt:      time.Now(),
		AccessedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	// No blob was ever written, so materializing has to fail with ErrNotExist.
	res, err := st.get(request{ID: 1, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Miss {
		t.Fatalf("get response = %+v, want miss", res)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); !errorsIs(err, sql.ErrNoRows) {
		t.Fatalf("entry = %v, want sql.ErrNoRows", err)
	}
}

func TestInvalidMaterializedBlobIsCacheMiss(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{38}, 32)
	body := []byte("body")
	outputHex := hexOf(sha256Sum(body))
	if err := os.MkdirAll(st.blobDir(outputHex), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := writeCompressedFile(st, st.blobPath(outputHex), body); err != nil {
		t.Fatal(err)
	}
	if err := st.upsertEntry(entry{
		ActionID:       hexOf(actionID),
		OutputID:       outputHex,
		Size:           int64(len(body)) + 1,
		CompressedSize: int64(len(body)),
		CreatedAt:      time.Now(),
		AccessedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	res, err := st.get(request{ID: 1, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Miss {
		t.Fatalf("get response = %+v, want miss", res)
	}
}

func TestOutputIDIsOpaque(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{39}, 32)
	outputID := bytes.Repeat([]byte{40}, 32)
	body := []byte("body")
	outputHex := hexOf(outputID)
	if err := os.MkdirAll(st.blobDir(outputHex), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := writeCompressedFile(st, st.blobPath(outputHex), body); err != nil {
		t.Fatal(err)
	}
	if err := st.upsertEntry(entry{
		ActionID:       hexOf(actionID),
		OutputID:       outputHex,
		Size:           int64(len(body)),
		CompressedSize: int64(len(body)),
		CreatedAt:      time.Now(),
		AccessedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	res, err := st.get(request{ID: 1, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if res.Miss || !bytes.Equal(res.OutputID, outputID) {
		t.Fatalf("get response = %+v, want hit with opaque OutputID %x", res, outputID)
	}
	got, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestPutAcceptsZeroSizeBody(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{35}, 32)
	outputID := sha256Sum(nil)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 0,
	}, bufio.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if res.DiskPath == "" {
		t.Fatalf("put response = %+v", res)
	}
	getRes, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if getRes.Miss || getRes.Size != 0 {
		t.Fatalf("get response = %+v", getRes)
	}
}

func TestCacheRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := st.put(request{ID: 1, Command: cmdPut}, bufio.NewReader(nil)); err == nil {
		t.Fatal("put accepted missing ActionID")
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{1}, 32),
	}, bufio.NewReader(nil)); err == nil {
		t.Fatal("put accepted missing OutputID")
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{1}, 32),
		OutputID: bytes.Repeat([]byte{2}, 32),
		BodySize: -1,
	}, bufio.NewReader(nil)); err == nil {
		t.Fatal("put accepted negative body size")
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{1}, 32),
		OutputID: bytes.Repeat([]byte{2}, 32),
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("x")))); err == nil {
		t.Fatal("put accepted mismatched body size")
	}
	if _, err := st.get(request{ID: 2, Command: cmdGet}); err == nil {
		t.Fatal("get accepted missing ActionID")
	}
}

// runDirOf recovers a live file's run directory. Live files sit one shard deep
// inside it, so this is two levels up rather than one (see createLiveFile).
func runDirOf(livePath string) string {
	return filepath.Dir(filepath.Dir(livePath))
}

// The type breakdown reads every blob it has no cached classification for. On a
// cache of 174 GiB across 267,900 outputs that is the report's entire cost, and it
// is a diagnostic rather than something you need to see the cache's size — so the
// default must not touch a blob. Deleting one is the cheapest way to prove it: any
// pass that opens blobs fails, and one that only reads the catalog does not.
func TestStatusDoesNotReadBlobsWithoutTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	outputID := bytes.Repeat([]byte{93}, 32)
	if _, err := st.put(request{
		ID: 1, Command: cmdPut,
		ActionID: bytes.Repeat([]byte{94}, 32),
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	st.close()
	if err := os.Remove(blobPath); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	// The catalog still counts it, so the report is complete without the blob.
	if !strings.Contains(stdout.String(), "Cached outputs") {
		t.Fatalf("status did not report the catalog: %q", stdout.String())
	}
	if strings.Contains(stdout.String(), "Unreadable blobs") {
		t.Error("status classified blobs without -types")
	}

	// With -types it does open them, and says so about the one that is gone.
	stdout.Reset()
	if err := run([]string{"status", "-types", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Unreadable blobs") {
		t.Errorf("-types did not classify blobs: %q", stdout.String())
	}
}

// The tuning is in a DSN string, where a typo is silent: the driver ignores a
// pragma it cannot parse and every lookup quietly goes back to pread. So ask the
// database what it ended up with.
func TestCatalogIsOpenedWithTheTuningApplied(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cache.db")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck

	pragma := func(d *sql.DB, name string) int64 {
		t.Helper()
		var v int64
		if err := d.QueryRowContext(context.Background(), "PRAGMA "+name).Scan(&v); err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return v
	}
	if got := pragma(db, "mmap_size"); got <= 0 {
		t.Errorf("mmap_size = %d, want the mapping enabled", got)
	}
	if got := pragma(db, "journal_size_limit"); got <= 0 {
		t.Errorf("journal_size_limit = %d, want the log bounded", got)
	}
	// Raising cache_size measured slower than the default, so it must stay put.
	if got := pragma(db, "cache_size"); got != -2000 {
		t.Errorf("cache_size = %d, want the default -2000", got)
	}

	// status opens read-only and scans, so it wants the mapping too.
	ro, err := openExistingDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close() //nolint:errcheck
	if got := pragma(ro, "mmap_size"); got <= 0 {
		t.Errorf("read-only mmap_size = %d, want the mapping enabled", got)
	}
}

// Without -types the type sections must be absent, not empty. A table reading
// "None 0 0B" says the cache holds nothing of any type, which is a different claim
// from not having looked — and on a cache of a quarter million blobs it is a
// wrong one.
func TestStatusOmitsTypeSectionsWithoutTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.put(request{
		ID: 1, Command: cmdPut,
		ActionID: bytes.Repeat([]byte{95}, 32),
		OutputID: bytes.Repeat([]byte{96}, 32),
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	for _, unwanted := range []string{"Compressed blob contents", "Retained go-list files:\n"} {
		if strings.Contains(stdout.String(), unwanted) {
			t.Errorf("status printed %q without -types:\n%s", unwanted, stdout.String())
		}
	}
	// The storage numbers still have to be there — that is what the report is for.
	if !strings.Contains(stdout.String(), "Compressed cache blobs") {
		t.Errorf("status dropped the storage figures too:\n%s", stdout.String())
	}
}

// Classifications were written only after the whole pass finished, so a status that
// ran out of time cached nothing and the next one repeated all of it — which is why
// a cache that had been status-ed many times still reported no types at all.
func TestClassificationSurvivesAnIncompletePass(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	// More outputs than one batch, so persistence has to happen mid-pass.
	outputs := classificationBatch + 20
	for i := range outputs {
		action := sha256.Sum256(fmt.Appendf(nil, "cls-action-%d", i))
		output := sha256.Sum256(fmt.Appendf(nil, "cls-output-%d", i))
		if _, err := st.put(request{
			ID: int64(i), Command: cmdPut,
			ActionID: action[:], OutputID: output[:], BodySize: 4,
		}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
			t.Fatal(err)
		}
	}
	versionDir := st.versionDir
	st.close()

	dbPath := filepath.Join(versionDir, "cache.db")
	classified := func() int {
		db, err := openExistingDB(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close() //nolint:errcheck
		var n int
		if err := db.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM outputs WHERE blob_type IS NOT NULL`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got := classified(); got != 0 {
		t.Fatalf("%d entries classified before any status ran", got)
	}

	_, _, outs, err := readCatalogStatus(dbPath, true)
	if err != nil {
		t.Fatal(err)
	}
	blobTypeStatuses(dbPath, filepath.Join(versionDir, "blobs"), outs, false)

	// Batched writes mean progress is on disk, not held until the end.
	if got := classified(); got < classificationBatch {
		t.Errorf("%d entries classified, want at least one full batch (%d) persisted",
			got, classificationBatch)
	}
}

// The scan enforces maxSize and maxAge, declines while any build is registered, and
// stamps nothing when it declines — so whether it has ever completed is the
// difference between the limits applying and being ignored. That was invisible in
// the report, and answering it meant stat-ing a file by hand.
func TestStatusReportsWhetherMaintenanceHasRun(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	// Registered, so the scan declines and leaves no stamp — the busy-host case.
	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Last scan           never") {
		t.Errorf("status did not report a scan that has never run: %q", stdout.String())
	}
	st.close()

	// Closing stamps both passes on an idle cache.
	stdout.Reset()
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Last scan           <1m ago", "Last sweep          <1m ago"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("status output missing %q: %q", want, stdout.String())
		}
	}
}

// erroringReader yields its payload and then a fixed error, standing in for stdin
// going away mid-session.
type erroringReader struct {
	payload []byte
	err     error
}

func (r *erroringReader) Read(p []byte) (int, error) {
	if len(r.payload) == 0 {
		return 0, r.err
	}
	n := copy(p, r.payload)
	r.payload = r.payload[n:]
	return n, nil
}

// A signalled helper closes its own stdin so the protocol loop ends the same way
// the go command's shutdown ends it, letting the deferred close strip live files
// and unregister the run. Treating that as a failure instead would skip nothing —
// the close is deferred either way — but it would report an error for an ordinary
// shutdown, and leave the run dir behind on the error path before this.
func TestRunTreatsClosedStdinAsEndOfInput(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	var stdout bytes.Buffer
	err := run([]string{"-dir", cacheDir},
		&erroringReader{err: os.ErrClosed}, &stdout)
	if err != nil {
		t.Fatalf("closed stdin reported as a failure: %v", err)
	}

	// The store shut down properly, so its run directory is gone rather than left
	// for the sweep. Nothing was retained here, so the whole directory goes.
	dirs, err := liveRunDirs(filepath.Join(testVersionDir(cacheDir), "live"))
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 0 {
		t.Errorf("live run dirs left behind after shutdown: %v", dirs)
	}
}

// Any other read error still has to wait for in-flight gets: they materialise into
// the run directory that the deferred close then strips.
func TestRunClosesCleanlyOnAReadError(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	var stdout bytes.Buffer
	err := run([]string{"-dir", cacheDir},
		&erroringReader{payload: []byte("{not json\n"), err: io.ErrUnexpectedEOF}, &stdout)
	if err == nil {
		t.Fatal("a malformed request was accepted")
	}

	dirs, err := liveRunDirs(filepath.Join(testVersionDir(cacheDir), "live"))
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 0 {
		t.Errorf("live run dirs left behind after a read error: %v", dirs)
	}
}

// recordingWriter counts Write calls, which is the only way the live file's write
// amplification is visible: the bytes on disk are identical either way.
type recordingWriter struct {
	writes int
	buf    bytes.Buffer
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.buf.Write(p)
}

// A put body is a base64 decoder, and its Read yields at most 768 bytes however
// large the destination, so writing it straight to the live file turned a 32MiB
// artifact into 43,691 write syscalls at 767 bytes each — measured, and matching a
// production trace of ~40,000 writes per blob. The cache's contents are the same
// either way, so only the call count can catch a regression.
func TestPutBatchesLiveFileWrites(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	// Deliberately not a whole number of buffers: a missing Flush would drop the
	// tail, which the content check below catches.
	body := bytes.Repeat([]byte("go build artifact "), (5*liveWriteBuffer/2)/18)
	encoded := base64.StdEncoding.EncodeToString(body)

	live := &recordingWriter{}
	written, err := st.streamPutBody(live, io.Discard,
		base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded)))
	if err != nil {
		t.Fatal(err)
	}

	if written != int64(len(body)) {
		t.Errorf("written = %d, want %d", written, len(body))
	}
	if !bytes.Equal(live.buf.Bytes(), body) {
		t.Errorf("live file got %d bytes, want %d — a dropped Flush loses the tail",
			live.buf.Len(), len(body))
	}
	// Ceiling of body/buffer, plus the flush of the partial tail.
	maxWrites := len(body)/liveWriteBuffer + 2
	if live.writes > maxWrites {
		t.Errorf("live file took %d writes for %d bytes (%d B/write); want at most %d",
			live.writes, len(body), len(body)/live.writes, maxWrites)
	}
}

func TestPutDrainsBodyOnSetupError(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	notDir := filepath.Join(t.TempDir(), "not-dir")
	if err := os.WriteFile(notDir, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	st.blobsDir = notDir

	var input bytes.Buffer
	input.Write(encodedBody([]byte("body")).Bytes())
	writeJSON(t, &input, request{ID: 2, Command: cmdClose})
	br := bufio.NewReader(&input)

	_, err = st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{41}, 32),
		OutputID: bytes.Repeat([]byte{42}, 32),
		BodySize: 4,
	}, br)
	if err == nil {
		t.Fatal("put succeeded with invalid blobs dir")
	}

	req, err := readRequest(br)
	if err != nil {
		t.Fatal(err)
	}
	if req.ID != 2 || req.Command != cmdClose {
		t.Fatalf("next request = %+v, want close request", req)
	}
}

func TestEncoderAndDecoderPools(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	var compressed bytes.Buffer
	enc, err := st.getEncoder(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Write([]byte("body")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	st.putEncoder(enc)

	var compressedAgain bytes.Buffer
	enc, err = st.getEncoder(&compressedAgain)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Write([]byte("again")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	st.putEncoder(enc)

	dec, err := st.getDecoder(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	st.putDecoder(dec)
	dec, err = st.getDecoder(strings.NewReader("not zstd"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(dec); err == nil {
		t.Fatal("pooled decoder accepted invalid zstd")
	}
	dec.Close()
}

func TestVersionedLayout(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	wantVersionDir := testVersionDir(cacheDir)
	if st.versionDir != wantVersionDir {
		t.Fatalf("versionDir = %q, want %q", st.versionDir, wantVersionDir)
	}
	if st.blobsDir != filepath.Join(wantVersionDir, "blobs") {
		t.Fatalf("blobsDir = %q, want under version dir", st.blobsDir)
	}
	if st.liveRoot != filepath.Join(wantVersionDir, "live") {
		t.Fatalf("liveRoot = %q, want under version dir", st.liveRoot)
	}
	if _, err := os.Stat(filepath.Join(wantVersionDir, "cache.db")); err != nil {
		t.Fatal(err)
	}
	var version int
	ctx := context.Background()
	if err := st.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != cacheSchemaVersion {
		t.Fatalf("user_version = %d, want %d", version, cacheSchemaVersion)
	}
	var synchronous int
	if err := st.db.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if synchronous != 1 {
		t.Fatalf("synchronous = %d, want 1 (NORMAL)", synchronous)
	}
}

func TestRejectsMismatchedDBVersion(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "cache.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `PRAGMA user_version = 999`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(dbPath)
	if err == nil {
		_ = db.Close()
		t.Fatal("openDB succeeded with an unsupported user_version")
	}
}

func TestReclaimsAbandonedUnlockedRun(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{18}, 32)
	outputID := bytes.Repeat([]byte{19}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	runID := st.runID
	runDir := st.runDir
	livePath := res.DiskPath
	abandonStore(t, st)

	st, err = newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("abandoned run dir still exists: err=%v", err)
	}
	if _, err := os.Stat(livePath); !os.IsNotExist(err) {
		t.Fatalf("abandoned live file still exists: err=%v", err)
	}
	if got := countRunRows(t, st.db, runID); got != 0 {
		t.Fatalf("abandoned run rows = %d, want 0", got)
	}
}

func TestKeepsLockedRun(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st1, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st1.close()
	actionID := bytes.Repeat([]byte{20}, 32)
	outputID := bytes.Repeat([]byte{21}, 32)
	if _, err := st1.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}

	st2, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.close()

	if _, err := os.Stat(st1.runDir); err != nil {
		t.Fatalf("locked run dir was removed: %v", err)
	}
	if got := countRunRows(t, st2.db, st1.runID); got != 1 {
		t.Fatalf("locked run rows = %d, want 1", got)
	}
}

func TestCleanupDropsMissingRunRecord(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	missingRunDir := filepath.Join(st.liveRoot, "run-missing")
	missingLock := filepath.Join(missingRunDir, "run.lock")
	if err := st.q.registerRun(context.Background(), "run-missing", missingRunDir, missingLock, unixMillis(time.Now())); err != nil {
		t.Fatal(err)
	}
	if err := st.cleanupAbandonedRuns(); err != nil {
		t.Fatal(err)
	}
	if got := countRunRows(t, st.db, "run-missing"); got != 0 {
		t.Fatalf("missing run rows = %d, want 0", got)
	}
}

// Every pass sees the same rows in the same order, so a run that cannot be
// reclaimed used to shadow every row behind it — permanently, since both hot
// callers only log the error. live/ then grew without bound while the reclaim
// that should have emptied it looked like it was running.
func TestCleanupReclaimsPastARunItCannotReclaim(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	// Registered first, so it comes first in the unordered listOtherRuns scan. Its
	// lock file does not exist and the directory denies creating one, so taking
	// the lock fails rather than reporting the run busy.
	blocked := filepath.Join(st.liveRoot, "run-unreclaimable")
	if err := os.MkdirAll(blocked, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o700) })
	if err := st.q.registerRun(context.Background(), "run-unreclaimable", blocked,
		filepath.Join(blocked, "run.lock"), unixMillis(time.Now())); err != nil {
		t.Fatal(err)
	}

	reclaimable := filepath.Join(st.liveRoot, "run-reclaimable")
	if err := os.MkdirAll(reclaimable, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := st.q.registerRun(context.Background(), "run-reclaimable", reclaimable,
		filepath.Join(reclaimable, "run.lock"), unixMillis(time.Now())); err != nil {
		t.Fatal(err)
	}

	// The failure is still reported; it just no longer stops the walk.
	if err := st.cleanupAbandonedRuns(); err == nil {
		t.Error("cleanupAbandonedRuns hid the run it could not reclaim")
	}
	if _, err := os.Stat(reclaimable); !os.IsNotExist(err) {
		t.Errorf("reclaimable run stranded behind a failing one: stat err = %v, want not exist", err)
	}
	if got := countRunRows(t, st.db, "run-reclaimable"); got != 0 {
		t.Errorf("reclaimable run rows = %d, want 0", got)
	}
}

func TestCloseDropsLiveFiles(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{5}, 32)
	outputID := bytes.Repeat([]byte{6}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(res.DiskPath); err != nil {
		t.Fatal(err)
	}
	runDir := st.runDir
	st.close()
	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("live run dir still exists: err=%v", err)
	}
}

func TestCloseStripsPackageArchiveLiveFile(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	exportData := []byte("uFAKE")
	pkgdef := goPkgdef(exportData)
	body := goArchive(pkgdef, bytes.Repeat([]byte("object data"), 1024))
	actionID := bytes.Repeat([]byte{53}, 32)
	outputID := bytes.Repeat([]byte{54}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body)))
	if err != nil {
		t.Fatal(err)
	}
	st.close()

	info, err := os.Stat(res.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() >= int64(len(body)) {
		t.Fatalf("stripped live archive size = %d, want < %d", info.Size(), len(body))
	}
	if got := readExportData(t, res.DiskPath); !bytes.Equal(got, exportData) {
		t.Fatalf("export data = %q, want %q", got, exportData)
	}
	if _, err := os.Stat(st.runDir); err != nil {
		t.Fatalf("run dir with stripped export archive was removed: %v", err)
	}
	if got := readExportData(t, retainedPath(cacheDir, outputID, ".a")); !bytes.Equal(got, exportData) {
		t.Fatalf("retained export data = %q, want %q", got, exportData)
	}
}

func TestCloseStoresRetainedExports(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	exportData := []byte("uFAKE")
	body := goArchive(goPkgdef(exportData), bytes.Repeat([]byte("object data"), 1024))
	outputID := bytes.Repeat([]byte{56}, 32)
	livePaths := make([]string, 0, 2)
	for i := range 2 {
		st, err := newStore(config{
			dir: cacheDir,
		})
		if err != nil {
			t.Fatal(err)
		}
		res, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: bytes.Repeat([]byte{byte(57 + i)}, 32),
			OutputID: outputID,
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body)))
		if err != nil {
			t.Fatal(err)
		}
		st.close()
		livePaths = append(livePaths, res.DiskPath)
	}

	exportPath := retainedPath(cacheDir, outputID, ".a")
	if got := readExportData(t, exportPath); !bytes.Equal(got, exportData) {
		t.Fatalf("retained export data = %q, want %q", got, exportData)
	}
	for _, livePath := range livePaths {
		if got := readExportData(t, livePath); !bytes.Equal(got, exportData) {
			t.Fatalf("export data = %q, want %q", got, exportData)
		}
	}
}

func TestPruneRemovesOrphanRetainedFiles(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir:     cacheDir,
		maxSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	exportData := []byte("uFAKE")
	body := goArchive(goPkgdef(exportData), bytes.Repeat([]byte("object data"), 1024))
	actionID := bytes.Repeat([]byte{61}, 32)
	outputID := bytes.Repeat([]byte{62}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	st.close()

	st, err = newStore(config{
		dir:     cacheDir,
		maxSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	exportPath := retainedPath(cacheDir, outputID, ".a")
	if _, err := os.Stat(exportPath); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(),
		`DELETE FROM outputs WHERE output_id = ?`, idKey(hexOf(outputID))); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(exportPath); !os.IsNotExist(err) {
		t.Fatalf("orphan retained export stat err = %v, want not exist", err)
	}
}

func TestPruneRemovesOldRetainedFilesAndLiveDirs(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	exportData := []byte("uFAKE")
	body := goArchive(goPkgdef(exportData), bytes.Repeat([]byte("object data"), 1024))
	actionID := bytes.Repeat([]byte{63}, 32)
	outputID := bytes.Repeat([]byte{64}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body)))
	if err != nil {
		t.Fatal(err)
	}
	st.close()

	exportPath := retainedPath(cacheDir, outputID, ".a")
	old := trimCutoff(defaultMaxAge, time.Now()).Add(-time.Minute)
	for _, path := range []string{exportPath, filepath.Join(runDirOf(res.DiskPath), "run.lock")} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	st, err = newStore(config{
		dir:    cacheDir,
		maxAge: defaultMaxAge,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(exportPath); !os.IsNotExist(err) {
		t.Fatalf("old retained export stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(res.DiskPath); !os.IsNotExist(err) {
		t.Fatalf("old retained live file stat err = %v, want not exist", err)
	}
}

func TestRefreshingRetainedFileDoesNotKeepOldLiveRun(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	exportData := []byte("uFAKE")
	body := goArchive(goPkgdef(exportData), bytes.Repeat([]byte("object data"), 1024))
	outputID := bytes.Repeat([]byte{65}, 32)

	st, err := newStore(config{
		dir:    cacheDir,
		maxAge: defaultMaxAge,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{66}, 32),
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body)))
	if err != nil {
		t.Fatal(err)
	}
	st.close()

	old := trimCutoff(defaultMaxAge, time.Now()).Add(-time.Minute)
	if err := os.Chtimes(filepath.Join(runDirOf(first.DiskPath), "run.lock"), old, old); err != nil {
		t.Fatal(err)
	}
	expireMaintenanceStamps(t, testVersionDir(cacheDir))

	st, err = newStore(config{
		dir:    cacheDir,
		maxAge: defaultMaxAge,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.put(request{
		ID:       2,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{67}, 32),
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	st.close()

	if _, err := os.Stat(first.DiskPath); !os.IsNotExist(err) {
		t.Fatalf("old retained live file stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(retainedPath(cacheDir, outputID, ".a")); err != nil {
		t.Fatalf("refreshed retained export was removed: %v", err)
	}
}

func TestCloseRefreshesRetainedFileMTime(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	exportData := []byte("uFAKE")
	body := goArchive(goPkgdef(exportData), bytes.Repeat([]byte("object data"), 1024))
	outputID := bytes.Repeat([]byte{66}, 32)
	for i := range 2 {
		st, err := newStore(config{
			dir:    cacheDir,
			maxAge: defaultMaxAge,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: bytes.Repeat([]byte{byte(67 + i)}, 32),
			OutputID: outputID,
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
		st.close()
		if i == 0 {
			exportPath := retainedPath(cacheDir, outputID, ".a")
			old := trimCutoff(defaultMaxAge, time.Now()).Add(-time.Minute)
			if err := os.Chtimes(exportPath, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}

	exportPath := retainedPath(cacheDir, outputID, ".a")
	info, err := os.Stat(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().After(trimCutoff(defaultMaxAge, time.Now())) {
		t.Fatalf("retained export mtime = %v, want refreshed after cutoff", info.ModTime())
	}
}

func TestCloseRetainsGeneratedCgoSourceLiveFile(t *testing.T) {
	t.Parallel()

	body := []byte("// Code generated by cmd/cgo; DO NOT EDIT.\n\npackage net\n\nconst x = 1\n")
	assertCloseRetainsGeneratedGoSource(t, body, bytes.Repeat([]byte{59}, 32), bytes.Repeat([]byte{60}, 32))
}

func TestCloseRetainsGeneratedTestmainLiveFile(t *testing.T) {
	t.Parallel()

	body := []byte("\n// Code generated by 'go test'. DO NOT EDIT.\n\npackage main\n\nfunc main() {}\n")
	assertCloseRetainsGeneratedGoSource(t, body, bytes.Repeat([]byte{75}, 32), bytes.Repeat([]byte{76}, 32))
}

func assertCloseRetainsGeneratedGoSource(t *testing.T, body, actionID, outputID []byte) {
	t.Helper()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body)))
	if err != nil {
		t.Fatal(err)
	}
	st.close()

	got, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("retained live source = %q, want %q", got, body)
	}
	retained, err := os.ReadFile(retainedPath(cacheDir, outputID, ".go"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(retained, body) {
		t.Fatalf("retained source = %q, want %q", retained, body)
	}
}

func TestPruneKeepsLiveBlobs(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{7}, 32)
	outputID := bytes.Repeat([]byte{8}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 64,
	}, bufio.NewReader(encodedBody(bytes.Repeat([]byte("x"), 64)))); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); err != nil {
		t.Fatalf("live entry was pruned: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(outputID))); err != nil {
		t.Fatalf("live blob was pruned: %v", err)
	}
}

func TestPruneSkipsScanWhileLifecycleLockIsHeld(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	blobPath := orphanBlob(t, st, strings.Repeat("d", 64))

	lock := flock.New(st.lifecycleLockPath)
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	// Contended: prune must neither scan nor wait. Waiting is what would put
	// another process's whole scan on this process's exit path.
	prunePromptly(t, st)
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("prune scanned without holding the lifecycle lock: %v", err)
	}

	// Uncontended: the stamp is still stale, so the next attempt does the work.
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("orphan blob stat err = %v, want not exist", err)
	}
}

func TestNewStoreUsesLifecycleLock(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	if err := os.MkdirAll(testVersionDir(cacheDir), 0o777); err != nil {
		t.Fatal(err)
	}
	lock := flock.New(filepath.Join(testVersionDir(cacheDir), "lifecycle.lock"))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	type result struct {
		st  *store
		err error
	}
	done := make(chan result, 1)
	go func() {
		st, err := newStore(config{dir: cacheDir})
		done <- result{st: st, err: err}
	}()

	select {
	case res := <-done:
		if res.st != nil {
			res.st.close()
		}
		t.Fatalf("newStore finished while lifecycle lock was held: %v", res.err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	res := <-done
	if res.err != nil {
		t.Fatal(res.err)
	}
	res.st.close()
}

func TestPruneRemovesUnusedBlobs(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir:     cacheDir,
		maxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{9}, 32)
	outputID := bytes.Repeat([]byte{10}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 64,
	}, bufio.NewReader(encodedBody(bytes.Repeat([]byte("x"), 64)))); err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	st.close()

	st, err = newStore(config{
		dir:     cacheDir,
		maxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("blob was not pruned: %v", err)
	}
	// The entry may outlive the blob — nothing indexes output back to action, so
	// eviction cannot find it. What has to hold is that it stops being a hit, and
	// that asking cleans it up.
	res, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Miss {
		t.Fatalf("get response = %+v after eviction, want miss", res)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); !errorsIs(err, sql.ErrNoRows) {
		t.Fatalf("a missed entry was not cleaned up: %v", err)
	}
}

func TestPruneRemovesOrphanBlobsWithSizePruningDisabled(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	blobPath := orphanBlob(t, st, strings.Repeat("a", 64))
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("orphan blob stat err = %v, want not exist", err)
	}
}

// orphanBlob writes a blob file with no catalog entry, which only a
// maintenance scan removes.
func orphanBlob(t *testing.T, st *store, outputID string) string {
	t.Helper()
	if err := os.MkdirAll(st.blobDir(outputID), 0o777); err != nil {
		t.Fatal(err)
	}
	path := st.blobPath(outputID)
	if err := os.WriteFile(path, []byte("orphan"), 0o666); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPruneSkipsScanWithinInterval(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(pruneStampPath(st.versionDir)); err != nil {
		t.Fatalf("prune did not stamp: %v", err)
	}

	blobPath := orphanBlob(t, st, strings.Repeat("a", 64))
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("prune scanned within pruneInterval: %v", err)
	}

	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("orphan blob stat err = %v, want not exist", err)
	}
	// The stamp is rewritten in place, so the refresh depends on the write
	// bumping mtime even though the size does not change. If it ever stops
	// doing so the gate never reopens and maintenance silently stops.
	info, err := os.Stat(pruneStampPath(st.versionDir))
	if err != nil {
		t.Fatal(err)
	}
	if age := time.Since(info.ModTime()); age >= pruneInterval {
		t.Fatalf("scan left the stamp %v old, want refreshed", age)
	}
}

func TestPlanPruneDoesNotTakeTheLifecycleLock(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir(), maxAge: defaultMaxAge, maxSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	orphanBlob(t, st, strings.Repeat("f", 64))

	// Deciding what to remove is the expensive part of a scan. Holding the lock
	// across it is what stalled a starting build, since newStore takes the same
	// lock, so planning must not need it.
	lock := flock.New(st.lifecycleLockPath)
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	type result struct {
		plan prunePlan
		err  error
	}
	done := make(chan result, 1)
	go func() {
		plan, err := st.planPrune(time.Now())
		done <- result{plan: plan, err: err}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatal(res.err)
		}
		// A plan that found nothing would pass this test for the wrong reason.
		if res.plan.empty() {
			t.Fatal("planPrune found nothing to remove, so it proves nothing here")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("planPrune waited for the lifecycle lock")
	}
}

// The default config sets both limits (20GiB, 5d), and eviction picks its
// candidates from the catalog as it was before the age delete ran. Crediting a
// candidate whose rows the age delete already removed ends the loop early and
// leaves the cache over budget after a completed, stamped scan.
// planPrune runs without the lifecycle lock, so a build can start, read, and exit
// while it walks — and it now flushes its reads every 30s and again on the way out.
// Eviction chose its candidates before any of that, so the delete has to re-assert
// that the output is still cold. Without it, the output picked as the coldest in the
// cache is evicted precisely when it has just become the hottest.
func TestEvictionKeepsAnOutputReadDuringPlanning(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir(), maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	outputs := make([][]byte, 0, 6)
	for i := range 6 {
		action := sha256.Sum256(fmt.Appendf(nil, "warm-action-%d", i))
		output := sha256.Sum256(fmt.Appendf(nil, "warm-output-%d", i))
		body := incompressibleBody(t, 4096, int64(i))
		if _, err := st.put(request{
			ID:       int64(i),
			Command:  cmdPut,
			ActionID: action[:],
			OutputID: output[:],
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, output[:])
	}
	// Distinct access times, so the eviction order is determined rather than
	// arbitrary and the coldest candidate is a known output.
	for i, output := range outputs {
		if _, err := st.db.ExecContext(context.Background(),
			`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`,
			unixMillis(time.Now())-int64(len(outputs)-i)*1000, idKey(hexOf(output))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	before, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	st.maxSize = before / 2

	plan, err := st.planPrune(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.blobs) == 0 {
		t.Fatal("nothing planned for eviction; the budget did not bite")
	}
	coldest := plan.blobs[0].outputID

	// A build reads the coldest output and exits, flushing the access — exactly
	// what the periodic flush now makes visible mid-pass.
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`,
		unixMillis(time.Now()), idKey(coldest)); err != nil {
		t.Fatal(err)
	}

	expireMaintenanceStamps(t, st.versionDir)
	if err := st.applyPrune(plan); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(st.blobPath(coldest)); err != nil {
		t.Errorf("evicted an output read during planning: %v", err)
	}
	var surviving int
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outputs WHERE output_id = ?`, idKey(coldest)).Scan(&surviving); err != nil {
		t.Fatal(err)
	}
	if surviving == 0 {
		t.Error("deleted the entries of an output read during planning")
	}
	// It still has to have evicted something, or the guard is just disabling
	// eviction.
	after, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	if after >= before {
		t.Errorf("compressed size %d did not fall from %d; nothing was evicted", after, before)
	}
}

// Several actions can map to one output. The guard has to be per output, not per
// row: deleting the actions that are still cold while a warm one survives reports
// rows affected, so the caller unlinks the blob and leaves that row pointing at
// nothing — a hit that resolves to a missing file.
func TestEvictionKeepsAnOutputReadWhilePlanning(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir(), maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	shared := sha256.Sum256([]byte("shared-output"))
	body := incompressibleBody(t, 8192, 99)
	for i := range 2 {
		action := sha256.Sum256(fmt.Appendf(nil, "shared-action-%d", i))
		if _, err := st.put(request{
			ID: int64(i), Command: cmdPut,
			ActionID: action[:], OutputID: shared[:], BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
	}
	// A second, warmer output so there is something else to evict and the budget
	// is reachable without the shared one.
	other := sha256.Sum256([]byte("other-output"))
	otherAction := sha256.Sum256([]byte("other-action"))
	otherBody := incompressibleBody(t, 8192, 100)
	if _, err := st.put(request{
		ID: 9, Command: cmdPut,
		ActionID: otherAction[:], OutputID: other[:], BodySize: int64(len(otherBody)),
	}, bufio.NewReader(encodedBody(otherBody))); err != nil {
		t.Fatal(err)
	}

	now := unixMillis(time.Now())
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`, now-10_000, idKey(hexOf(shared[:]))); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`, now, idKey(hexOf(other[:]))); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	before, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	st.maxSize = before / 4

	plan, err := st.planPrune(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.blobs) == 0 || plan.blobs[0].outputID != hexOf(shared[:]) {
		t.Fatalf("expected the shared output to be the coldest candidate, got %v", plan.blobs)
	}

	// It is read and flushed while the plan is in hand, which moves the output's
	// clock past what the plan recorded.
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`,
		now+10_000, idKey(hexOf(shared[:]))); err != nil {
		t.Fatal(err)
	}

	expireMaintenanceStamps(t, st.versionDir)
	if err := st.applyPrune(plan); err != nil {
		t.Fatal(err)
	}

	var surviving int
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outputs WHERE output_id = ?`, idKey(hexOf(shared[:]))).Scan(&surviving); err != nil {
		t.Fatal(err)
	}
	if surviving == 0 {
		t.Fatal("evicted a warm output entirely")
	}
	// Whatever rows survive must still resolve, so the blob has to be there.
	if _, err := os.Stat(st.blobPath(hexOf(shared[:]))); err != nil {
		t.Errorf("%d entries survive but the blob is gone: %v", surviving, err)
	}
}

func TestPruneEnforcesMaxSizeWhenMaxAgeAlsoFires(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir, maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	// Incompressible, so the stored size is predictable and the cache really
	// does exceed the budget set below.
	actions := make([][]byte, 0, 6)
	for i := range 6 {
		action := sha256.Sum256(fmt.Appendf(nil, "budget-action-%d", i))
		output := sha256.Sum256(fmt.Appendf(nil, "budget-output-%d", i))
		body := incompressibleBody(t, 4096, int64(i))
		if _, err := st.put(request{
			ID:       int64(i),
			Command:  cmdPut,
			ActionID: action[:],
			OutputID: output[:],
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
		actions = append(actions, action[:])
	}

	// Age out half of them, so the age delete frees bytes that eviction's
	// candidate list still believes it is responsible for.
	stale := unixMillis(trimCutoff(defaultMaxAge, time.Now())) - int64(time.Minute/time.Millisecond)
	for _, action := range actions[:3] {
		if _, err := st.db.ExecContext(context.Background(),
			`UPDATE entries SET accessed_at = ? WHERE action_id = ?`, stale, idKey(hexOf(action))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}

	// Budget for roughly one surviving output, so eviction has to do real work
	// after the age delete rather than coasting on bytes it did not free.
	before, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	st.maxSize = before / 5
	if before <= st.maxSize {
		t.Fatalf("cache is %d bytes, not over the %d budget: nothing would be evicted", before, st.maxSize)
	}

	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	total, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	if total > st.maxSize {
		t.Fatalf("cache is %d bytes after a completed scan, over its %d budget", total, st.maxSize)
	}
}

func TestPruneSkipsScanWhileAnotherScanHoldsMaintenanceLock(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	blobPath := orphanBlob(t, st, strings.Repeat("c", 64))

	// Another process is already analysing. Planning is expensive and its result
	// would be discarded once that one stamps, so this scan should decline
	// rather than duplicate it — and must not wait, since it is on an exit path.
	lock := flock.New(maintenanceLockPath(st.versionDir))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	prunePromptly(t, st)
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("scan ran while another process held the maintenance lock: %v", err)
	}

	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("orphan blob stat err = %v, want not exist", err)
	}
}

func TestPruneDoesNotCreateRunLockWhileCheckingLiveRuns(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir(), maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	// A run dir mid-removal has no run.lock. Asking whether it is expired must
	// not recreate one, or the removal's rmdir fails and the run comes back.
	runDir := filepath.Join(st.liveRoot, "run-halfremoved")
	if err := os.MkdirAll(runDir, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := st.planPrune(time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "run.lock")); !os.IsNotExist(err) {
		t.Fatalf("planning created run.lock: stat err = %v, want not exist", err)
	}
}

// Age pruning no longer asks whether anything is stale before running: without an
// access index on entries that question costs the same table scan as the delete.
// So the pass always runs, and what needs asserting is that it keeps what is fresh.
func TestPruneKeepsEverythingWhenNothingIsStale(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir(), maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{97}, 32)
	outputID := bytes.Repeat([]byte{98}, 32)
	body := []byte("fresh")
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(),
		`DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := st.lookupEntry(hexOf(actionID)); err != nil {
		t.Fatalf("a fresh entry was pruned: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(outputID))); err != nil {
		t.Fatalf("a fresh blob was pruned: %v", err)
	}
}

// A size-evicted output's retained export has to go with it. The orphan list is
// built before eviction runs, so the output was still referenced then and its
// retained files were not candidates — eviction has to offer them afterwards.
// retainedPaths is where one retained go-list file lives: the durable copy under
// retained/, and the hard link in a live run dir that is the path a tool
// actually opened.
type retainedPaths struct {
	export string
	live   string
	runDir string
}

// retainedCache puts a package archive and closes, which is what turns a live
// file into a retained export archive plus a hard link in the live run dir.
func retainedCache(t *testing.T, cfg config, outputID []byte) retainedPaths {
	t.Helper()
	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	body := goArchive(goPkgdef([]byte("uFAKE")), bytes.Repeat([]byte("object data"), 64))
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{120}, 32),
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body)))
	if err != nil {
		t.Fatal(err)
	}
	st.close()

	paths := retainedPaths{
		export: retainedPath(cfg.dir, outputID, ".a"),
		live:   res.DiskPath,
		runDir: runDirOf(res.DiskPath),
	}
	if _, err := os.Stat(paths.export); err != nil {
		t.Fatalf("no retained export was produced: %v", err)
	}
	if _, err := os.Stat(paths.live); err != nil {
		t.Fatalf("no escaped live path survived close: %v", err)
	}
	return paths
}

func TestRetainedFilesExpireOnTheirOwnAge(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	cfg := config{dir: cacheDir, maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	outputID := bytes.Repeat([]byte{121}, 32)
	paths := retainedCache(t, cfg, outputID)

	// Older than the retained age but far younger than maxAge, so only the
	// separate setting can reclaim it. The live run dir carries the escaped
	// path's hard link, so it has to go too or the inode survives.
	old := trimCutoff(time.Hour, time.Now()).Add(-time.Minute)
	for _, path := range []string{paths.export, filepath.Join(paths.runDir, "run.lock")} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(paths.export); !os.IsNotExist(err) {
		t.Fatalf("retained export stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(paths.live); !os.IsNotExist(err) {
		t.Fatalf("escaped live path stat err = %v, want not exist", err)
	}
	// The blob is the cache entry and is nowhere near maxAge: losing a retained
	// file costs a re-strip, losing this would cost a rebuild.
	if _, err := os.Stat(st.blobPath(hexOf(outputID))); err != nil {
		t.Fatalf("blob was pruned along with its retained file: %v", err)
	}
}

// A live run dir is guarded by its own run.lock, so it can be reclaimed while
// other builds are running. A retained file has no such guard and waits for an
// idle cache. Both are equally expired here, so the idleness gate is the only
// thing telling them apart — and bundling the sweep behind that gate is what let
// run dirs pile up on a machine where builds overlap continuously.
func TestSweepRemovesExpiredLiveRunWhileAnotherRunIsRegistered(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir(), maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	paths := retainedCache(t, cfg, bytes.Repeat([]byte{122}, 32))

	old := trimCutoff(time.Hour, time.Now()).Add(-time.Minute)
	for _, path := range []string{paths.export, filepath.Join(paths.runDir, "run.lock")} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	// This store's own run stays registered, so the cache is never idle.
	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(paths.runDir); !os.IsNotExist(err) {
		t.Fatalf("expired live run survived a busy cache: stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(paths.export); err != nil {
		t.Fatalf("retained file was deleted while a run was registered: %v", err)
	}
}

// A build materialises one live file per output it touches, and a large one was
// measured at 28,000 in a single directory. At that fanout create and unlink cost
// filesystem metadata and journal IO, not just lookups.
func TestLiveFilesAreShardedWithinTheRunDir(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	shards := map[string]bool{}
	for i := range 24 {
		outputID := bytes.Repeat([]byte{byte(i + 1)}, 32)
		res, err := st.put(request{
			ID:       int64(i + 1),
			Command:  cmdPut,
			ActionID: bytes.Repeat([]byte{byte(200 - i)}, 32),
			OutputID: outputID,
			BodySize: 4,
		}, bufio.NewReader(encodedBody([]byte("body"))))
		if err != nil {
			t.Fatal(err)
		}
		if got := runDirOf(res.DiskPath); got != st.runDir {
			t.Fatalf("live file %q is not one shard inside %q", res.DiskPath, st.runDir)
		}
		// The shard has to come from the output ID, or a run dir full of one
		// package's outputs would still land in one directory.
		want := outputShard(hexOf(outputID))
		if got := filepath.Base(filepath.Dir(res.DiskPath)); got != want {
			t.Errorf("live file shard = %q, want %q", got, want)
		}
		shards[want] = true
	}

	entries, err := os.ReadDir(st.runDir)
	if err != nil {
		t.Fatal(err)
	}
	files := 0
	for _, entry := range entries {
		if !entry.IsDir() && entry.Name() != "run.lock" {
			files++
		}
	}
	if files != 0 {
		t.Errorf("%d live files sit directly in the run dir, want 0", files)
	}
	if len(shards) < 2 {
		t.Errorf("24 distinct outputs produced %d shard(s); the split is not by output", len(shards))
	}
}

// A run dir that retains an escaped path outlives its build, so it must not keep
// the shards it no longer needs — 256 empty directories per retained run would
// trade one metadata problem for another.
func TestCloseLeavesNoEmptyShardsInARetainedRunDir(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir(), maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// One archive, which escapes and is retained, so the run dir outlives close and
	// keeps its shard. Then plain bodies in other shards, which do not escape and
	// are removed — leaving those shards empty and nothing else to clear them.
	archive := goArchive(goPkgdef([]byte("uFAKE")), bytes.Repeat([]byte("object data"), 64))
	kept, err := st.put(request{
		ID: 1, Command: cmdPut,
		ActionID: bytes.Repeat([]byte{129}, 32),
		OutputID: bytes.Repeat([]byte{128}, 32),
		BodySize: int64(len(archive)),
	}, bufio.NewReader(encodedBody(archive)))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if _, err := st.put(request{
			ID: int64(i + 2), Command: cmdPut,
			ActionID: bytes.Repeat([]byte{byte(140 + i)}, 32),
			OutputID: bytes.Repeat([]byte{byte(150 + i)}, 32),
			BodySize: 4,
		}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
			t.Fatal(err)
		}
	}
	runDir := st.runDir
	st.close()

	paths := retainedPaths{live: kept.DiskPath, runDir: runDir}
	var empty []string
	if err := filepath.WalkDir(paths.runDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == paths.runDir {
			return err
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			empty = append(empty, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Errorf("retained run dir kept empty shards: %v", empty)
	}
	// And the escaped path itself is still there, still one shard deep.
	if _, err := os.Stat(paths.live); err != nil {
		t.Fatalf("pruning empty shards took the escaped path with it: %v", err)
	}
}

func TestNewStoreShardsTheRunDirectory(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	rel, err := filepath.Rel(st.liveRoot, st.runDir)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(rel) == "." {
		t.Errorf("run dir %q is directly under live/, want one shard deep", rel)
	}
	entries, err := os.ReadDir(st.liveRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), runDirPrefix) {
			t.Errorf("live/ holds run dir %q directly", entry.Name())
		}
	}
	dirs, err := liveRunDirs(st.liveRoot)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(dirs, st.runDir) {
		t.Errorf("liveRunDirs = %v, want it to contain %q", dirs, st.runDir)
	}
}

// Caches written before sharding keep run dirs directly under live/ — and those
// are exactly the caches with a backlog to clear, so a walker that only looked one
// shard deep would strand every one of them permanently.
func TestSweepReclaimsUnshardedRunDirs(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir(), maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	legacy := filepath.Join(st.liveRoot, "run-fromtheoldlayout")
	if err := os.MkdirAll(legacy, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "aa11-unstripped"), []byte("artifact"), 0o666); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(legacy, "run.lock")
	if err := os.WriteFile(lock, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	old := trimCutoff(time.Hour, time.Now()).Add(-time.Minute)
	if err := os.Chtimes(lock, old, old); err != nil {
		t.Fatal(err)
	}

	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Errorf("unsharded run dir survived: stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(st.runDir); err != nil {
		t.Errorf("this store's own sharded run dir was removed: %v", err)
	}
}

// status reports on whatever layout it finds, so a mixed cache mid-migration is
// counted once per run rather than once per layout.
func TestStatusCountsRunsInBothLayouts(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	legacy := filepath.Join(st.liveRoot, "run-fromtheoldlayout")
	if err := os.MkdirAll(legacy, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "run.lock"), nil, 0o666); err != nil {
		t.Fatal(err)
	}

	active, inactive, err := readLiveStatus(st.liveRoot)
	if err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Errorf("active = %d, want 1 (this store's sharded run)", active)
	}
	if inactive != 1 {
		t.Errorf("inactive = %d, want 1 (the unsharded run)", inactive)
	}
}

// Nothing holds a live run dir's lock once the build that made it has exited, so
// age is the only thing protecting it — and the escaped go-list paths inside are
// the entire reason it outlived that build. A sweep that ignored the age would
// yank them out from under a tool still holding one.
func TestSweepKeepsLiveRunYoungerThanTheRetainedAge(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir(), maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	paths := retainedCache(t, cfg, bytes.Repeat([]byte{126}, 32))

	// close stamped run.lock, so the dir is as fresh as it ever gets.
	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(paths.live); err != nil {
		t.Fatalf("sweep removed an escaped path from a live run dir well inside its age: %v", err)
	}
}

// With age-based pruning off, nothing may expire by age — live run dirs included.
// trimCutoff of a zero age is an hour ago rather than the beginning of time, so
// the check for a disabled age is what has to stop the sweep; the cutoff
// arithmetic would happily expire everything older than mtimeInterval.
func TestSweepKeepsLiveRunsWhenAgeBasedPruningIsDisabled(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir()} // maxAge and maxRetainedAge both zero
	paths := retainedCache(t, cfg, bytes.Repeat([]byte{127}, 32))

	old := time.Now().Add(-24 * time.Hour)
	if err := os.Chtimes(filepath.Join(paths.runDir, "run.lock"), old, old); err != nil {
		t.Fatal(err)
	}

	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(paths.live); err != nil {
		t.Fatalf("sweep expired a live run with age-based pruning disabled: %v", err)
	}
}

// The two passes are stamped independently and neither implies the other. In
// normal operation the scan's stamp is never fresher than the sweep's — it is
// only written when a scan completes — so the passes tend to come due together
// and a suite that only ever exercises them in lockstep would not notice the
// sweep being made to depend on the scan.
func TestSweepRunsWhenOnlyItsOwnStampIsStale(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir(), maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	paths := retainedCache(t, cfg, bytes.Repeat([]byte{124}, 32))

	old := trimCutoff(time.Hour, time.Now()).Add(-time.Minute)
	for _, path := range []string{paths.export, filepath.Join(paths.runDir, "run.lock")} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	// Idle, so a scan would really delete: the retained file below is the witness
	// for whether one ran, which a stamp cannot be — a scan that declines for a
	// busy cache leaves the stamp untouched too.
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	backdate := time.Now().Add(-2 * pruneInterval)
	if err := os.Chtimes(sweepStampPath(st.versionDir), backdate, backdate); err != nil {
		t.Fatal(err)
	}

	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(paths.runDir); !os.IsNotExist(err) {
		t.Fatalf("sweep did not run on its own stamp: stat err = %v, want not exist", err)
	}
	// The retained file is equally expired and the cache is idle, so the scan's
	// own interval is the only thing keeping it. Two checks enforce that — the
	// early return in prune and the re-check inside applyPrune — and this pins the
	// outcome rather than either one; see the test below for the early return.
	if _, err := os.Stat(paths.export); err != nil {
		t.Fatalf("scan deleted while its own interval had not elapsed: %v", err)
	}
}

// Whether the scan was skipped or merely declined is invisible in the cache, but
// it is the whole cost: planning walks the retained tree and reconciles 256 blob
// shards. A sweep coming due must not drag that along, and must not reach for the
// lifecycle lock — on an idle cache it would take it, briefly blocking any build
// trying to open the store.
func TestSweepAloneDoesNotAttemptTheLifecycleLock(t *testing.T) {
	cfg := config{dir: t.TempDir(), maxAge: defaultMaxAge, maxRetainedAge: time.Hour, verbose: true}
	paths := retainedCache(t, cfg, bytes.Repeat([]byte{125}, 32))

	old := trimCutoff(time.Hour, time.Now()).Add(-time.Minute)
	if err := os.Chtimes(filepath.Join(paths.runDir, "run.lock"), old, old); err != nil {
		t.Fatal(err)
	}

	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	// Idle: a registered run would make the scan bail before it ever reached the
	// lock, hiding the very attempt this test is looking for.
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	backdate := time.Now().Add(-2 * pruneInterval)
	if err := os.Chtimes(sweepStampPath(st.versionDir), backdate, backdate); err != nil {
		t.Fatal(err)
	}

	// Stand in for another process holding the lock, so any attempt on it reports
	// contention instead of passing silently.
	lock := flock.New(st.lifecycleLockPath)
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	var logged bytes.Buffer
	flags := log.Flags()
	log.SetOutput(&logged)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(flags)
	})

	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(paths.runDir); !os.IsNotExist(err) {
		t.Fatalf("sweep did not run: stat err = %v, want not exist", err)
	}
	if strings.Contains(logged.String(), "another process holds the cache lock") {
		t.Errorf("sweep alone reached for the lifecycle lock: %q", logged.String())
	}
}

// The sweep walks every live run dir and locks each one, so it must not run on
// every exit: with maxRetainedAge following maxAge, a long editor session
// accumulates days of dirs and would pay for the walk on every build.
func TestSweepSkipsExpiredLiveRunWithinInterval(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir(), maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	paths := retainedCache(t, cfg, bytes.Repeat([]byte{123}, 32))

	old := trimCutoff(time.Hour, time.Now()).Add(-time.Minute)
	if err := os.Chtimes(filepath.Join(paths.runDir, "run.lock"), old, old); err != nil {
		t.Fatal(err)
	}

	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	// Only the scan is overdue. retainedCache's close stamped the sweep, so the
	// interval has not elapsed for it.
	backdate := time.Now().Add(-2 * pruneInterval)
	if err := os.Chtimes(pruneStampPath(st.versionDir), backdate, backdate); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(paths.runDir); err != nil {
		t.Fatalf("sweep ran within its interval: %v", err)
	}
}

func TestRetainedFilesFollowMaxAgeByDefault(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	// maxRetainedAge unset: the previous behaviour, so a long editor session
	// holding an escaped path is unaffected until someone opts in.
	cfg := config{dir: cacheDir, maxAge: defaultMaxAge}
	paths := retainedCache(t, cfg, bytes.Repeat([]byte{122}, 32))

	old := trimCutoff(time.Hour, time.Now()).Add(-time.Minute)
	for _, path := range []string{paths.export, filepath.Join(paths.runDir, "run.lock")} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(paths.export); err != nil {
		t.Fatalf("retained export expired at an hour without being asked to: %v", err)
	}
	if _, err := os.Stat(paths.live); err != nil {
		t.Fatalf("escaped live path expired at an hour without being asked to: %v", err)
	}
}

func TestRetainedFileIsRecreatedAfterExpiring(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	cfg := config{dir: cacheDir, maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	outputID := bytes.Repeat([]byte{123}, 32)
	paths := retainedCache(t, cfg, outputID)

	old := trimCutoff(time.Hour, time.Now()).Add(-time.Minute)
	for _, path := range []string{paths.export, filepath.Join(paths.runDir, "run.lock")} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	st.close()
	if _, err := os.Stat(paths.export); !os.IsNotExist(err) {
		t.Fatalf("retained export stat err = %v, want not exist", err)
	}

	// Expiring one is cheap precisely because using the output again rebuilds it:
	// the get materializes a live file and close re-strips it. That is what makes
	// a short retained age safe where a short maxAge would not be.
	st, err = newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := st.get(request{
		ID:       1,
		Command:  cmdGet,
		ActionID: bytes.Repeat([]byte{120}, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Miss {
		t.Fatal("get missed: the blob should have outlived its retained file")
	}
	st.close()

	if _, err := os.Stat(paths.export); err != nil {
		t.Fatalf("retained export was not recreated by using the output: %v", err)
	}
}

func TestPruneRemovesRetainedFilesOfEvictedOutputs(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	evicted := bytes.Repeat([]byte{101}, 32)
	kept := bytes.Repeat([]byte{103}, 32)
	body := goArchive(goPkgdef([]byte("uFAKE")), incompressibleBody(t, 8<<10, 1))
	for _, tc := range []struct{ action, output []byte }{
		{bytes.Repeat([]byte{100}, 32), evicted},
		{bytes.Repeat([]byte{102}, 32), kept},
	} {
		if _, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: tc.action,
			OutputID: tc.output,
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
	}
	st.close()

	exportPath := retainedPath(cacheDir, evicted, ".a")
	if _, err := os.Stat(exportPath); err != nil {
		t.Fatalf("no retained export to evict: %v", err)
	}

	st, err = newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`,
		unixMillis(time.Now().Add(-time.Hour)), idKey(hexOf(evicted))); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}

	total, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	// Room for one output, so the older of the two is evicted by size — not by
	// age, which would have taken its retained file along a different path.
	st.maxSize = total * 2 / 3
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(st.blobPath(hexOf(evicted))); !os.IsNotExist(err) {
		t.Fatalf("evicted blob stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(exportPath); !os.IsNotExist(err) {
		t.Fatalf("retained export of an evicted output survived: stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(retainedPath(cacheDir, kept, ".a")); err != nil {
		t.Fatalf("retained export of a surviving output was removed: %v", err)
	}
}

func TestPruneDeletesNothingWhileARunIsRegistered(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	// This store's own run is registered, so the cache is in use. put and get
	// never take the lifecycle lock, so an idle cache is the only thing that
	// makes deletion safe — nothing may go while a build could be writing.
	blobPath := orphanBlob(t, st, strings.Repeat("9", 64))
	plan, err := st.planPrune(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.orphans) == 0 {
		t.Fatal("planPrune did not see the orphan blob")
	}
	if err := st.applyPrune(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("prune deleted while a run was registered: %v", err)
	}
	// And it must not stamp, or the scan it declined would be skipped for an
	// hour after the cache went idle.
	if _, err := os.Stat(pruneStampPath(st.versionDir)); !os.IsNotExist(err) {
		t.Fatalf("prune stamped without scanning: stat err = %v, want not exist", err)
	}
}

func TestPruneKeepsOrphanThatGainedAnEntryAfterPlanning(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}

	outputID := strings.Repeat("7", 64)
	blobPath := orphanBlob(t, st, outputID)
	plan, err := st.planPrune(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.orphans) == 0 {
		t.Fatal("planPrune did not see the orphan blob")
	}

	// A put writes its blob before inserting the catalog row, so a blob can be
	// unreferenced while the unlocked scan walks it and referenced by the time
	// the scan deletes. Deleting it then would leave a row with no blob.
	now := time.Now()
	if err := st.q.upsertEntry(context.Background(), entry{
		ActionID:   strings.Repeat("8", 64),
		OutputID:   outputID,
		Size:       6,
		CreatedAt:  now,
		AccessedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := st.applyPrune(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("blob referenced after planning was deleted anyway: %v", err)
	}
}

func TestPruneKeepsRetainedFileRefreshedAfterPlanning(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir, maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	outputID := bytes.Repeat([]byte{95}, 32)
	body := goArchive(goPkgdef([]byte("uFAKE")), bytes.Repeat([]byte("object data"), 64))
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{96}, 32),
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	st.close()

	exportPath := retainedPath(cacheDir, outputID, ".a")
	old := trimCutoff(defaultMaxAge, time.Now()).Add(-time.Minute)
	if err := os.Chtimes(exportPath, old, old); err != nil {
		t.Fatal(err)
	}

	st, err = newStore(config{dir: cacheDir, maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	plan, err := st.planPrune(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.retained) == 0 {
		t.Fatal("planPrune did not see the expired retained file")
	}

	// A build that ran while the scan was planning would have refreshed this
	// via markRetainedFileUsed, which is the whole point of the mtime.
	fresh := time.Now()
	if err := os.Chtimes(exportPath, fresh, fresh); err != nil {
		t.Fatal(err)
	}
	// The first close already stamped; without this applyPrune declines and the
	// file survives for the wrong reason.
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.applyPrune(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(exportPath); err != nil {
		t.Fatalf("retained file refreshed after planning was deleted anyway: %v", err)
	}
}

func TestPruneRechecksStampAfterTakingTheLock(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	// The stamp is read before the lock is taken, and again before anything is
	// deleted, so two processes exiting together can both plan a scan. Whichever
	// reaches the deletions second has to notice the first already did the work
	// rather than repeating it. Planning and then applying against a stamp that
	// went fresh in between is exactly that process, and prune() cannot reach
	// the state on its own.
	blobPath := orphanBlob(t, st, strings.Repeat("e", 64))
	plan, err := st.planPrune(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.applyPrune(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("scan repeated work another process had already stamped: %v", err)
	}

	expireMaintenanceStamps(t, st.versionDir)
	if err := st.applyPrune(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("orphan blob stat err = %v, want not exist", err)
	}
}

// Not parallel: it swaps the global log sink to tell the two skip paths apart.
func TestPruneWithFreshStampDoesNotTouchTheLifecycleLock(t *testing.T) {
	st, err := newStore(config{dir: t.TempDir(), verbose: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	// Stand in for another process holding the lock.
	lock := flock.New(st.lifecycleLockPath)
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	var logged bytes.Buffer
	flags := log.Flags()
	log.SetOutput(&logged)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(flags)
	})

	// A fresh stamp must short-circuit before the lock is attempted at all.
	// Attempting it would report contention — and on an idle cache would take
	// it, briefly blocking any build trying to open the store. Bounded, because
	// a prune that blocked here would otherwise hang the whole test binary and
	// swallow its siblings' failures.
	prunePromptly(t, st)
	if strings.Contains(logged.String(), "another process holds the cache lock") {
		t.Fatalf("prune attempted the lifecycle lock with a fresh stamp: %q", logged.String())
	}

	// With the stamp stale it does attempt the lock, and reports the loss.
	logged.Reset()
	expireMaintenanceStamps(t, st.versionDir)
	prunePromptly(t, st)
	if !strings.Contains(logged.String(), "another process holds the cache lock") {
		t.Fatalf("contended prune logged %q, want a skip notice", logged.String())
	}
}

func TestPruneScansEarlyWhenRunInstallsLargeShareOfBudget(t *testing.T) {
	t.Parallel()

	const maxSize = 16 << 10
	st, err := newStore(config{dir: t.TempDir(), maxSize: maxSize})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	// A fresh stamp would normally hold the scan off for pruneInterval, but a
	// run that adds this much of the budget cannot wait without overshooting.
	blobPath := orphanBlob(t, st, strings.Repeat("b", 64))
	body := bytes.Repeat([]byte("cache me"), 256)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{93}, 32),
		OutputID: bytes.Repeat([]byte{94}, 32),
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	// put must be what feeds the gate; without this wiring the escape hatch
	// can never fire in production no matter what the threshold is.
	put := st.installed.Load()
	if put <= 0 {
		t.Fatalf("installed = %d after a put, want the put's compressed size", put)
	}
	st.installed.Add(maxSize/pruneOvershootDivisor + 1 - put)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("orphan blob stat err = %v, want not exist", err)
	}
	if got := st.installed.Load(); got != 0 {
		t.Fatalf("installed = %d after scan, want 0", got)
	}
}

func TestPruneScansWhenStampIsDatedInTheFuture(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	// Clock skew or a restored backup must not lock maintenance out.
	ahead := time.Now().Add(24 * time.Hour)
	if err := os.Chtimes(pruneStampPath(st.versionDir), ahead, ahead); err != nil {
		t.Fatal(err)
	}
	blobPath := orphanBlob(t, st, strings.Repeat("c", 64))
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("orphan blob stat err = %v, want not exist", err)
	}
}

func TestCatalogQueriesRespectCanceledContext(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.q.listOtherRuns(ctx, st.runID); err == nil {
		t.Fatal("listOtherRuns succeeded with canceled context")
	}
	if _, err := st.q.evictionCandidates(ctx, 0, 1); err == nil {
		t.Fatal("evictionCandidates succeeded with canceled context")
	}
	if err := st.q.referencedOutputIDs(ctx, 0, keepEveryOutput, make(map[string]struct{})); err == nil {
		t.Fatal("referencedOutputIDs succeeded with canceled context")
	}
}

// The point of paging outputs_accessed_at is that one step costs a bounded amount
// regardless of how big the cache is. Correctness does not depend on it — an
// unbounded step still evicts the right things — so it needs asserting directly or
// a regression would be invisible.
func TestEvictionStepStaysBounded(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	const outputs = evictionSampleSize * 2
	now := unixMillis(time.Now())
	ctx := context.Background()
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range outputs {
		action := sha256.Sum256(fmt.Appendf(nil, "bounded-action-%d", i))
		output := sha256.Sum256(fmt.Appendf(nil, "bounded-output-%d", i))
		// Distinct access times, so no step has to over-reach to avoid splitting
		// a timestamp.
		accessedAt := now - int64(outputs) + int64(i)
		var outputRef int64
		if err := tx.QueryRowContext(ctx,
			`INSERT INTO outputs(output_id, size, compressed_size, accessed_at) VALUES (?, 1, 1, ?)
			 RETURNING id`,
			output[:], accessedAt,
		).Scan(&outputRef); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO entries(action_id, output, created_at, accessed_at) VALUES (?, ?, ?, ?)`,
			action[:], outputRef, now, accessedAt,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	candidates, err := st.q.evictionCandidates(ctx, 0, evictionSampleSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) == 0 {
		t.Fatal("a step returned no candidates, so it proves nothing here")
	}
	if len(candidates) > evictionSampleSize {
		t.Fatalf("one step returned %d candidates from a %d-output catalog, want at most %d",
			len(candidates), outputs, evictionSampleSize)
	}
}

// Eviction pages outputs_accessed_at, so both of the things asserted here need a
// cache larger than one page: that the walk keeps advancing until the budget is
// met, and that an output read recently outlives outputs that are old by every
// measure, even though an ancient entry still names it. The second is the soft
// reference working — entry age must not drag an output down with it.
func TestPruneEvictsAcrossStepsWithoutTakingFreshOutputs(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	const doomed = evictionSampleSize + 600
	for i := range doomed {
		action := sha256.Sum256(fmt.Appendf(nil, "converge-action-%d", i))
		output := sha256.Sum256(fmt.Appendf(nil, "converge-output-%d", i))
		body := incompressibleBody(t, 64, int64(i))
		if _, err := st.put(request{
			ID:       int64(i),
			Command:  cmdPut,
			ActionID: action[:],
			OutputID: output[:],
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
	}

	// Two action IDs onto one output, big enough that the budget below has room for
	// it and nothing else.
	shared := bytes.Repeat([]byte{111}, 32)
	oldAction := bytes.Repeat([]byte{112}, 32)
	freshAction := bytes.Repeat([]byte{113}, 32)
	sharedBody := incompressibleBody(t, 32<<10, 7)
	for _, action := range [][]byte{oldAction, freshAction} {
		if _, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: action,
			OutputID: shared,
			BodySize: int64(len(sharedBody)),
		}, bufio.NewReader(encodedBody(sharedBody))); err != nil {
			t.Fatal(err)
		}
	}

	// Everything ancient, with distinct times so no single page can swallow the
	// whole cache and defeat the point of the test. The first byte of the ID
	// spreads them without needing a rowid, which a WITHOUT ROWID table lacks.
	ancient := unixMillis(time.Now().Add(-90 * 24 * time.Hour))
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE outputs SET accessed_at = ? + 1 + unicode(hex(substr(output_id, 1, 1)))`, ancient); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE entries SET accessed_at = ?`, ancient); err != nil {
		t.Fatal(err)
	}
	// The shared output was read just now, while the entries naming it stay
	// ancient: eviction must rank it by the output's own access time.
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`,
		unixMillis(time.Now()), idKey(hexOf(shared))); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}

	var sharedSize int64
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT compressed_size FROM outputs WHERE output_id = ?`,
		idKey(hexOf(shared))).Scan(&sharedSize); err != nil {
		t.Fatal(err)
	}
	total, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	// Room for the shared output alone. Reaching that means evicting more of the
	// ancient outputs than one page examines, so the walk has to take several.
	st.maxSize = sharedSize * 3 / 2
	if total-st.maxSize <= 0 {
		t.Fatal("cache is not over budget: nothing would be evicted")
	}

	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	after, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	if after > st.maxSize {
		t.Fatalf("cache is %d bytes after eviction, over its %d budget", after, st.maxSize)
	}
	if _, err := os.Stat(st.blobPath(hexOf(shared))); err != nil {
		t.Fatalf("evicted the output that was read most recently: %v", err)
	}
}

func TestPruneUsesBlobLRU(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	sharedOutputID := bytes.Repeat([]byte{11}, 32)
	oldSharedActionID := bytes.Repeat([]byte{12}, 32)
	newSharedActionID := bytes.Repeat([]byte{13}, 32)
	prunedOutputID := bytes.Repeat([]byte{14}, 32)
	prunedActionID := bytes.Repeat([]byte{15}, 32)
	body := bytes.Repeat([]byte("x"), 64)

	for _, tc := range []struct {
		actionID []byte
		outputID []byte
	}{
		{oldSharedActionID, sharedOutputID},
		{newSharedActionID, sharedOutputID},
		{prunedActionID, prunedOutputID},
	} {
		if _, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: tc.actionID,
			OutputID: tc.outputID,
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
	}

	// The shared output was read more recently than the other one, so the other is
	// what eviction should take. Its two actions were read at different times; that
	// no longer decides anything, because the output carries its own clock.
	recent := unixMillis(time.Now())
	for _, tc := range []struct {
		outputID   []byte
		accessedAt int64
	}{
		{sharedOutputID, recent - 1000},
		{prunedOutputID, recent - 2000},
	} {
		if _, err := st.db.ExecContext(context.Background(),
			`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`,
			tc.accessedAt, idKey(hexOf(tc.outputID)),
		); err != nil {
			t.Fatal(err)
		}
	}

	total, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	prunedInfo, err := os.Stat(st.blobPath(hexOf(prunedOutputID)))
	if err != nil {
		t.Fatal(err)
	}
	st.maxSize = total - prunedInfo.Size()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(st.blobPath(hexOf(prunedOutputID))); !os.IsNotExist(err) {
		t.Fatalf("middle-aged blob was not pruned: %v", err)
	}
	if _, err := st.lookupEntry(hexOf(oldSharedActionID)); err != nil {
		t.Fatalf("shared output old action was pruned: %v", err)
	}
	if _, err := st.lookupEntry(hexOf(newSharedActionID)); err != nil {
		t.Fatalf("shared output new action was pruned: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(sharedOutputID))); err != nil {
		t.Fatalf("shared output blob was pruned: %v", err)
	}
}

func TestPruneRemovesEntriesOlderThanTrimLimit(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
		maxAge:  defaultMaxAge,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	oldActionID := bytes.Repeat([]byte{20}, 32)
	oldOutputID := bytes.Repeat([]byte{21}, 32)
	freshActionID := bytes.Repeat([]byte{22}, 32)
	freshOutputID := bytes.Repeat([]byte{23}, 32)
	sharedOutputID := bytes.Repeat([]byte{24}, 32)
	oldSharedActionID := bytes.Repeat([]byte{25}, 32)
	freshSharedActionID := bytes.Repeat([]byte{26}, 32)
	body := bytes.Repeat([]byte("x"), 64)

	for _, tc := range []struct {
		actionID []byte
		outputID []byte
	}{
		{oldActionID, oldOutputID},
		{freshActionID, freshOutputID},
		{oldSharedActionID, sharedOutputID},
		{freshSharedActionID, sharedOutputID},
	} {
		if _, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: tc.actionID,
			OutputID: tc.outputID,
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
	}

	stale := unixMillis(trimCutoff(defaultMaxAge, time.Now())) - int64(time.Minute/time.Millisecond)
	for _, actionID := range [][]byte{oldActionID, oldSharedActionID} {
		if _, err := st.db.ExecContext(
			context.Background(),
			`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
			stale,
			idKey(hexOf(actionID)),
		); err != nil {
			t.Fatal(err)
		}
	}
	// oldOutputID has no other action, so it went unread with its entry. The shared
	// output stays fresh: its other action was read, which is what keeps the blob
	// alive while the stale action's entry goes.
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`,
		stale, idKey(hexOf(oldOutputID))); err != nil {
		t.Fatal(err)
	}

	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := st.lookupEntry(hexOf(oldActionID)); !errorsIs(err, sql.ErrNoRows) {
		t.Fatalf("stale entry was not pruned: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(oldOutputID))); !os.IsNotExist(err) {
		t.Fatalf("stale entry blob stat err = %v, want not exist", err)
	}
	if _, err := st.lookupEntry(hexOf(freshActionID)); err != nil {
		t.Fatalf("fresh entry was pruned: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(freshOutputID))); err != nil {
		t.Fatalf("fresh entry blob was pruned: %v", err)
	}
	if _, err := st.lookupEntry(hexOf(oldSharedActionID)); !errorsIs(err, sql.ErrNoRows) {
		t.Fatalf("stale shared action was not pruned: %v", err)
	}
	if _, err := st.lookupEntry(hexOf(freshSharedActionID)); err != nil {
		t.Fatalf("fresh shared action was pruned: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(sharedOutputID))); err != nil {
		t.Fatalf("shared output blob was pruned while still referenced: %v", err)
	}
}

func TestPruneKeepsOldEntriesWhenMaxAgeDisabled(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
		maxAge:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{40}, 32)
	outputID := bytes.Repeat([]byte{41}, 32)
	body := bytes.Repeat([]byte("x"), 64)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}

	if _, err := st.db.ExecContext(
		context.Background(),
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		int64(1000),
		idKey(hexOf(actionID)),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := st.lookupEntry(hexOf(actionID)); err != nil {
		t.Fatalf("entry pruned with age-based pruning disabled: %v", err)
	}
}

func TestAccessTimesFlushOnClose(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{30}, 32)
	outputID := bytes.Repeat([]byte{31}, 32)
	body := []byte("access body")
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	actionHex, outputHex := hexOf(actionID), hexOf(outputID)
	for _, backdate := range []struct{ query, id string }{
		{`UPDATE entries SET accessed_at = ? WHERE action_id = ?`, actionHex},
		{`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`, outputHex},
	} {
		if _, err := st.db.ExecContext(
			context.Background(), backdate.query, int64(1000), idKey(backdate.id),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID}); err != nil {
		t.Fatal(err)
	}
	if err := st.flushAccessTimes(); err != nil {
		t.Fatal(err)
	}
	// Both clocks, because they answer different questions: the entry's decides
	// when the mapping expires, the output's decides what eviction takes. An
	// output that is read constantly but never restamped is exactly what eviction
	// would pick first.
	for _, check := range []struct{ what, query, id string }{
		{"entry", `SELECT accessed_at FROM entries WHERE action_id = ?`, actionHex},
		{"output", `SELECT accessed_at FROM outputs WHERE output_id = ?`, outputHex},
	} {
		var accessedAt int64
		if err := st.db.QueryRowContext(
			context.Background(), check.query, idKey(check.id),
		).Scan(&accessedAt); err != nil {
			t.Fatal(err)
		}
		if accessedAt <= 1000 {
			t.Fatalf("%s accessed_at = %d, want > 1000", check.what, accessedAt)
		}
	}
}

// Hits were only persisted on close, so a running build's reads did not exist as
// far as the catalog was concerned — and a killed helper lost them entirely, which
// on a host that kills helpers makes the entries most in use look like the coldest
// in the cache. Eviction ranks on exactly this column.
func TestGetPersistsAccessTimesWithoutClosing(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{91}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: bytes.Repeat([]byte{92}, 32),
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	actionHex := hexOf(actionID)
	storedAccess := func() int64 {
		t.Helper()
		var at int64
		if err := st.db.QueryRowContext(context.Background(),
			`SELECT accessed_at FROM entries WHERE action_id = ?`, idKey(actionHex)).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}

	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`, int64(1000), idKey(actionHex)); err != nil {
		t.Fatal(err)
	}

	// Inside the interval the hit stays buffered: one transaction per get would
	// put the catalog's single writer on the read path.
	if _, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID}); err != nil {
		t.Fatal(err)
	}
	if got := storedAccess(); got != 1000 {
		t.Errorf("accessed_at = %d after a hit inside the interval, want it still buffered at 1000", got)
	}

	// Stand in for accessFlushInterval elapsing.
	st.mu.Lock()
	st.accessFlushed = time.Now().Add(-2 * accessFlushInterval)
	st.mu.Unlock()

	if _, err := st.get(request{ID: 3, Command: cmdGet, ActionID: actionID}); err != nil {
		t.Fatal(err)
	}
	if got := storedAccess(); got <= 1000 {
		t.Errorf("accessed_at = %d, want the hit persisted without a close", got)
	}
	// And the buffer is handed over, not copied.
	st.mu.Lock()
	buffered := len(st.accessed)
	st.mu.Unlock()
	if buffered != 0 {
		t.Errorf("%d access times still buffered after a flush, want 0", buffered)
	}

	// The flush must restart the interval. Without that, the first one to come due
	// leaves every later get flushing too, which is the per-get transaction the
	// buffer exists to avoid.
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`, int64(2000), idKey(actionHex)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.get(request{ID: 4, Command: cmdGet, ActionID: actionID}); err != nil {
		t.Fatal(err)
	}
	if got := storedAccess(); got != 2000 {
		t.Errorf("accessed_at = %d after the flush, want 2000 — the interval did not restart", got)
	}
}

func TestRunAnnouncesCommandsBeforeOpeningStore(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	if err := os.MkdirAll(testVersionDir(cacheDir), 0o777); err != nil {
		t.Fatal(err)
	}
	lock := flock.New(filepath.Join(testVersionDir(cacheDir), "lifecycle.lock"))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{ID: 1, Command: cmdClose})
	stdoutR, stdoutW := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := run([]string{"-dir", cacheDir, "-max-size", "0"}, &stdin, stdoutW)
		_ = stdoutW.CloseWithError(err)
		done <- err
	}()

	// The handshake must not wait on the store, whose lock is held here. Keep
	// draining afterwards: the pipe is unbuffered, so run blocks on its next
	// write until someone reads.
	hello := make(chan response, 1)
	go func() {
		dec := json.NewDecoder(stdoutR)
		var res response
		if err := dec.Decode(&res); err != nil {
			return
		}
		hello <- res
		_, _ = io.Copy(io.Discard, stdoutR)
	}()
	select {
	case res := <-hello:
		if len(res.KnownCommands) != 3 {
			t.Errorf("KnownCommands = %v, want 3", res.KnownCommands)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no handshake while the lifecycle lock was held")
	}

	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunProtocol(t *testing.T) {
	t.Parallel()

	actionID := bytes.Repeat([]byte{3}, 32)
	outputID := bytes.Repeat([]byte{4}, 32)
	body := []byte("protocol body")

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	})
	stdin.WriteByte('\n')
	writeJSON(t, &stdin, base64.StdEncoding.EncodeToString(body))
	writeJSON(t, &stdin, request{
		ID:       2,
		Command:  cmdGet,
		ActionID: actionID,
	})
	writeJSON(t, &stdin, request{ID: 3, Command: cmdClose})

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	if len(hello.KnownCommands) != 3 {
		t.Fatalf("KnownCommands = %v", hello.KnownCommands)
	}
	var putRes response
	if err := dec.Decode(&putRes); err != nil {
		t.Fatal(err)
	}
	if putRes.ID != 1 || putRes.Err != "" || putRes.DiskPath == "" {
		t.Fatalf("put response = %+v", putRes)
	}
	var getRes response
	if err := dec.Decode(&getRes); err != nil {
		t.Fatal(err)
	}
	if getRes.ID != 2 || getRes.Err != "" || getRes.Miss || !bytes.Equal(getRes.OutputID, outputID) {
		t.Fatalf("get response = %+v", getRes)
	}
	var closeRes response
	if err := dec.Decode(&closeRes); err != nil {
		t.Fatal(err)
	}
	if closeRes.ID != 3 || closeRes.Err != "" {
		t.Fatalf("close response = %+v", closeRes)
	}
}

func TestRunProtocolHandlesFinalRequestWithoutNewline(t *testing.T) {
	t.Parallel()

	var stdin bytes.Buffer
	stdin.WriteString(`{"ID":1,"Command":"close"}`)

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	var closeRes response
	if err := dec.Decode(&closeRes); err != nil {
		t.Fatal(err)
	}
	if closeRes.ID != 1 || closeRes.Err != "" {
		t.Fatalf("close response = %+v", closeRes)
	}
}

func TestRunReturnsAfterEOF(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	var hello response
	if err := json.NewDecoder(&stdout).Decode(&hello); err != nil {
		t.Fatal(err)
	}
	if len(hello.KnownCommands) != 3 {
		t.Fatalf("KnownCommands = %v", hello.KnownCommands)
	}
}

func TestRunRejectsBadProfilePath(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{
		"-dir", t.TempDir(),
		"-cpuprofile", t.TempDir(),
	}, strings.NewReader(""), &stdout)
	if err == nil {
		t.Fatal("run accepted directory CPU profile path")
	}
}

func TestRunRejectsBadArgsAndCacheDir(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"-bad"}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted bad args")
	}
	if err := run([]string{"wat"}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted unexpected argument")
	}
	if err := run([]string{"-dir", t.TempDir(), "status"}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted flags before subcommand")
	}

	cacheFile := filepath.Join(t.TempDir(), "cache-file")
	if err := os.WriteFile(cacheFile, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := run([]string{"-dir", cacheFile}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted file cache dir")
	}
}

func TestRunHelp(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "root", args: []string{"-h"}, want: "Usage:\n  gocachez [flags]\n"},
		{name: "clean", args: []string{"clean", "-h"}, want: "Usage:\n  gocachez clean [flags]\n"},
		{name: "status", args: []string{"status", "-dir", t.TempDir(), "-h"}, want: "Usage:\n  gocachez status [flags]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			if err := run(tc.args, strings.NewReader(""), &stdout); err != nil {
				t.Fatal(err)
			}
			assertContains(t, stdout.String(), tc.want)
			assertContains(t, stdout.String(), "-h")
		})
	}
}

func TestRunHelpDoesNotCreateCacheState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	if err := run([]string{"clean", "-h", "-dir", cacheDir}, strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(testVersionDir(cacheDir)); !os.IsNotExist(err) {
		t.Fatalf("help created cache state: %v", err)
	}
}

// The command exists to take maintenance off a build's exit path, so its whole
// job is to run when the interval says no. It also proves the command does not
// register a run of its own: countRuns counts every row including the caller's,
// so a self-registering prune would find the cache busy and delete nothing.
func TestRunPruneDeletesInsideTheInterval(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	outputID := bytes.Repeat([]byte{47}, 32)
	st, err := newStore(config{dir: cacheDir, maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{48}, 32),
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	stale := unixMillis(trimCutoff(defaultMaxAge, time.Now())) - int64(time.Minute/time.Millisecond)
	for _, table := range []string{"entries", "outputs"} {
		if _, err := st.db.ExecContext(context.Background(),
			`UPDATE `+table+` SET accessed_at = ?`, stale); err != nil {
			t.Fatal(err)
		}
	}
	blobPath := st.blobPath(hexOf(outputID))
	// close both stamps the interval and leaves the cache idle, so nothing but an
	// explicit request can prune here.
	st.close()
	if _, err := os.Stat(pruneStampPath(st.versionDir)); err != nil {
		t.Fatalf("close did not stamp, so the interval is not what blocks: %v", err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"prune", "-dir", cacheDir, "-max-age", "5d"}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("prune command did not delete the stale blob: stat err = %v, want not exist", err)
	}
	if stdout.Len() != 0 {
		t.Errorf("prune stdout = %q, want empty on an idle cache", stdout.String())
	}
}

// A directory left by a killed helper holds full uncompressed artifacts, because
// nothing stripped them, and no tool holds a path into a build that died. Waiting
// for it to age out is what let live/ grow to several times the size of the cache
// it belongs to, so prune must reclaim it on its runs row, not on its age — and
// while other builds are running, which on a busy machine is always.
func TestRunPruneReclaimsAKilledRunYoungerThanTheCutoff(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	cfg := config{dir: cacheDir, maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	busy, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.close()

	// Stand in for a helper killed mid-build: its row survives, its directory
	// still holds what it had materialised, and its lock is free.
	killed := filepath.Join(busy.liveRoot, "run-killed")
	if err := os.MkdirAll(killed, 0o777); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(killed, "aa11-unstripped")
	if err := os.WriteFile(artifact, bytes.Repeat([]byte("artifact"), 512), 0o666); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(killed, "run.lock"), nil, 0o666); err != nil {
		t.Fatal(err)
	}
	if err := busy.q.registerRun(context.Background(), "run-killed", killed,
		filepath.Join(killed, "run.lock"), unixMillis(time.Now())); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"prune", "-dir", cacheDir, "-max-retained-age", "1h"}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(killed); !os.IsNotExist(err) {
		t.Errorf("prune left a killed run to age out: stat err = %v, want not exist", err)
	}
	if got := countRunRows(t, busy.db, "run-killed"); got != 0 {
		t.Errorf("killed run rows = %d, want 0", got)
	}
	// The live build must be untouched, and still counted.
	if _, err := os.Stat(busy.runDir); err != nil {
		t.Errorf("prune removed a running build's live dir: %v", err)
	}
	if !strings.Contains(stdout.String(), "1 build(s)") {
		t.Errorf("prune stdout = %q, want the live build counted and the dead row not", stdout.String())
	}
}

// Explicitly asking cannot waive the rule that makes deletion safe. Reporting
// nothing here would look like success, which is the trap for a CI step.
func TestRunPruneReportsBuildsStillUsingTheCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	cfg := config{dir: cacheDir, maxAge: defaultMaxAge, maxRetainedAge: time.Hour}
	paths := retainedCache(t, cfg, bytes.Repeat([]byte{49}, 32))
	old := trimCutoff(time.Hour, time.Now()).Add(-time.Minute)
	for _, path := range []string{paths.export, filepath.Join(paths.runDir, "run.lock")} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	busy, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.close()

	var stdout bytes.Buffer
	if err := run([]string{"prune", "-dir", cacheDir, "-max-retained-age", "1h"}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "still using the cache") {
		t.Errorf("prune stdout = %q, want a note that the cache is in use", stdout.String())
	}
	// The live run is guarded by its own lock, so it still goes; the retained
	// file, equally expired, has to wait for an idle cache.
	if _, err := os.Stat(paths.runDir); !os.IsNotExist(err) {
		t.Errorf("expired live run survived: stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(paths.export); err != nil {
		t.Errorf("retained file deleted while a build was registered: %v", err)
	}
}

func TestRunPruneOnACacheThatWasNeverUsed(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"prune", "-dir", filepath.Join(t.TempDir(), "absent")}, strings.NewReader(""), &stdout); err != nil {
		t.Fatalf("prune on an unused cache: %v", err)
	}
	if stdout.Len() != 0 {
		t.Errorf("prune stdout = %q, want empty", stdout.String())
	}
}

func TestRunCleanRemovesInactiveState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{43}, 32)
	outputID := bytes.Repeat([]byte{44}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	dbPath := filepath.Join(st.versionDir, "cache.db")
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"clean", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("clean stdout = %q, want empty", stdout.String())
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("blob stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(res.DiskPath); !os.IsNotExist(err) {
		t.Fatalf("live file stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("catalog stat err = %v, want not exist", err)
	}
}

func TestRunCleanKeepsActiveState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{45}, 32)
	outputID := bytes.Repeat([]byte{46}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))

	if err := run([]string{"clean", "-dir", cacheDir}, strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("active blob was removed: %v", err)
	}
	if _, err := os.Stat(res.DiskPath); err != nil {
		t.Fatalf("active live file was removed: %v", err)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); err != nil {
		t.Fatalf("active entry was removed: %v", err)
	}
}

func TestRunCleanRemovesAbandonedState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{47}, 32)
	outputID := bytes.Repeat([]byte{48}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	runDir := st.runDir
	abandonStore(t, st)

	if err := run([]string{"clean", "-dir", cacheDir}, strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("abandoned run dir stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(res.DiskPath); !os.IsNotExist(err) {
		t.Fatalf("abandoned live file stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("abandoned blob stat err = %v, want not exist", err)
	}
}

func TestRunStatusEmptyCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	var stdout bytes.Buffer
	if err := run([]string{"status", "-types", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Configuration:\n")
	assertContains(t, got, "Cache directory")
	assertContains(t, got, cacheDir)
	assertContains(t, got, "Max size")
	assertContains(t, got, "20.0GiB")
	assertContains(t, got, "Max age            5d")
	assertContains(t, got, "Verbose")
	assertContains(t, got, "false")
	assertContains(t, got, "Summary:\n")
	assertContains(t, got, "State               missing")
	assertContains(t, got, "Cached actions      0")
	assertContains(t, got, "Cached outputs      0")
	assertContains(t, got, "Oldest cached blob  n/a")
	assertContains(t, got, "Live runs           0 active, 0 inactive")
	assertContains(t, got, "Storage:\n")
	assertContains(t, got, "Original output size    0B")
	assertContains(t, got, "Compressed cache blobs  0B (0 files)")
	assertContains(t, got, "Blob max usage          0B / 20.0GiB (0.0%, 20.0GiB remaining)")
	assertContains(t, got, "Retained go-list files  0B (0 files)")
	assertContains(t, got, "Total stored            0B")
	assertContains(t, got, "Blob-only savings       0B (0.0%)")
	assertContains(t, got, "Overall savings         0B (0.0%)")
	assertContains(t, got, "Compressed blob contents:\n")
	assertContains(t, got, "None      0        0B      0B  0B (0.0%)")
	assertContains(t, got, "Retained go-list files:\n")
}

func TestRunStatusShowsEffectiveConfig(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir, "-max-size", "1MiB", "-max-age", "2d", "-v"}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Cache directory")
	assertContains(t, got, cacheDir)
	assertContains(t, got, "Max size           1.0MiB")
	assertContains(t, got, "Max age            2d")
	assertContains(t, got, "Verbose            true")
}

func TestRunStatusInactiveCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{49}, 32)
	outputID := bytes.Repeat([]byte{50}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"status", "-types", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "State               present")
	assertContains(t, got, "Cached actions      1")
	assertContains(t, got, "Cached outputs      1")
	assertContains(t, got, "Oldest cached blob  <1m")
	assertContains(t, got, "Live runs           0 active, 0 inactive")
	assertContains(t, got, "Original output size")
	assertContains(t, got, "Compressed cache blobs")
	assertContains(t, got, "Blob max usage")
	assertContains(t, got, "Retained go-list files")
	assertContains(t, got, "Total stored")
	assertContains(t, got, "Blob-only savings")
	assertContains(t, got, "Overall savings")
	assertContains(t, got, "Compressed blob contents:")
	assertContains(t, got, "Text files      1        4B")
}

func TestRunStatusDoesNotWaitForLifecycleLock(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{81}, 32),
		OutputID: bytes.Repeat([]byte{82}, 32),
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	st.close()

	// Stand in for a build holding the lock across a maintenance scan.
	lock := flock.New(filepath.Join(testVersionDir(cacheDir), "lifecycle.lock"))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	done := make(chan error, 1)
	var stdout bytes.Buffer
	go func() {
		done <- run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("status blocked on the lifecycle lock")
	}
	assertContains(t, stdout.String(), "Cached outputs      1")
}

func TestStatusSkipsRunDirWithoutLockFile(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	st.close()

	// A prune removing a run dir unlinks run.lock before the dir itself, so
	// status can see the dir in that state. It must neither fail nor recreate
	// the lock: doing so would make the prune's rmdir fail and resurrect the
	// run in the report.
	runDir := filepath.Join(testVersionDir(cacheDir), "live", "run-halfremoved")
	if err := os.MkdirAll(runDir, 0o777); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	assertContains(t, stdout.String(), "Live runs           0 active, 0 inactive")
	if _, err := os.Stat(filepath.Join(runDir, "run.lock")); !os.IsNotExist(err) {
		t.Fatalf("status created run.lock: stat err = %v, want not exist", err)
	}
}

func TestStatusToleratesRetainedFilesRemovedDuringWalk(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	body := goArchive(goPkgdef([]byte("uFAKE")), bytes.Repeat([]byte("object data"), 16))
	retained := make([]string, 0, 400)
	for i := range 400 {
		action := sha256.Sum256(fmt.Appendf(nil, "walk-action-%d", i))
		output := sha256.Sum256(fmt.Appendf(nil, "walk-output-%d", i))
		if _, err := st.put(request{
			ID:       int64(i),
			Command:  cmdPut,
			ActionID: action[:],
			OutputID: output[:],
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
		retained = append(retained, retainedPath(cacheDir, output[:], ".a"))
	}
	st.close()

	// Delete retained files while the report walks them, which is exactly what
	// a concurrent prune does now that status holds no lock. Split by shard,
	// not by file: taking a whole shard directory is a failing descent, while
	// unlinking within a surviving shard is a failing stat, and those are
	// different branches of the walk. Splitting by file would let the removed
	// directories hide the files the stat branch needs.
	byShard := map[string][]string{}
	for _, path := range retained {
		dir := filepath.Dir(path)
		byShard[dir] = append(byShard[dir], path)
	}
	shards := slices.Sorted(maps.Keys(byShard))

	deleted := make(chan struct{})
	go func() {
		defer close(deleted)
		for i, shard := range shards {
			if i%2 == 0 {
				_ = os.RemoveAll(shard)
				continue
			}
			for _, path := range byShard[shard] {
				_ = os.Remove(path)
			}
		}
	}()

	var stdout bytes.Buffer
	err = run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout)
	<-deleted
	if err != nil {
		t.Fatalf("status failed while retained files were being removed: %v", err)
	}
	assertContains(t, stdout.String(), "Cached outputs      400")
}

func TestRunStatusActiveCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{51}, 32)
	outputID := bytes.Repeat([]byte{52}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"status", "-types", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "State               present")
	assertContains(t, got, "Cached actions      1")
	assertContains(t, got, "Cached outputs      1")
	assertContains(t, got, "Live runs           1 active, 0 inactive")
	assertContains(t, got, "Compressed blob contents:")
	assertContains(t, got, "Text files      1        4B")
}

func TestRunStatusShowsBlobTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	exportData := []byte("uFAKE")
	archive := goArchive(goPkgdef(exportData), bytes.Repeat([]byte("object data"), 16))
	compilerID := append([]byte("/usr/lib/ccache/bin/gcc\x00stat 1448536 1ed 2026-05-13 07:28:29 -0700 PDT false\n\x00"),
		bytes.Repeat([]byte{0x86}, 32)...)
	entries := []struct {
		actionID byte
		outputID byte
		body     []byte
	}{
		{63, 64, archive},
		{65, 66, []byte("// Code generated by cmd/cgo; DO NOT EDIT.\n\npackage net\n\nconst x = 1\n")},
		{67, 68, []byte{0x7f, 'E', 'L', 'F', 1, 2, 3}},
		{71, 72, []byte("go index v2\n\x00\x01\x02")},
		{73, 74, compilerID},
	}
	for _, entry := range entries {
		if _, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: bytes.Repeat([]byte{entry.actionID}, 32),
			OutputID: bytes.Repeat([]byte{entry.outputID}, 32),
			BodySize: int64(len(entry.body)),
		}, bufio.NewReader(encodedBody(entry.body))); err != nil {
			t.Fatal(err)
		}
	}
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"status", "-types", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Compressed blob contents:")
	assertContains(t, got, "Go package archives        1")
	assertContains(t, got, "Go package indexes         1")
	assertContains(t, got, "Generated cgo sources")
	assertContains(t, got, "ELF binaries")
	assertContains(t, got, "C compiler IDs")
}

func TestRunStatusShowsRetainedFileTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	exportData := []byte("uFAKE")
	entries := []struct {
		actionID byte
		outputID byte
		body     []byte
	}{
		{77, 78, goArchive(goPkgdef(exportData), bytes.Repeat([]byte("object data"), 16))},
		{79, 80, []byte("// Code generated by cmd/cgo; DO NOT EDIT.\n\npackage net\n\nconst x = 1\n")},
		{81, 82, []byte("\n// Code generated by 'go test'. DO NOT EDIT.\n\npackage main\n\nfunc main() {}\n")},
	}
	for _, entry := range entries {
		if _, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: bytes.Repeat([]byte{entry.actionID}, 32),
			OutputID: bytes.Repeat([]byte{entry.outputID}, 32),
			BodySize: int64(len(entry.body)),
		}, bufio.NewReader(encodedBody(entry.body))); err != nil {
			t.Fatal(err)
		}
	}
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"status", "-types", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Retained go-list files")
	assertContains(t, got, "Export archives")
	assertContains(t, got, "Generated cgo sources")
	assertContains(t, got, "Generated test mains")
}

func TestRunStatusCountsUnreadableBlobTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{69}, 32)
	outputID := bytes.Repeat([]byte{70}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	st.close()
	if err := os.WriteFile(blobPath, []byte("not zstd"), 0o666); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"status", "-types", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Compressed blob contents:")
	assertContains(t, got, "Unreadable blobs      1        4B")
}

func TestStatusCachesBlobTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	archive := goArchive(goPkgdef([]byte("uFAKE")), bytes.Repeat([]byte("object data"), 64))
	outputID := bytes.Repeat([]byte{90}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{89}, 32),
		OutputID: outputID,
		BodySize: int64(len(archive)),
	}, bufio.NewReader(encodedBody(archive))); err != nil {
		t.Fatal(err)
	}
	st.close()

	versionDir, blobsDir, _, _ := cachePaths(config{dir: cacheDir})
	dbPath := filepath.Join(versionDir, "cache.db")

	statuses, err := readBlobTypeStatus(dbPath, blobsDir)
	if err != nil {
		t.Fatal(err)
	}
	assertBlobKind(t, statuses, blobTypeGoPackageArchive, 1)

	// Removing the blob forces the second pass to rely on the cached
	// classification; if it decompressed it would report the blob unreadable.
	if err := os.Remove(blobPath(blobsDir, hexOf(outputID))); err != nil {
		t.Fatal(err)
	}
	statuses, err = readBlobTypeStatus(dbPath, blobsDir)
	if err != nil {
		t.Fatal(err)
	}
	assertBlobKind(t, statuses, blobTypeGoPackageArchive, 1)
}

func TestStatusCachesClassificationsPastOneBatch(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	// One past a full batch, so the trailing partial batch has to commit too.
	const outputs = classificationBatch + 1
	body := []byte("package main\n")
	for i := range outputs {
		action := sha256.Sum256(fmt.Appendf(nil, "batch-action-%d", i))
		output := sha256.Sum256(fmt.Appendf(nil, "batch-output-%d", i))
		if _, err := st.put(request{
			ID:       int64(i),
			Command:  cmdPut,
			ActionID: action[:],
			OutputID: output[:],
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
	}
	st.close()

	versionDir, blobsDir, _, _ := cachePaths(config{dir: cacheDir})
	dbPath := filepath.Join(versionDir, "cache.db")
	if _, err := readBlobTypeStatus(dbPath, blobsDir); err != nil {
		t.Fatal(err)
	}

	// With every blob gone, only cached classifications can still name them;
	// a dropped trailing batch would surface as unreadable blobs.
	if err := os.RemoveAll(blobsDir); err != nil {
		t.Fatal(err)
	}
	statuses, err := readBlobTypeStatus(dbPath, blobsDir)
	if err != nil {
		t.Fatal(err)
	}
	assertBlobKind(t, statuses, blobTypeGoSource, outputs)
}

// Not parallel: it swaps the global log sink to observe the warning.
func TestPersistClassificationsReportsFailure(t *testing.T) {
	// A persist that cannot open the catalog has to say so. Discarding this
	// error is what let a cache sit permanently in the reclassify-everything
	// path with no way to tell.
	dbPath := filepath.Join(t.TempDir(), "missing-dir", "cache.db")
	classified := map[string]blobTypeKind{strings.Repeat("a", 64): blobTypeGoSource}

	if err := writeClassifications(dbPath, blobClassifier, classified); err == nil {
		t.Fatal("writeClassifications succeeded against an unwritable catalog path")
	}

	var logged bytes.Buffer
	flags := log.Flags()
	log.SetOutput(&logged)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(flags)
	})

	persistClassifications(dbPath, blobClassifier, classified, true)
	if !strings.Contains(logged.String(), "caching classifications failed") {
		t.Fatalf("verbose persist logged %q, want a failure warning", logged.String())
	}

	logged.Reset()
	persistClassifications(dbPath, blobClassifier, classified, false)
	if logged.Len() != 0 {
		t.Fatalf("non-verbose persist logged %q, want silence", logged.String())
	}
}

func TestStatusReclassifiesWhenClassifierVersionChanges(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	archive := goArchive(goPkgdef([]byte("uFAKE")), bytes.Repeat([]byte("object data"), 64))
	outputID := bytes.Repeat([]byte{92}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{91}, 32),
		OutputID: outputID,
		BodySize: int64(len(archive)),
	}, bufio.NewReader(encodedBody(archive))); err != nil {
		t.Fatal(err)
	}
	st.close()

	versionDir, blobsDir, _, _ := cachePaths(config{dir: cacheDir})
	dbPath := filepath.Join(versionDir, "cache.db")

	// Seed a wrong classification recorded under a different classifier version.
	execCatalog(t, dbPath, `UPDATE outputs SET blob_type = ?, blob_type_version = ?`,
		int64(blobTypeText), int64(blobClassifierVersion+1))

	statuses, err := readBlobTypeStatus(dbPath, blobsDir)
	if err != nil {
		t.Fatal(err)
	}
	assertBlobKind(t, statuses, blobTypeGoPackageArchive, 1)

	// The stale value is recomputed and re-stored at the current version.
	var kind, version sql.NullInt64
	queryCatalog(t, dbPath, `SELECT blob_type, blob_type_version FROM outputs WHERE output_id = ?`,
		[]any{idKey(hexOf(outputID))}, &kind, &version)
	if !kind.Valid || blobTypeKind(kind.Int64) != blobTypeGoPackageArchive {
		t.Fatalf("cached blob_type = %v, want %d", kind, blobTypeGoPackageArchive)
	}
	if !version.Valid || version.Int64 != int64(blobClassifierVersion) {
		t.Fatalf("cached blob_type_version = %v, want %d", version, blobClassifierVersion)
	}
}

func TestStatusCachesRetainedTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("// Code generated by cmd/cgo; DO NOT EDIT.\n\npackage net\n\nconst x = 1\n")
	outputID := bytes.Repeat([]byte{94}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{93}, 32),
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	st.close()

	versionDir := cacheVersionDir(config{dir: cacheDir})
	dbPath := filepath.Join(versionDir, "cache.db")
	outputHex := hexOf(outputID)

	// New retained files are classified when they are created.
	var kind, version sql.NullInt64
	queryCatalog(t, dbPath, `SELECT retained_type, retained_type_version FROM outputs WHERE output_id = ?`,
		[]any{idKey(outputHex)}, &kind, &version)
	if !kind.Valid || retainedTypeKind(kind.Int64) != retainedTypeGeneratedCgoSource {
		t.Fatalf("cached retained_type = %v, want %d", kind, retainedTypeGeneratedCgoSource)
	}
	if !version.Valid || version.Int64 != int64(retainedClassifierVersion) {
		t.Fatalf("cached retained_type_version = %v, want %d", version, retainedClassifierVersion)
	}

	// Simulate an older cache and verify status backfills its classification.
	execCatalog(t, dbPath, `UPDATE outputs SET retained_type = NULL, retained_type_version = NULL WHERE output_id = ?`, idKey(outputHex))
	_, _, outputs, err := readCatalogStatus(dbPath, true)
	if err != nil {
		t.Fatal(err)
	}
	_, _, statuses, err := readRetainedStatus(dbPath, retainedRoot(versionDir), outputs, true, false)
	if err != nil {
		t.Fatal(err)
	}
	assertRetainedKind(t, statuses, retainedTypeGeneratedCgoSource, 1)

	queryCatalog(t, dbPath, `SELECT retained_type, retained_type_version FROM outputs WHERE output_id = ?`,
		[]any{idKey(outputHex)}, &kind, &version)
	if !kind.Valid || retainedTypeKind(kind.Int64) != retainedTypeGeneratedCgoSource {
		t.Fatalf("backfilled retained_type = %v, want %d", kind, retainedTypeGeneratedCgoSource)
	}
	if !version.Valid || version.Int64 != int64(retainedClassifierVersion) {
		t.Fatalf("backfilled retained_type_version = %v, want %d", version, retainedClassifierVersion)
	}

	// Changing the file contents proves the next pass uses the cached value.
	retainedPath := filepath.Join(retainedRoot(versionDir), outputHex[:2], outputHex+".go")
	if err := os.WriteFile(retainedPath, []byte("// Code generated by 'go test'. DO NOT EDIT.\n\npackage main\n"), 0o666); err != nil {
		t.Fatal(err)
	}
	_, _, outputs, err = readCatalogStatus(dbPath, true)
	if err != nil {
		t.Fatal(err)
	}
	_, _, statuses, err = readRetainedStatus(dbPath, retainedRoot(versionDir), outputs, true, false)
	if err != nil {
		t.Fatal(err)
	}
	assertRetainedKind(t, statuses, retainedTypeGeneratedCgoSource, 1)
}

func TestPutKeepsOutputClassification(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	ctx := context.Background()
	output1 := hexOf(bytes.Repeat([]byte{2}, 32))
	output2 := hexOf(bytes.Repeat([]byte{3}, 32))
	now := time.Now()
	put := func(actionID, outputID string) {
		if err := st.q.upsertEntry(ctx, entry{
			ActionID: actionID, OutputID: outputID,
			Size: 1, CompressedSize: 1, CreatedAt: now, AccessedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	kindOf := func(outputID string) (sql.NullInt64, sql.NullInt64) {
		var blobType, retainedType sql.NullInt64
		if err := st.db.QueryRowContext(ctx,
			`SELECT blob_type, retained_type FROM outputs WHERE output_id = ?`,
			idKey(outputID)).Scan(&blobType, &retainedType); err != nil {
			t.Fatal(err)
		}
		return blobType, retainedType
	}

	action := hexOf(bytes.Repeat([]byte{1}, 32))
	put(action, output1)
	if err := st.q.updateBlobType(ctx, output1, blobTypeGoPackageArchive, blobClassifierVersion); err != nil {
		t.Fatal(err)
	}
	if err := st.q.updateRetainedType(ctx, output1, retainedTypeExportArchive); err != nil {
		t.Fatal(err)
	}

	// Re-putting the same output must not discard what classifying it cost: the
	// output ID is the content, so the answer is still the same answer.
	put(hexOf(bytes.Repeat([]byte{9}, 32)), output1)
	blobType, retainedType := kindOf(output1)
	if !blobType.Valid || blobTypeKind(blobType.Int64) != blobTypeGoPackageArchive {
		t.Fatalf("blob_type = %v after re-put, want %d", blobType, blobTypeGoPackageArchive)
	}
	if !retainedType.Valid || retainedTypeKind(retainedType.Int64) != retainedTypeExportArchive {
		t.Fatalf("retained_type = %v after re-put, want %d", retainedType, retainedTypeExportArchive)
	}

	// Repointing the action at a different output cannot carry the old
	// classification across, because the classification belongs to the output.
	put(action, output2)
	blobType, retainedType = kindOf(output2)
	if blobType.Valid {
		t.Fatalf("blob_type = %d for an unclassified output, want NULL", blobType.Int64)
	}
	if retainedType.Valid {
		t.Fatalf("retained_type = %d for an unclassified output, want NULL", retainedType.Int64)
	}
	if blobType, _ := kindOf(output1); !blobType.Valid {
		t.Fatal("moving an action away from an output cleared that output's classification")
	}
}

func TestCatalogSchemaRejectsHexKeys(t *testing.T) {
	t.Parallel()

	// The catalog stores raw digests, and Go carries them as hex. Binding the hex
	// by mistake would otherwise store a TEXT value that never compares equal to
	// the key it stands for, making the row invisible rather than wrong. STRICT is
	// what makes that a failure instead of a silent one.
	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	ctx := context.Background()
	actionHex := hexOf(bytes.Repeat([]byte{7}, 32))
	outputHex := hexOf(bytes.Repeat([]byte{8}, 32))
	if err := st.q.upsertEntry(ctx, entry{
		ActionID: actionHex, OutputID: outputHex,
		Size: 1, CompressedSize: 1, CreatedAt: time.Now(), AccessedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := st.db.ExecContext(ctx,
		`UPDATE entries SET output_id = ? WHERE action_id = ?`, outputHex, idKey(actionHex)); err == nil {
		t.Fatal("stored a hex string in a BLOB key column")
	}
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO entries(action_id, output_id, size, compressed_size, created_at, accessed_at)
		 VALUES (?, ?, 1, 1, 0, 0)`, actionHex, idKey(outputHex)); err == nil {
		t.Fatal("inserted a hex string as an action ID")
	}

	// The round trip still works, so the rejection above is about the type and not
	// about the statements being wrong.
	ent, err := st.q.lookupEntry(ctx, actionHex)
	if err != nil {
		t.Fatal(err)
	}
	if ent.OutputID != outputHex {
		t.Fatalf("output ID = %q, want %q", ent.OutputID, outputHex)
	}
}

func assertBlobKind(t *testing.T, statuses []blobTypeStatus, kind blobTypeKind, count int64) {
	t.Helper()
	for _, status := range statuses {
		if status.kind == kind {
			if status.count != count {
				t.Fatalf("kind %s count = %d, want %d", kind.label(), status.count, count)
			}
			return
		}
	}
	t.Fatalf("kind %s not found in statuses", kind.label())
}

func assertRetainedKind(t *testing.T, statuses []retainedTypeStatus, kind retainedTypeKind, count int64) {
	t.Helper()
	for _, status := range statuses {
		if status.kind == kind {
			if status.count != count {
				t.Fatalf("kind %s count = %d, want %d", kind.label(), status.count, count)
			}
			return
		}
	}
	t.Fatalf("kind %s not found in statuses", kind.label())
}

func execCatalog(t *testing.T, dbPath, query string, args ...any) {
	t.Helper()
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func queryCatalog(t *testing.T, dbPath, query string, args []any, dest ...any) {
	t.Helper()
	db, err := openExistingDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(dest...); err != nil {
		t.Fatal(err)
	}
}

func TestRunReportsInitialWriteError(t *testing.T) {
	t.Parallel()

	if err := run([]string{"-dir", t.TempDir()}, strings.NewReader(""), errWriter{}); err == nil {
		t.Fatal("run accepted failing stdout")
	}
}

func TestStdoutIsTerminal(t *testing.T) {
	t.Parallel()

	// The go command connects the helper's stdout to a pipe and tests use
	// in-memory writers; none of these must be detected as a terminal, so the
	// protocol handshake is still emitted rather than usage.
	if stdoutIsTerminal(&bytes.Buffer{}) {
		t.Fatal("bytes.Buffer reported as terminal")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close() //nolint:errcheck
	defer w.Close() //nolint:errcheck
	if stdoutIsTerminal(w) {
		t.Fatal("os.Pipe reported as terminal")
	}
}

func TestRunProtocolIgnoresTerminalCheckForPipes(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	stdin := strings.NewReader(`{"ID":1,"Command":"close"}` + "\n")
	if err := run([]string{"-dir", t.TempDir()}, stdin, &stdout); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), `"KnownCommands"`) {
		t.Fatalf("protocol handshake missing; got %q", stdout.String())
	}
}

func TestRunReportsUsageErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		args []string
		mode runMode
	}{
		{"unknown flag", []string{"-bogus"}, runModeProtocol},
		{"missing flag value", []string{"-dir"}, runModeProtocol},
		{"unknown subcommand", []string{"bogus"}, runModeProtocol},
		{"status unknown flag", []string{"status", "-bogus"}, runModeStatus},
		{"clean stray argument", []string{"clean", "extra"}, runModeClean},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var stdout bytes.Buffer
			err := run(tc.args, strings.NewReader(""), &stdout)
			var ue *usageError
			if !errors.As(err, &ue) {
				t.Fatalf("error = %v, want usageError", err)
			}
			if ue.mode != tc.mode {
				t.Fatalf("mode = %q, want %q", ue.mode, tc.mode)
			}
			if stdout.Len() != 0 {
				t.Fatalf("usage errors must not write to stdout; got %q", stdout.String())
			}
		})
	}
}

func TestRunValueErrorIsNotUsageError(t *testing.T) {
	t.Parallel()

	err := run([]string{"-dir", t.TempDir(), "-max-size", "wat"}, strings.NewReader(""), &bytes.Buffer{})
	if err == nil {
		t.Fatal("run accepted bad max-size")
	}
	var ue *usageError
	if errors.As(err, &ue) {
		t.Fatalf("value error classified as usageError: %v", err)
	}
}

func TestRunWritesProfiles(t *testing.T) {
	dir := t.TempDir()
	cpuProfile := filepath.Join(dir, "cpu.pprof")
	memProfile := filepath.Join(dir, "mem.pprof")

	var stdin bytes.Buffer
	stdin.WriteString(`{"ID":1,"Command":"close"}`)

	var stdout bytes.Buffer
	if err := run([]string{
		"-dir", filepath.Join(dir, "cache"),
		"-max-size", "0",
		"-cpuprofile", cpuProfile,
		"-memprofile", memProfile,
	}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	assertNonEmptyFile(t, cpuProfile)
	assertNonEmptyFile(t, memProfile)
}

func TestRunProtocolConcurrentGets(t *testing.T) {
	t.Parallel()

	actionID := bytes.Repeat([]byte{16}, 32)
	outputID := bytes.Repeat([]byte{17}, 32)
	body := []byte("concurrent protocol body")

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	})
	stdin.WriteByte('\n')
	writeJSON(t, &stdin, base64.StdEncoding.EncodeToString(body))
	for id := int64(2); id < 42; id++ {
		writeJSON(t, &stdin, request{
			ID:       id,
			Command:  cmdGet,
			ActionID: actionID,
		})
	}

	writeJSON(t, &stdin, request{ID: 42, Command: cmdClose})

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	var putRes response
	if err := dec.Decode(&putRes); err != nil {
		t.Fatal(err)
	}
	if putRes.ID != 1 || putRes.Err != "" {
		t.Fatalf("put response = %+v", putRes)
	}

	seen := map[int64]bool{}
	for range 41 {
		var res response
		if err := dec.Decode(&res); err != nil {
			t.Fatal(err)
		}
		if res.Err != "" {
			t.Fatalf("response %d has error: %s", res.ID, res.Err)
		}
		seen[res.ID] = true
		if res.ID >= 2 && res.ID < 42 && (res.Miss || !bytes.Equal(res.OutputID, outputID)) {
			t.Fatalf("get response = %+v", res)
		}
	}

	for id := int64(2); id <= 42; id++ {
		if !seen[id] {
			t.Fatalf("missing response ID %d", id)
		}
	}
}

func TestRunReturnsProtocolReadError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, strings.NewReader("{bad json}\n"), &stdout)
	if err == nil {
		t.Fatal("run succeeded with bad JSON request")
	}
}

func TestRunUnknownCommandResponse(t *testing.T) {
	t.Parallel()

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{ID: 1, Command: command("wat")})
	writeJSON(t, &stdin, request{ID: 2, Command: cmdClose})

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	var unknown response
	if err := dec.Decode(&unknown); err != nil {
		t.Fatal(err)
	}
	if unknown.ID != 1 || !strings.Contains(unknown.Err, "unknown command") {
		t.Fatalf("unknown command response = %+v", unknown)
	}
}

func TestProtocolHelpersRejectInvalidBodies(t *testing.T) {
	t.Parallel()

	if _, err := bodyReader(bufio.NewReader(strings.NewReader("")), -1); err == nil {
		t.Fatal("bodyReader accepted negative size")
	}
	if _, err := bodyReader(bufio.NewReader(strings.NewReader("null")), 1); err == nil {
		t.Fatal("bodyReader accepted non-string body")
	}
	r, err := bodyReader(bufio.NewReader(strings.NewReader(`"bad\n"`)), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("bodyReader accepted escaped string body")
	}
}

func TestJSONStringReaderSmallReads(t *testing.T) {
	t.Parallel()

	raw, err := newJSONStringReader(bufio.NewReaderSize(strings.NewReader(`"abcdef"`), 3))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	var got []byte
	for {
		n, err := raw.Read(buf)
		got = append(got, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if string(got) != "abcdef" {
		t.Fatalf("read = %q, want abcdef", got)
	}
}

func TestReadRequestSkipsBlankLinesAndReportsEOF(t *testing.T) {
	t.Parallel()

	br := bufio.NewReader(strings.NewReader("\n \n"))
	if _, err := readRequest(br); !errors.Is(err, io.EOF) {
		t.Fatalf("readRequest err = %v, want EOF", err)
	}
}

func TestBodyReaderZeroSize(t *testing.T) {
	t.Parallel()

	r, err := bodyReader(bufio.NewReader(strings.NewReader("")), 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("body = %q, want empty", got)
	}
}

func TestBodyReaderReportsEOFBeforeString(t *testing.T) {
	t.Parallel()

	if _, err := bodyReader(bufio.NewReader(strings.NewReader("")), 1); !errors.Is(err, io.EOF) {
		t.Fatalf("bodyReader err = %v, want EOF", err)
	}
}

func TestJSONStringReaderLargeStringAndZeroRead(t *testing.T) {
	t.Parallel()

	want := strings.Repeat("a", 5000)
	raw, err := newJSONStringReader(bufio.NewReaderSize(strings.NewReader(strconvQuote(want)), 16))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := raw.Read(nil); n != 0 || err != nil {
		t.Fatalf("zero Read = %d, %v; want 0, nil", n, err)
	}
	got, err := io.ReadAll(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("read len = %d, want %d", len(got), len(want))
	}
}

func TestResponseWriterKeepsFirstError(t *testing.T) {
	t.Parallel()

	rw := &responseWriter{enc: json.NewEncoder(errWriter{})}
	if err := rw.write(response{ID: 1}); err == nil {
		t.Fatal("write succeeded")
	}
	if err := rw.write(response{ID: 2}); err == nil {
		t.Fatal("second write succeeded")
	}
	if err := rw.err(); err == nil {
		t.Fatal("err returned nil")
	}
}

func TestCorruptBlobIsCacheMiss(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{28}, 32)
	outputID := bytes.Repeat([]byte{29}, 32)
	body := []byte("valid body")
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	st.close()

	st, err = newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if err := os.WriteFile(blobPath, []byte("not zstd"), 0o666); err != nil {
		t.Fatal(err)
	}

	res, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Miss {
		t.Fatalf("get response = %+v, want miss", res)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); !errorsIs(err, sql.ErrNoRows) {
		t.Fatalf("entry = %v, want sql.ErrNoRows", err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("blob stat err = %v, want not exist", err)
	}
	// The inventory row has to go with it. Left behind it would keep counting the
	// blob's compressed bytes toward maxSize, and eviction would work to a total
	// that no longer exists.
	var inventoried int
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outputs WHERE output_id = ?`,
		idKey(hexOf(outputID))).Scan(&inventoried); err != nil {
		t.Fatal(err)
	}
	if inventoried != 0 {
		t.Error("a corrupt blob is still inventoried")
	}
}

func TestParseFlagsLoadsDefaultConfigFile(t *testing.T) {
	setUserDirEnv(t)

	configPath, _ := defaultConfigPath()
	cacheDir := filepath.Join(t.TempDir(), "configured-cache")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o777); err != nil {
		t.Fatal(err)
	}
	configJSON := `{
		"cacheDir": ` + strconvQuote(cacheDir) + `,
		"maxSize": "123MiB",
		"maxAge": "3d",
		"verbose": true
	}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o666); err != nil {
		t.Fatal(err)
	}

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != cacheDir {
		t.Fatalf("dir = %q, want %q", cfg.dir, cacheDir)
	}
	if cfg.maxSize != 123<<20 {
		t.Fatalf("maxSize = %d, want %d", cfg.maxSize, 123<<20)
	}
	if cfg.maxAge != 3*24*time.Hour {
		t.Fatalf("maxAge = %v, want %v", cfg.maxAge, 3*24*time.Hour)
	}
	if !cfg.verbose {
		t.Fatal("verbose = false, want true")
	}
}

func TestParseFlagsUsesUserCacheDirDefault(t *testing.T) {
	setUserDirEnv(t)

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(userCacheDir, "gocachez")
	if cfg.dir != want {
		t.Fatalf("dir = %q, want %q", cfg.dir, want)
	}
	if cfg.maxAge != defaultMaxAge {
		t.Fatalf("maxAge = %v, want %v", cfg.maxAge, defaultMaxAge)
	}
}

func TestParseFlagsRequiresExplicitConfig(t *testing.T) {
	if _, err := parseFlags([]string{"-config", filepath.Join(t.TempDir(), "missing.json")}); err == nil {
		t.Fatal("parseFlags succeeded with a missing explicit config")
	}
}

func TestParseFlagsOverrideConfigFile(t *testing.T) {
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.json")
	configCacheDir := filepath.Join(configDir, "config-cache")
	flagCacheDir := filepath.Join(configDir, "flag-cache")
	configJSON := `{"cacheDir": ` + strconvQuote(configCacheDir) + `, "maxSize": "10MiB", "maxAge": "10d"}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o666); err != nil {
		t.Fatal(err)
	}

	cfg, err := parseFlags([]string{"-config", configPath, "-dir", flagCacheDir, "-max-size", "1MiB", "-max-age", "2d"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != flagCacheDir {
		t.Fatalf("dir = %q, want %q", cfg.dir, flagCacheDir)
	}
	if cfg.maxSize != 1<<20 {
		t.Fatalf("maxSize = %d, want %d", cfg.maxSize, 1<<20)
	}
	if cfg.maxAge != 2*24*time.Hour {
		t.Fatalf("maxAge = %v, want %v", cfg.maxAge, 2*24*time.Hour)
	}
}

func TestParseFlagsUsesEnvironmentOverrides(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv("GOCACHEZ_DIR", cacheDir)
	t.Setenv("GOCACHEZ_MAX_SIZE", "7MiB")
	t.Setenv("GOCACHEZ_MAX_AGE", "36h")
	t.Setenv("GOCACHEZ_VERBOSE", "true")
	t.Setenv("GOCACHEZ_CONFIG", "")

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != cacheDir {
		t.Fatalf("dir = %q, want %q", cfg.dir, cacheDir)
	}
	if cfg.maxSize != 7<<20 {
		t.Fatalf("maxSize = %d, want %d", cfg.maxSize, 7<<20)
	}
	if cfg.maxAge != 36*time.Hour {
		t.Fatalf("maxAge = %v, want %v", cfg.maxAge, 36*time.Hour)
	}
	if !cfg.verbose {
		t.Fatal("verbose = false, want true")
	}
}

// A command with no entry in rootHelp is undiscoverable, and one with no case in
// writeHelp answers -h with the root text, which does not mention it. Both stayed
// green through a whole release of -max-retained-age being missing from the help,
// so pin them the way the flags are pinned.
func TestRootHelpDocumentsEveryCommand(t *testing.T) {
	t.Parallel()

	listed := map[string]bool{}
	inCommands := false
	for line := range strings.SplitSeq(rootHelp, "\n") {
		if strings.HasPrefix(line, "Commands:") {
			inCommands = true
			continue
		}
		if inCommands {
			if strings.TrimSpace(line) == "" {
				break
			}
			name, _, _ := strings.Cut(strings.TrimSpace(line), " ")
			listed[name] = true
		}
	}

	for _, mode := range runModes {
		if !listed[string(mode)] {
			t.Errorf("%q is accepted but not listed under Commands in rootHelp", mode)
		}
		var help bytes.Buffer
		if err := writeHelp(&help, mode); err != nil {
			t.Fatal(err)
		}
		if help.String() == rootHelp {
			t.Errorf("%q has no help text of its own; -h falls through to rootHelp", mode)
		}
		if !strings.Contains(help.String(), "gocachez "+string(mode)) {
			t.Errorf("%q help does not show its own usage line: %q", mode, help.String())
		}
	}
}

func TestRootHelpListsEveryFlag(t *testing.T) {
	t.Parallel()

	// Deliberately undocumented: profiling knobs for working on gocachez itself,
	// not for configuring a cache.
	hidden := map[string]bool{"cpuprofile": true, "memprofile": true}

	fs := flag.NewFlagSet("gocachez", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	bindFlags(fs, &rawFlags{})
	// Match where a flag is *listed*, not merely mentioned: one flag's
	// description can name another, which would otherwise pass for it.
	listed := map[string]bool{}
	for line := range strings.SplitSeq(rootHelp, "\n") {
		name, _, _ := strings.Cut(strings.TrimPrefix(strings.TrimSpace(line), "-"), " ")
		if name != "" {
			listed[name] = true
		}
	}
	fs.VisitAll(func(f *flag.Flag) {
		if hidden[f.Name] {
			return
		}
		if !listed[f.Name] {
			t.Errorf("-%s is accepted but not listed in rootHelp", f.Name)
		}
	})
}

func TestParseFlagsResolvesMaxRetainedAge(t *testing.T) {
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.json")
	configJSON := `{"maxAge": "10d", "maxRetainedAge": "4h"}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o666); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		args []string
		env  string
		want time.Duration
	}{
		{name: "config file", args: []string{"-config", configPath}, want: 4 * time.Hour},
		{name: "env overrides config", args: []string{"-config", configPath}, env: "90m", want: 90 * time.Minute},
		{name: "flag overrides env", args: []string{"-config", configPath, "-max-retained-age", "30m"}, env: "90m", want: 30 * time.Minute},
		// Unset, and explicit zero, both mean "follow maxAge" — the zero value of
		// a config must not silently switch retained pruning off.
		{name: "unset follows maxAge", args: []string{"-max-age", "3d"}, want: 3 * 24 * time.Hour},
		{name: "zero follows maxAge", args: []string{"-max-age", "3d", "-max-retained-age", "0"}, want: 3 * 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOCACHEZ_MAX_RETAINED_AGE", tc.env)
			t.Setenv("GOCACHEZ_CONFIG", "")
			cfg, err := parseFlags(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.retainedAge(); got != tc.want {
				t.Fatalf("retainedAge = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDefaultConfigPathUsesEnvironment(t *testing.T) {
	t.Setenv("GOCACHEZ_CONFIG", filepath.Join(t.TempDir(), "config.json"))

	path, required := defaultConfigPath()
	if path != os.Getenv("GOCACHEZ_CONFIG") || !required {
		t.Fatalf("defaultConfigPath = %q, %v; want env path required", path, required)
	}
}

func TestParseFlagsRejectsInvalidInputs(t *testing.T) {
	if _, err := parseFlags([]string{"-bad"}); err == nil {
		t.Fatal("parseFlags accepted unknown flag")
	}
	if _, err := parseFlags([]string{"-dir", t.TempDir(), "-max-size", "bad"}); err == nil {
		t.Fatal("parseFlags accepted bad max size")
	}
	if _, err := parseFlags([]string{"-dir", t.TempDir(), "-max-age", "bad"}); err == nil {
		t.Fatal("parseFlags accepted bad max age")
	}

	t.Setenv("GOCACHEZ_MAX_SIZE", "bad")
	if _, err := parseFlags([]string{"-dir", t.TempDir()}); err == nil {
		t.Fatal("parseFlags accepted bad GOCACHEZ_MAX_SIZE")
	}
	t.Setenv("GOCACHEZ_MAX_SIZE", "")
	t.Setenv("GOCACHEZ_MAX_AGE", "bad")
	if _, err := parseFlags([]string{"-dir", t.TempDir()}); err == nil {
		t.Fatal("parseFlags accepted bad GOCACHEZ_MAX_AGE")
	}
	t.Setenv("GOCACHEZ_MAX_AGE", "")
	t.Setenv("GOCACHEZ_VERBOSE", "bad")
	if _, err := parseFlags([]string{"-dir", t.TempDir()}); err == nil {
		t.Fatal("parseFlags accepted bad GOCACHEZ_VERBOSE")
	}
}

func TestApplyConfigFileRejectsInvalidJSONAndSize(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	badJSON := filepath.Join(dir, "bad-json.json")
	if err := os.WriteFile(badJSON, []byte("{"), 0o666); err != nil {
		t.Fatal(err)
	}
	var cfg config
	if err := applyConfigFile(&cfg, badJSON, true); err == nil {
		t.Fatal("applyConfigFile accepted bad JSON")
	}

	badSize := filepath.Join(dir, "bad-size.json")
	if err := os.WriteFile(badSize, []byte(`{"maxSize":"wat"}`), 0o666); err != nil {
		t.Fatal(err)
	}
	if err := applyConfigFile(&cfg, badSize, true); err == nil {
		t.Fatal("applyConfigFile accepted bad maxSize")
	}
}

func TestParseSize(t *testing.T) {
	t.Parallel()

	tests := map[string]int64{
		"0":      0,
		"42":     42,
		"1KiB":   1 << 10,
		"1.5MiB": 1<<20 + 1<<19,
		"2g":     2 << 30,
	}
	for input, want := range tests {
		got, err := parseSize(input)
		if err != nil {
			t.Fatalf("parseSize(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("parseSize(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestParseSizeErrors(t *testing.T) {
	t.Parallel()

	for _, input := range []string{"", "MiB", "1bad", "-1", "."} {
		if _, err := parseSize(input); err == nil {
			t.Fatalf("parseSize(%q) succeeded", input)
		}
	}
}

func TestParseAge(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]time.Duration{
		"0":    0,
		"5d":   5 * 24 * time.Hour,
		"1.5d": 36 * time.Hour,
		"36h":  36 * time.Hour,
		"90m":  90 * time.Minute,
	} {
		got, err := parseAge(input)
		if err != nil {
			t.Fatalf("parseAge(%q) error: %v", input, err)
		}
		if got != want {
			t.Fatalf("parseAge(%q) = %v, want %v", input, got, want)
		}
	}

	for _, input := range []string{"", "d", "-5d", "-1h", "wat", "5x"} {
		if _, err := parseAge(input); err == nil {
			t.Fatalf("parseAge(%q) succeeded", input)
		}
	}
}

func TestFormatSize(t *testing.T) {
	t.Parallel()

	tests := map[int64]string{
		0:       "0B",
		42:      "42B",
		1024:    "1.0KiB",
		1536:    "1.5KiB",
		1 << 20: "1.0MiB",
		1 << 30: "1.0GiB",
	}
	for input, want := range tests {
		if got := formatSize(input); got != want {
			t.Fatalf("formatSize(%d) = %q, want %q", input, got, want)
		}
	}
}

func TestFormatSavings(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		uncompressed int64
		compressed   int64
		want         string
	}{
		"empty": {
			uncompressed: 0,
			compressed:   0,
			want:         "0B (0.0%)",
		},
		"saved": {
			uncompressed: 100,
			compressed:   25,
			want:         "75B (75.0%)",
		},
		"grew": {
			uncompressed: 100,
			compressed:   125,
			want:         "-25B (-25.0%)",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := formatSavings(tc.uncompressed, tc.compressed); got != tc.want {
				t.Fatalf("formatSavings(%d, %d) = %q, want %q", tc.uncompressed, tc.compressed, got, tc.want)
			}
		})
	}
}

func TestFormatSavingsParts(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		uncompressed int64
		compressed   int64
		wantAmount   string
		wantPercent  string
	}{
		"empty": {
			uncompressed: 0,
			compressed:   13,
			wantAmount:   "0B",
			wantPercent:  "0.0%",
		},
		"saved": {
			uncompressed: 100,
			compressed:   25,
			wantAmount:   "75B",
			wantPercent:  "75.0%",
		},
		"grew": {
			uncompressed: 100,
			compressed:   125,
			wantAmount:   "-25B",
			wantPercent:  "-25.0%",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := formatSavingsAmount(tc.uncompressed, tc.compressed); got != tc.wantAmount {
				t.Fatalf("formatSavingsAmount(%d, %d) = %q, want %q", tc.uncompressed, tc.compressed, got, tc.wantAmount)
			}
			if got := formatSavingsPercent(tc.uncompressed, tc.compressed); got != tc.wantPercent {
				t.Fatalf("formatSavingsPercent(%d, %d) = %q, want %q", tc.uncompressed, tc.compressed, got, tc.wantPercent)
			}
		})
	}
}

func TestFormatAge(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		age  time.Duration
		want string
	}{
		"negative": {-time.Second, "<1m"},
		"seconds":  {30 * time.Second, "<1m"},
		"minutes":  {42 * time.Minute, "42m"},
		"hours":    {3*time.Hour + 12*time.Minute, "3h 12m"},
		"days":     {5*24*time.Hour + 4*time.Hour, "5d 4h"},
		"years":    {2*365*24*time.Hour + 3*24*time.Hour, "2y 3d"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := formatAge(tc.age); got != tc.want {
				t.Fatalf("formatAge(%v) = %q, want %q", tc.age, got, tc.want)
			}
		})
	}
}

func TestFormatMaxUsage(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		size    int64
		maxSize int64
		want    string
	}{
		"disabled": {size: 10, maxSize: 0, want: "disabled"},
		"empty":    {size: 0, maxSize: 20 << 30, want: "0B / 20.0GiB (0.0%, 20.0GiB remaining)"},
		"half":     {size: 10 << 30, maxSize: 20 << 30, want: "10.0GiB / 20.0GiB (50.0%, 10.0GiB remaining)"},
		"over":     {size: 25 << 30, maxSize: 20 << 30, want: "25.0GiB / 20.0GiB (125.0%, -5.0GiB remaining)"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := formatMaxUsage(tc.size, tc.maxSize); got != tc.want {
				t.Fatalf("formatMaxUsage(%d, %d) = %q, want %q", tc.size, tc.maxSize, got, tc.want)
			}
		})
	}
}

func TestFormatMaxAge(t *testing.T) {
	t.Parallel()

	for input, want := range map[time.Duration]string{
		0:                  "disabled",
		-time.Hour:         "disabled",
		5 * 24 * time.Hour: "5d",
		36 * time.Hour:     "1d 12h",
		90 * time.Minute:   "1h 30m",
	} {
		if got := formatMaxAge(input); got != want {
			t.Fatalf("formatMaxAge(%v) = %q, want %q", input, got, want)
		}
	}
}

func encodedBody(body []byte) *bytes.Buffer {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(base64.StdEncoding.EncodeToString(body)); err != nil {
		panic(err)
	}
	return &buf
}

func writeJSON(t *testing.T, buf *bytes.Buffer, v any) {
	t.Helper()
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		t.Fatal(err)
	}
}

func hexOf(id []byte) string {
	return hex.EncodeToString(id)
}

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func writeCompressedFile(st *store, path string, body []byte) error {
	var buf bytes.Buffer
	enc, err := st.getEncoder(&buf)
	if err != nil {
		return err
	}
	if _, err := enc.Write(body); err != nil {
		return err
	}
	if err := enc.Close(); err != nil {
		return err
	}
	st.putEncoder(enc)
	return os.WriteFile(path, buf.Bytes(), 0o666)
}

func goArchive(pkgdef, object []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(archiveMagic)
	writeArchiveMember(&buf, pkgdefName, pkgdef)
	writeArchiveMember(&buf, "_go_.o", object)
	return buf.Bytes()
}

func goPkgdef(exportData []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("go object fake\n")
	buf.WriteString("$$B\n")
	buf.Write(exportData)
	buf.WriteString("\n$$\n")
	return buf.Bytes()
}

func writeArchiveMember(buf *bytes.Buffer, name string, body []byte) {
	fmt.Fprintf(buf, "%-16s%-12d%-6d%-6d%-8o%-10d`\n", name, 0, 0, 0, 0o644, len(body))
	buf.Write(body)
	if len(body)%2 != 0 {
		buf.WriteByte('\n')
	}
}

func readExportData(t *testing.T, path string) []byte {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close() //nolint:errcheck

	reader, err := gcexportdata.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func retainedPath(cacheDir string, outputID []byte, ext string) string {
	outputHex := hexOf(outputID)
	return filepath.Join(testVersionDir(cacheDir), retainedDirName, outputHex[:2], outputHex+ext)
}

// incompressibleBody returns bytes zstd cannot shrink, so a test can reason
// about the cache's stored size.
func incompressibleBody(t *testing.T, size int, seed int64) []byte {
	t.Helper()
	body := make([]byte, 0, size)
	for i := 0; len(body) < size; i++ {
		sum := sha256.Sum256(fmt.Appendf(nil, "incompressible-%d-%d", seed, i))
		body = append(body, sum[:]...)
	}
	return body[:size]
}

// prunePromptly runs prune and fails if it blocks, rather than deadlocking the
// test binary when something is holding a lock it should have declined.
func prunePromptly(t *testing.T, st *store) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- st.prune() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prune blocked instead of skipping")
	}
}

// expireMaintenanceStamps backdates every maintenance stamp, standing in for the
// passage of pruneInterval so the next prune sweeps and scans instead of
// skipping. The sweep and the scan are stamped separately, and a test that wants
// maintenance to happen almost always wants both.
func expireMaintenanceStamps(t *testing.T, versionDir string) {
	t.Helper()
	old := time.Now().Add(-2 * pruneInterval)
	for _, path := range []string{pruneStampPath(versionDir), sweepStampPath(versionDir)} {
		if err := os.Chtimes(path, old, old); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
}

func errorsIs(err, target error) bool {
	return err != nil && errors.Is(err, target)
}

func countRunRows(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE run_id = ?`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertNonEmptyFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatalf("%s is empty", path)
	}
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("output missing %q:\n%s", want, got)
	}
}

func setUserDirEnv(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	home := filepath.Join(root, "home")
	cacheDir := filepath.Join(root, "cache")
	configDir := filepath.Join(root, "config")
	for _, dir := range []string{home, cacheDir, configDir} {
		if err := os.MkdirAll(dir, 0o777); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CACHE_HOME", cacheDir)
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("LOCALAPPDATA", cacheDir)
	t.Setenv("APPDATA", configDir)
	t.Setenv("GOCACHEZ_CONFIG", "")
}

func abandonStore(t *testing.T, st *store) {
	t.Helper()
	if err := st.runLock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := st.runLock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.db.Close(); err != nil {
		t.Fatal(err)
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func strconvQuote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// testVersionDir is where a cache directory's current schema version lives, so
// tests do not carry a version number that a schema change has to chase.
func testVersionDir(cacheDir string) string {
	return cacheVersionDir(config{dir: cacheDir})
}

// An action re-put onto a different output leaves the old output inventoried with
// nothing naming it — cmd/go does this whenever a build's result changes. Nothing
// indexes output back to action, so the put cannot notice, and every entry in the
// catalog can be fresh while that row is months old. Deciding whether an age pass
// is worth the lock therefore has to look at both tables, or this garbage is only
// ever reclaimed by size pressure.
func TestPruneReclaimsAnOutputNoEntryNamesAnyMore(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir(), maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{71}, 32)
	abandoned := bytes.Repeat([]byte{72}, 32)
	replacement := bytes.Repeat([]byte{73}, 32)
	body := []byte("rebuilt output")
	for _, outputID := range [][]byte{abandoned, replacement} {
		if _, err := st.put(request{
			ID: 1, Command: cmdPut, ActionID: actionID, OutputID: outputID,
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
	}

	// Only the abandoned output is stale; the entry, now naming the replacement,
	// was written just above and is fresh.
	stale := unixMillis(trimCutoff(defaultMaxAge, time.Now())) - int64(time.Minute/time.Millisecond)
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE outputs SET accessed_at = ? WHERE output_id = ?`,
		stale, idKey(hexOf(abandoned))); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(),
		`DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	var inventoried int
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outputs WHERE output_id = ?`,
		idKey(hexOf(abandoned))).Scan(&inventoried); err != nil {
		t.Fatal(err)
	}
	if inventoried != 0 {
		t.Error("an output no entry names was left in the inventory")
	}
	if _, err := os.Stat(st.blobPath(hexOf(abandoned))); !os.IsNotExist(err) {
		t.Errorf("abandoned blob stat err = %v, want not exist", err)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); err != nil {
		t.Fatalf("the surviving entry was pruned: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(replacement))); err != nil {
		t.Errorf("replacement blob was pruned: %v", err)
	}
}

// entries reference an output by an interned id, so that id must never be handed
// out twice. A plain INTEGER PRIMARY KEY assigns max(id)+1 over the surviving
// rows, which means evicting the newest output frees its id for the next put — and
// an entry still naming it would then resolve to a different blob and hand back
// the wrong build artifact, silently. AUTOINCREMENT is what forbids that.
func TestEvictedOutputIDIsNeverReused(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	ctx := context.Background()
	action := bytes.Repeat([]byte{81}, 32)
	evicted := bytes.Repeat([]byte{82}, 32)
	body := []byte("first artifact")
	if _, err := st.put(request{
		ID: 1, Command: cmdPut, ActionID: action, OutputID: evicted,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}

	var evictedRef int64
	if err := st.db.QueryRowContext(ctx, `SELECT output FROM entries WHERE action_id = ?`,
		idKey(hexOf(action))).Scan(&evictedRef); err != nil {
		t.Fatal(err)
	}

	// Size pressure takes the output. The entry naming it is left behind, because
	// nothing indexes output back to action.
	if _, err := st.q.evictOutput(ctx, hexOf(evicted), unixMillis(time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}

	// A different action produces different content.
	replacement := bytes.Repeat([]byte{83}, 32)
	otherBody := []byte("unrelated artifact")
	if _, err := st.put(request{
		ID: 2, Command: cmdPut, ActionID: bytes.Repeat([]byte{84}, 32), OutputID: replacement,
		BodySize: int64(len(otherBody)),
	}, bufio.NewReader(encodedBody(otherBody))); err != nil {
		t.Fatal(err)
	}
	var replacementRef int64
	if err := st.db.QueryRowContext(ctx, `SELECT id FROM outputs WHERE output_id = ?`,
		idKey(hexOf(replacement))).Scan(&replacementRef); err != nil {
		t.Fatal(err)
	}
	if replacementRef == evictedRef {
		t.Fatalf("output id %d was reused after eviction", replacementRef)
	}

	// And the stale entry still reads as a miss rather than as the new content.
	res, err := st.get(request{ID: 3, Command: cmdGet, ActionID: action})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Miss {
		t.Fatalf("stale entry resolved to %x, want a miss", res.OutputID)
	}
}

// A shared output read by one action keeps its own access time fresh, so the other
// action's entry can pass maxAge while nothing in the inventory is stale at all.
// Those mappings are most of the catalog, and no blob is waiting on them, so an
// age pass that decided what to do by looking at outputs would never collect them.
func TestPruneReapsAStaleEntryWhoseOutputIsStillWarm(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir(), maxAge: defaultMaxAge})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	shared := bytes.Repeat([]byte{91}, 32)
	warmAction := bytes.Repeat([]byte{92}, 32)
	coldAction := bytes.Repeat([]byte{93}, 32)
	body := []byte("shared artifact")
	for i, action := range [][]byte{warmAction, coldAction} {
		if _, err := st.put(request{
			ID: int64(i), Command: cmdPut, ActionID: action, OutputID: shared,
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
	}

	// Only the cold action's mapping is stale. Every outputs row stays fresh,
	// because the warm action is still reading the blob.
	stale := unixMillis(trimCutoff(defaultMaxAge, time.Now())) - int64(time.Minute/time.Millisecond)
	if _, err := st.db.ExecContext(context.Background(),
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		stale, idKey(hexOf(coldAction))); err != nil {
		t.Fatal(err)
	}
	var oldestOutput int64
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT MIN(accessed_at) FROM outputs`).Scan(&oldestOutput); err != nil {
		t.Fatal(err)
	}
	if oldestOutput < stale {
		t.Fatal("an output is stale, so this would not exercise the entries-only case")
	}
	if _, err := st.db.ExecContext(context.Background(),
		`DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	expireMaintenanceStamps(t, st.versionDir)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := st.lookupEntry(hexOf(coldAction)); !errorsIs(err, sql.ErrNoRows) {
		t.Fatalf("stale mapping was not reaped: %v", err)
	}
	if _, err := st.lookupEntry(hexOf(warmAction)); err != nil {
		t.Fatalf("the warm mapping was reaped: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(shared))); err != nil {
		t.Fatalf("the shared blob was removed while still in use: %v", err)
	}
}
