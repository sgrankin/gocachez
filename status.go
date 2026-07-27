package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

type cacheStatus struct {
	cacheDir         string
	maxSize          int64
	maxAge           time.Duration
	verbose          bool
	versionDir       string
	catalogExists    bool
	catalog          catalogStatus
	blobFiles        int64
	blobTypes        []blobTypeStatus
	retainedFiles    int64
	retainedSize     int64
	retainedTypes    []retainedTypeStatus
	activeLiveRuns   int64
	inactiveLiveRuns int64
}

type catalogStatus struct {
	entries           int64
	outputs           int64
	size              int64
	compressedSize    int64
	runs              int64
	oldestAccessedAt  time.Time
	hasOldestAccessed bool
}

type retainedTypeKind int

// retainedClassifierVersion identifies the behavior of retainedFileKind. Cached
// values use this version so classification changes can invalidate them.
const retainedClassifierVersion = 1

// retainedTypeKind values are persisted in entries.retained_type. Existing
// values must not be renumbered; append new kinds at the end.
const (
	retainedTypeExportArchive retainedTypeKind = iota
	retainedTypeGeneratedCgoSource
	retainedTypeGeneratedTestmain
	retainedTypeOther
)

type retainedTypeStatus struct {
	kind  retainedTypeKind
	count int64
	size  int64
}

func writeStatus(cfg config, w io.Writer) error {
	status, err := readStatus(cfg)
	if err != nil {
		return err
	}

	if err := writeTable(w, "Configuration", [][]string{
		{"Cache directory", status.cacheDir},
		{"Version directory", status.versionDir},
		{"Max size", formatBytes(status.maxSize)},
		{"Max age", formatMaxAge(status.maxAge)},
		{"Verbose", strconv.FormatBool(status.verbose)},
	}); err != nil {
		return err
	}
	catalogState := "missing"
	if status.catalogExists {
		catalogState = "present"
	}
	if err := writeTable(w, "Summary", [][]string{
		{"State", catalogState},
		{"Cached actions", formatInt(status.catalog.entries)},
		{"Cached outputs", formatInt(status.catalog.outputs)},
		{"Oldest cache entry", formatOldestAccess(status.catalog, time.Now())},
		{"Live runs", formatLiveRuns(status.activeLiveRuns, status.inactiveLiveRuns)},
	}); err != nil {
		return err
	}
	if err := writeTable(w, "Storage", [][]string{
		{"Original output size", formatBytes(status.catalog.size)},
		{"Compressed cache blobs", formatStorageSize(status.catalog.compressedSize, status.blobFiles)},
		{"Blob max usage", formatMaxUsage(status.catalog.compressedSize, status.maxSize)},
		{"Retained go-list files", formatStorageSize(status.retainedSize, status.retainedFiles)},
		{"Total stored", formatBytes(status.catalog.compressedSize + status.retainedSize)},
		{"Blob-only savings", formatSavings(status.catalog.size, status.catalog.compressedSize)},
		{"Overall savings", formatSavings(status.catalog.size, status.catalog.compressedSize+status.retainedSize)},
	}); err != nil {
		return err
	}
	if err := writeBlobTypeStatus(w, status.blobTypes); err != nil {
		return err
	}
	if err := writeRetainedTypeStatus(w, status.retainedTypes); err != nil {
		return err
	}
	return nil
}

func readStatus(cfg config) (cacheStatus, error) {
	// Status deliberately does not take the lifecycle lock, so it has no use
	// for its path. Reading the cache is O(cache) — it decompresses
	// unclassified blobs and walks the retained tree — and holding the lock
	// that long stalls every build that starts or exits meanwhile. The readers
	// below tolerate entries and files disappearing underneath them instead,
	// which costs only accuracy in a report that is a snapshot of a moving
	// cache either way.
	versionDir, blobsDir, liveRoot, _ := cachePaths(cfg)
	status := cacheStatus{
		cacheDir:   cfg.dir,
		maxSize:    cfg.maxSize,
		maxAge:     cfg.maxAge,
		verbose:    cfg.verbose,
		versionDir: versionDir,
	}
	if _, err := os.Stat(versionDir); errors.Is(err, os.ErrNotExist) {
		return status, nil
	} else if err != nil {
		return cacheStatus{}, fmt.Errorf("stat cache version dir: %w", err)
	}

	var err error
	dbPath := filepath.Join(versionDir, "cache.db")
	var outputs []catalogOutput
	status.catalogExists, status.catalog, outputs, err = readCatalogStatus(dbPath)
	if err != nil {
		return cacheStatus{}, err
	}
	// Blob file count matches the number of cached outputs, so derive it
	// from the catalog instead of walking the blobs directory.
	status.blobFiles = status.catalog.outputs
	status.blobTypes = blobTypeStatuses(dbPath, blobsDir, outputs)
	status.retainedFiles, status.retainedSize, status.retainedTypes, err = readRetainedStatus(dbPath, retainedRoot(versionDir), outputs)
	if err != nil {
		return cacheStatus{}, err
	}
	status.activeLiveRuns, status.inactiveLiveRuns, err = readLiveStatus(liveRoot)
	if err != nil {
		return cacheStatus{}, err
	}
	return status, nil
}

// readCatalogStatus opens the catalog once and returns the aggregate status
// plus the per-output rows. It performs a single GROUP BY output_id scan
// (listOutputs) to derive the output count, sizes, and cached blob types, so
// callers can reuse outputs for the blob-type breakdown without rescanning.
func readCatalogStatus(dbPath string) (bool, catalogStatus, []catalogOutput, error) {
	if !regularFile(dbPath) {
		return false, catalogStatus{}, nil, nil
	}

	db, err := openExistingDB(dbPath)
	if err != nil {
		return false, catalogStatus{}, nil, err
	}
	defer db.Close() //nolint:errcheck

	ctx := context.Background()
	var status catalogStatus
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries`).Scan(&status.entries); err != nil {
		return false, catalogStatus{}, nil, fmt.Errorf("count catalog entries: %w", err)
	}
	var oldestAccessedAt sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MIN(accessed_at) FROM entries`).Scan(&oldestAccessedAt); err != nil {
		return false, catalogStatus{}, nil, fmt.Errorf("find oldest catalog access: %w", err)
	}
	if oldestAccessedAt.Valid {
		status.oldestAccessedAt = millisTime(oldestAccessedAt.Int64)
		status.hasOldestAccessed = true
	}

	q := newCatalog(db)
	hasBlobType, err := entriesHasColumn(ctx, db, "blob_type_version")
	if err != nil {
		return false, catalogStatus{}, nil, fmt.Errorf("inspect catalog schema: %w", err)
	}
	hasRetainedType, err := entriesHasColumn(ctx, db, "retained_type_version")
	if err != nil {
		return false, catalogStatus{}, nil, fmt.Errorf("inspect catalog schema: %w", err)
	}
	outputs, err := q.listOutputs(ctx, hasBlobType, blobClassifierVersion, hasRetainedType, retainedClassifierVersion)
	if err != nil {
		return false, catalogStatus{}, nil, fmt.Errorf("list catalog outputs: %w", err)
	}
	for _, output := range outputs {
		status.size += output.size
		status.compressedSize += output.compressedSize
	}
	status.outputs = int64(len(outputs))

	status.runs, err = q.countRuns(ctx)
	if err != nil {
		return false, catalogStatus{}, nil, fmt.Errorf("count catalog runs: %w", err)
	}
	return true, status, outputs, nil
}

func writeBlobTypeStatus(w io.Writer, statuses []blobTypeStatus) error {
	if len(statuses) == 0 {
		return writeRightAlignedTable(w, "Compressed blob contents", [][]string{
			{"Type", "Files", "Original", "Stored", "Savings"},
			{"None", "0", formatBytes(0), formatBytes(0), formatAlignedSavings(0, 0, 2, 4)},
		})
	}
	rows := [][]string{
		{"Type", "Files", "Original", "Stored", "Savings"},
	}
	amountWidth, percentWidth := savingsWidths(statuses)
	for _, status := range statuses {
		rows = append(rows, []string{
			status.kind.label(),
			formatInt(status.count),
			formatBytes(status.size),
			formatBytes(status.compressedSize),
			formatAlignedSavings(status.size, status.compressedSize, amountWidth, percentWidth),
		})
	}
	return writeRightAlignedTable(w, "Compressed blob contents", rows)
}

func savingsWidths(statuses []blobTypeStatus) (int, int) {
	amountWidth := len("0B")
	percentWidth := len("0.0%")
	for _, status := range statuses {
		amountWidth = max(amountWidth, len(formatSavingsAmount(status.size, status.compressedSize)))
		percentWidth = max(percentWidth, len(formatSavingsPercent(status.size, status.compressedSize)))
	}
	return amountWidth, percentWidth
}

func formatAlignedSavings(uncompressed, compressed int64, amountWidth, percentWidth int) string {
	return fmt.Sprintf("%*s (%*s)",
		amountWidth,
		formatSavingsAmount(uncompressed, compressed),
		percentWidth,
		formatSavingsPercent(uncompressed, compressed),
	)
}

func writeRetainedTypeStatus(w io.Writer, statuses []retainedTypeStatus) error {
	if len(statuses) == 0 {
		return writeRightAlignedTable(w, "Retained go-list files", [][]string{
			{"Type", "Files", "Size"},
			{"None", "0", formatBytes(0)},
		})
	}
	rows := [][]string{
		{"Type", "Files", "Size"},
	}
	for _, status := range statuses {
		rows = append(rows, []string{
			status.kind.label(),
			formatInt(status.count),
			formatBytes(status.size),
		})
	}
	return writeRightAlignedTable(w, "Retained go-list files", rows)
}

func writeTable(w io.Writer, title string, rows [][]string) error {
	return writeTableAligned(w, title, rows, false)
}

func writeRightAlignedTable(w io.Writer, title string, rows [][]string) error {
	return writeTableAligned(w, title, rows, true)
}

func writeTableAligned(w io.Writer, title string, rows [][]string, rightAlignColumns bool) error {
	if _, err := fmt.Fprintf(w, "%s:\n", title); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	widths := tableWidths(rows)
	for _, row := range rows {
		if _, err := fmt.Fprint(w, "  "); err != nil {
			return fmt.Errorf("write status: %w", err)
		}
		last := len(row) - 1
		for last > 0 && row[last] == "" {
			last--
		}
		for i, cell := range row[:last+1] {
			if i > 0 {
				if _, err := fmt.Fprint(w, "  "); err != nil {
					return fmt.Errorf("write status: %w", err)
				}
			}
			padding := widths[i] - len(cell)
			if rightAlignColumns && i > 0 {
				if _, err := fmt.Fprint(w, strings.Repeat(" ", padding)); err != nil {
					return fmt.Errorf("write status: %w", err)
				}
			}
			if _, err := fmt.Fprint(w, cell); err != nil {
				return fmt.Errorf("write status: %w", err)
			}
			if i < last && (!rightAlignColumns || i == 0) {
				if _, err := fmt.Fprint(w, strings.Repeat(" ", padding)); err != nil {
					return fmt.Errorf("write status: %w", err)
				}
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return fmt.Errorf("write status: %w", err)
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	return nil
}

func tableWidths(rows [][]string) []int {
	var widths []int
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(widths) {
				widths = append(widths, 0)
			}
			widths[i] = max(widths[i], len(cell))
		}
	}
	return widths
}

func formatBytes(size int64) string {
	return formatSize(size)
}

func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

func formatStorageSize(size, files int64) string {
	return fmt.Sprintf("%s (%s)", formatBytes(size), formatFileCount(files))
}

func formatFileCount(files int64) string {
	if files == 1 {
		return "1 file"
	}
	return formatInt(files) + " files"
}

func formatLiveRuns(active, inactive int64) string {
	return fmt.Sprintf("%s active, %s inactive", formatInt(active), formatInt(inactive))
}

func formatMaxUsage(size, maxSize int64) string {
	if maxSize <= 0 {
		return "disabled"
	}
	percent := float64(size) / float64(maxSize) * 100
	remaining := maxSize - size
	return fmt.Sprintf("%s / %s (%.1f%%, %s remaining)", formatBytes(size), formatBytes(maxSize), percent, formatBytes(remaining))
}

func formatMaxAge(maxAge time.Duration) string {
	if maxAge <= 0 {
		return "disabled"
	}
	return formatAge(maxAge)
}

func formatOldestAccess(status catalogStatus, now time.Time) string {
	if !status.hasOldestAccessed {
		return "n/a"
	}
	return formatAge(now.Sub(status.oldestAccessedAt))
}

func formatAge(age time.Duration) string {
	if age < 0 {
		age = 0
	}
	switch {
	case age < time.Minute:
		return "<1m"
	case age < time.Hour:
		return fmt.Sprintf("%dm", int(age/time.Minute))
	case age < 24*time.Hour:
		hours := int(age / time.Hour)
		minutes := int((age % time.Hour) / time.Minute)
		if minutes == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		return fmt.Sprintf("%dh %dm", hours, minutes)
	case age < 365*24*time.Hour:
		days := int(age / (24 * time.Hour))
		hours := int((age % (24 * time.Hour)) / time.Hour)
		if hours == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd %dh", days, hours)
	default:
		years := int(age / (365 * 24 * time.Hour))
		days := int((age % (365 * 24 * time.Hour)) / (24 * time.Hour))
		if days == 0 {
			return fmt.Sprintf("%dy", years)
		}
		return fmt.Sprintf("%dy %dd", years, days)
	}
}

func readRetainedStatus(dbPath, root string, outputs []catalogOutput) (int64, int64, []retainedTypeStatus, error) {
	byKind := make(map[retainedTypeKind]*retainedTypeStatus)
	cached := make(map[string]retainedTypeKind)
	for _, output := range outputs {
		if output.retainedType.Valid {
			cached[output.outputID] = retainedTypeKind(output.retainedType.Int64)
		}
	}
	classified := make(map[string]retainedTypeKind)
	var files, size int64
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// A concurrent prune may remove a file or shard between the readdir
			// and the visit. Skip what is already gone rather than failing the
			// whole report.
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("stat retained file: %w", err)
		}
		outputID := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		kind, ok := cached[outputID]
		if !ok {
			kind = retainedFileKind(path)
			// Archive classification follows directly from the extension. Only
			// source files need an expensive content-based result cached.
			if filepath.Ext(path) == ".go" {
				classified[outputID] = kind
			}
		}
		status := byKind[kind]
		if status == nil {
			status = &retainedTypeStatus{
				kind: kind,
			}
			byKind[kind] = status
		}
		status.count++
		status.size += info.Size()
		files++
		size += info.Size()
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, err
	}
	persistClassifications(dbPath, updateRetainedTypeSQL, retainedClassifierVersion, classified)

	statuses := make([]retainedTypeStatus, 0, len(byKind))
	for _, status := range byKind {
		statuses = append(statuses, *status)
	}
	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].size != statuses[j].size {
			return statuses[i].size > statuses[j].size
		}
		return statuses[i].kind.label() < statuses[j].kind.label()
	})
	return files, size, statuses, nil
}


func retainedFileKind(path string) retainedTypeKind {
	switch filepath.Ext(path) {
	case ".a":
		return retainedTypeExportArchive
	case ".go":
		data, err := os.ReadFile(path)
		if err != nil {
			return retainedTypeOther
		}
		if kind, ok := retainedGeneratedSourceKind(data); ok {
			return kind
		}
	}
	return retainedTypeOther
}

func readLiveStatus(liveRoot string) (int64, int64, error) {
	entries, err := os.ReadDir(liveRoot)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read live dir: %w", err)
	}

	var active, inactive int64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runLock := flock.New(filepath.Join(liveRoot, entry.Name(), "run.lock"))
		locked, err := runLock.TryLock()
		if err != nil {
			_ = runLock.Close()
			// A concurrent prune may reclaim the run dir between the readdir
			// and the lock attempt; a run that no longer exists counts as
			// neither active nor inactive.
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, 0, fmt.Errorf("try lock live run %s: %w", entry.Name(), err)
		}
		if !locked {
			_ = runLock.Close()
			active++
			continue
		}
		if err := runLock.Unlock(); err != nil {
			_ = runLock.Close()
			return 0, 0, fmt.Errorf("unlock live run: %w", err)
		}
		if err := runLock.Close(); err != nil {
			return 0, 0, fmt.Errorf("close live run lock: %w", err)
		}
		inactive++
	}
	return active, inactive, nil
}

func (kind retainedTypeKind) label() string {
	switch kind {
	case retainedTypeExportArchive:
		return "Export archives"
	case retainedTypeGeneratedCgoSource:
		return "Generated cgo sources"
	case retainedTypeGeneratedTestmain:
		return "Generated test mains"
	default:
		return "Other retained files"
	}
}
