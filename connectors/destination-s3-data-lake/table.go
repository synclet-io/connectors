package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// maxCommitAttempts is the first commit and four retries after
	// conflicts.
	maxCommitAttempts = 5
	minCommitBackoff  = 100 * time.Millisecond
	maxCommitBackoff  = 2 * time.Second
	formatVersion     = "2"
)

// errCommitUnknown marks a commit the catalog may have applied: its files
// must stay.
var errCommitUnknown = errors.New("commit state unknown")

// streamTable is a stream's loaded table and how the stream's columns map
// onto the table's Arrow schema.
type streamTable struct {
	plan   *streamPlan
	lake   *lake
	tbl    *table.Table
	schema *arrow.Schema
	// sources holds, per Arrow field, the index of the plan column written
	// to it, or -1 for a table column the catalog lacks, written null.
	sources []int
	// pkFields are the Arrow field indices of the primary key in schema
	// order, pkIDs their Iceberg field IDs.
	pkFields []int
	pkIDs    []int
	// cursorField is the Arrow field index of the cursor, or -1.
	cursorField int
}

func (s *streamTable) name() string {
	return s.lake.displayNamespace() + "." + s.plan.table
}

// prepareTable loads or creates the stream's table and adds the columns the
// table lacks in one schema commit.
func prepareTable(ctx context.Context, l *lake, p *streamPlan) (*streamTable, error) {
	s := &streamTable{plan: p, lake: l}
	tbl, found, err := l.loadTable(ctx, p.table)
	if err != nil {
		return nil, err
	}
	if !found {
		tbl, err = createTable(ctx, l, p)
		if err != nil {
			return nil, err
		}
	} else {
		added, err := evolve(tbl.Schema(), p.columns)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", s.name(), err)
		}
		s.tbl = tbl
		if len(added) > 0 {
			err := s.commit(ctx, "add columns", func(_ context.Context, tx *table.Transaction, _ *table.Table) error {
				us := tx.UpdateSchema(true, false)
				for _, c := range added {
					us.AddColumn([]string{c.name}, c.typ, "", false, nil)
				}
				return us.Commit()
			})
			if err != nil {
				return nil, err
			}
			tbl = s.tbl
		}
	}
	s.tbl = tbl
	if p.mode == synclet.DestinationSyncModeAppendDedup && tbl.Metadata().Version() < 2 {
		return nil, fmt.Errorf("table %s is format v%d; append_dedup needs equality deletes, which need format v2 or later", s.name(), tbl.Metadata().Version())
	}
	if err := s.bind(); err != nil {
		return nil, err
	}
	return s, nil
}

func createTable(ctx context.Context, l *lake, p *streamPlan) (*table.Table, error) {
	loc := l.cfg.tableLocation(l.namespace, p.table)
	tbl, err := l.cat.CreateTable(ctx, l.ident(p.table), newSchema(p.columns),
		catalog.WithLocation(loc),
		catalog.WithProperties(iceberg.Properties{table.PropertyFormatVersion: formatVersion}))
	switch {
	case errors.Is(err, catalog.ErrNoSuchNamespace):
		return nil, fmt.Errorf("namespace %q does not exist; create it in the catalog", l.displayNamespace())
	case errors.Is(err, catalog.ErrTableAlreadyExists):
		t, found, lerr := l.loadTable(ctx, p.table)
		if lerr != nil {
			return nil, lerr
		}
		if !found {
			return nil, fmt.Errorf("create table %s.%s: %w", l.displayNamespace(), p.table, err)
		}
		if _, eerr := evolve(t.Schema(), p.columns); eerr != nil {
			return nil, fmt.Errorf("table %s.%s: %w", l.displayNamespace(), p.table, eerr)
		}
		return t, nil
	case err != nil:
		return nil, fmt.Errorf("create table %s.%s at %s: %w", l.displayNamespace(), p.table, loc, err)
	}
	return l.wrap(tbl), nil
}

// bind maps the table's current schema onto the stream's columns.
func (s *streamTable) bind() error {
	sc := s.tbl.Schema()
	arrowSchema, err := table.SchemaToArrowSchema(sc, nil, true, false)
	if err != nil {
		return fmt.Errorf("table %s: arrow schema: %w", s.name(), err)
	}
	s.schema = arrowSchema
	s.sources = make([]int, len(sc.Fields()))
	s.pkFields, s.pkIDs, s.cursorField = nil, nil, -1
	for i, f := range sc.Fields() {
		idx, ok := s.plan.byName[f.Name]
		if !ok {
			s.sources[i] = -1
			continue
		}
		s.sources[i] = idx
		if slices.Contains(s.plan.pk, f.Name) {
			s.pkFields = append(s.pkFields, i)
			s.pkIDs = append(s.pkIDs, f.ID)
		}
		if f.Name == s.plan.cursor {
			s.cursorField = i
		}
	}
	return nil
}

// commit applies one change to the table in a transaction. A conflict with
// another writer reloads the table and rebuilds the change on it, up to
// maxCommitAttempts in all; an error wrapping errCommitUnknown means the
// catalog may have applied the change.
func (s *streamTable) commit(ctx context.Context, what string, build func(context.Context, *table.Transaction, *table.Table) error) error {
	tbl := s.tbl
	for attempt := 1; ; attempt++ {
		tx := tbl.NewTransaction()
		if err := build(ctx, tx, tbl); err != nil {
			return fmt.Errorf("stream %q: %s on %s: %w", s.plan.desc.Name, what, s.name(), err)
		}
		next, err := tx.Commit(ctx)
		if err == nil {
			s.tbl = s.lake.wrap(next)
			return nil
		}
		if !errors.Is(err, table.ErrCommitFailed) {
			return fmt.Errorf("stream %q: %s on %s: %w: %w", s.plan.desc.Name, what, s.name(), errCommitUnknown, err)
		}
		if attempt == maxCommitAttempts {
			return fmt.Errorf("stream %q: commit to %s failed after %d conflicts: %w", s.plan.desc.Name, s.name(), attempt, err)
		}
		synclet.Logger(ctx).Warn(fmt.Sprintf("stream %q: commit to %s conflicted (attempt %d of %d), retrying: %v",
			s.plan.desc.Name, s.name(), attempt, maxCommitAttempts, err))
		if err := sleep(ctx, backoff(attempt)); err != nil {
			return err
		}
		fresh, found, err := s.lake.loadTable(ctx, s.plan.table)
		if err != nil {
			return fmt.Errorf("stream %q: refresh %s after a commit conflict: %w", s.plan.desc.Name, s.name(), err)
		}
		if !found {
			return fmt.Errorf("stream %q: table %s was dropped during the sync", s.plan.desc.Name, s.name())
		}
		tbl = fresh
	}
}

func backoff(attempt int) time.Duration {
	d := minCommitBackoff << (attempt - 1)
	return min(d, maxCommitBackoff)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}

// removeFiles deletes uncommitted files, best-effort, on a context of its
// own, since the sync's may be cancelled.
func (s *streamTable) removeFiles(ctx context.Context, files []iceberg.DataFile) {
	if len(files) == 0 {
		return
	}
	log := synclet.Logger(ctx)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	fs, err := s.tbl.FS(ctx)
	if err != nil {
		log.Warn(fmt.Sprintf("stream %q: remove uncommitted files: %v", s.plan.desc.Name, err))
		return
	}
	for _, f := range files {
		if err := fs.Remove(f.FilePath()); err != nil {
			log.Warn(fmt.Sprintf("stream %q: remove uncommitted file %s: %v", s.plan.desc.Name, f.FilePath(), err))
		}
	}
}

// isUnknown reports whether a commit error leaves the commit state unknown.
func isUnknown(err error) bool {
	return errors.Is(err, errCommitUnknown) || errors.Is(err, rest.ErrCommitStateUnknown)
}
