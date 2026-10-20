package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

// partPlaceholder stands for {part} in a file's group key; rendered values
// are path-escaped, so it never occurs in them.
const partPlaceholder = "{part}"

// streamTarget is one configured stream and the keys this sync wrote for it.
type streamTarget struct {
	desc    synclet.StreamDescriptor
	mode    synclet.DestinationSyncMode
	layout  *streamLayout
	prefix  string
	written map[string]bool
}

// openFile is an output file being encoded into a temporary file.
type openFile struct {
	target   *streamTarget
	group    string
	key      string
	tmp      *os.File
	fw       fileWriter
	records  int
	lastUsed uint64
}

// syncWriter routes records into files, rotates and uploads them, and on
// completion removes the previous objects of overwrite streams.
type syncWriter struct {
	store      objectStore
	format     Format
	tmpl       *pathTemplate
	keyPrefix  string
	start      time.Time
	runID      string
	maxBytes   int64
	maxRecords int
	maxOpen    int
	tmpDir     string

	targets map[synclet.StreamDescriptor]*streamTarget
	open    map[string]*openFile
	seq     int
	tick    uint64
}

func newSyncWriter(cfg Config, catalog *synclet.ConfiguredCatalog, store objectStore, start time.Time, tmpDir string) (*syncWriter, error) {
	tmpl, err := parsePathTemplate(cfg.PathTemplate)
	if err != nil {
		return nil, fmt.Errorf("path_template: %w", err)
	}
	w := &syncWriter{
		store:      store,
		format:     cfg.Format,
		tmpl:       tmpl,
		keyPrefix:  cfg.keyPrefix(),
		start:      start,
		runID:      strings.ReplaceAll(uuid.NewString(), "-", "")[:12],
		maxBytes:   int64(cfg.FileSizeMB) << 20,
		maxRecords: cfg.MaxRecordsPerFile,
		maxOpen:    maxOpenFilesPerWrite,
		tmpDir:     tmpDir,
		targets:    map[synclet.StreamDescriptor]*streamTarget{},
		open:       map[string]*openFile{},
	}
	for _, cs := range catalog.Streams {
		switch cs.DestinationSyncMode {
		case synclet.DestinationSyncModeAppend, synclet.DestinationSyncModeOverwrite:
		default:
			return nil, fmt.Errorf("stream %q: destination sync mode %q is not supported; use append or overwrite", cs.Stream.Name, cs.DestinationSyncMode)
		}
		cols, err := streamColumns(cs.Stream.JSONSchema)
		if err != nil {
			return nil, fmt.Errorf("stream %q: json_schema: %w", cs.Stream.Name, err)
		}
		desc := cs.Stream.Descriptor()
		w.targets[desc] = &streamTarget{
			desc:    desc,
			mode:    cs.DestinationSyncMode,
			layout:  newStreamLayout(cs.Stream.Name, cols),
			prefix:  w.fullKey(tmpl.streamPrefix(w.renderContext(desc))),
			written: map[string]bool{},
		}
	}
	if err := w.checkPrefixes(); err != nil {
		return nil, err
	}
	return w, nil
}

// checkPrefixes rejects an overwrite stream whose prefix contains, or is
// contained in, another stream's prefix: its cleanup would delete them.
func (w *syncWriter) checkPrefixes() error {
	for _, a := range w.sortedTargets() {
		if a.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		for _, b := range w.sortedTargets() {
			if a == b {
				continue
			}
			if strings.HasPrefix(a.prefix, b.prefix) || strings.HasPrefix(b.prefix, a.prefix) {
				return fmt.Errorf("overwrite stream %q writes under %q, which overlaps stream %q under %q; change path_template",
					a.desc.Name, a.prefix, b.desc.Name, b.prefix)
			}
		}
	}
	return nil
}

func (w *syncWriter) renderContext(desc synclet.StreamDescriptor) renderContext {
	return renderContext{namespace: desc.Namespace, stream: desc.Name, start: w.start}
}

func (w *syncWriter) fullKey(rendered string) string {
	if w.keyPrefix == "" {
		return rendered
	}
	return w.keyPrefix + "/" + rendered
}

func (w *syncWriter) sortedTargets() []*streamTarget {
	out := make([]*streamTarget, 0, len(w.targets))
	for _, t := range w.targets {
		out = append(out, t)
	}
	slices.SortFunc(out, func(a, b *streamTarget) int { return strings.Compare(a.prefix, b.prefix) })
	return out
}

// add encodes one record into the file of its stream and partition.
func (w *syncWriter) add(ctx context.Context, msg *synclet.RecordMessage) error {
	desc := synclet.StreamDescriptor{Name: msg.Stream, Namespace: msg.Namespace}
	target, ok := w.targets[desc]
	if !ok {
		return fmt.Errorf("record for stream %q (namespace %q) that is not in the configured catalog", msg.Stream, msg.Namespace)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(msg.Data, &fields); err != nil || fields == nil {
		return fmt.Errorf("stream %q: record data is not a JSON object: %s", msg.Stream, msg.Data)
	}
	group, err := w.tmpl.render(w.renderContext(desc), fields, partPlaceholder)
	if err != nil {
		return fmt.Errorf("stream %q: %w", msg.Stream, err)
	}
	f, err := w.file(ctx, target, group)
	if err != nil {
		return err
	}
	rec := &record{id: uuid.NewString(), extractedAt: msg.EmittedAt, data: msg.Data, fields: fields}
	if err := f.fw.write(rec); err != nil {
		return err
	}
	f.records++
	if (w.maxRecords > 0 && f.records >= w.maxRecords) || f.fw.size() >= w.maxBytes {
		return w.finish(ctx, f)
	}
	return nil
}

// file returns the open file of a group, opening one and closing the least
// recently written file when maxOpen files are open.
func (w *syncWriter) file(ctx context.Context, target *streamTarget, group string) (*openFile, error) {
	w.tick++
	id := target.prefix + "\x00" + group
	if f, ok := w.open[id]; ok {
		f.lastUsed = w.tick
		return f, nil
	}
	if len(w.open) >= w.maxOpen {
		var lru *openFile
		for _, f := range w.open {
			if lru == nil || f.lastUsed < lru.lastUsed {
				lru = f
			}
		}
		if err := w.finish(ctx, lru); err != nil {
			return nil, err
		}
	}
	tmp, err := os.CreateTemp(w.tmpDir, "part-*")
	if err != nil {
		return nil, fmt.Errorf("creating temporary file: %w", err)
	}
	fw, err := newFileWriter(w.format, target.layout, tmp)
	if err != nil {
		return nil, multierr.Combine(fmt.Errorf("stream %q: %w", target.desc.Name, err), tmp.Close(), os.Remove(tmp.Name()))
	}
	part := strconv.FormatInt(w.start.UnixMilli(), 10) + "_" + w.runID + "_" + strconv.Itoa(w.seq)
	w.seq++
	f := &openFile{
		target:   target,
		group:    group,
		key:      w.fullKey(strings.Replace(group, partPlaceholder, part, 1)) + w.format.extension(),
		tmp:      tmp,
		fw:       fw,
		lastUsed: w.tick,
	}
	w.open[id] = f
	return f, nil
}

// finish completes a file, uploads it and records its key.
func (w *syncWriter) finish(ctx context.Context, f *openFile) (rerr error) {
	delete(w.open, f.target.prefix+"\x00"+f.group)
	defer multierr.AppendInvoke(&rerr, multierr.Invoke(func() error { return os.Remove(f.tmp.Name()) }))
	defer multierr.AppendInvoke(&rerr, multierr.Close(f.tmp))
	if err := f.fw.close(); err != nil {
		return fmt.Errorf("stream %q: finishing %s: %w", f.target.desc.Name, f.key, err)
	}
	if _, err := f.tmp.Seek(0, 0); err != nil {
		return err
	}
	if err := w.store.put(ctx, f.key, f.tmp); err != nil {
		return err
	}
	f.target.written[f.key] = true
	return nil
}

// flush uploads every open file.
func (w *syncWriter) flush(ctx context.Context) error {
	files := make([]*openFile, 0, len(w.open))
	for _, f := range w.open {
		files = append(files, f)
	}
	slices.SortFunc(files, func(a, b *openFile) int { return strings.Compare(a.key, b.key) })
	for _, f := range files {
		if err := w.finish(ctx, f); err != nil {
			return err
		}
	}
	return nil
}

// complete uploads every open file, then deletes the objects of each
// overwrite stream that this sync did not write.
func (w *syncWriter) complete(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	for _, t := range w.sortedTargets() {
		if t.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		keys, err := w.store.list(ctx, t.prefix)
		if err != nil {
			return fmt.Errorf("stream %q: %w", t.desc.Name, err)
		}
		var stale []string
		for _, k := range keys {
			if !t.written[k] {
				stale = append(stale, k)
			}
		}
		if err := w.store.delete(ctx, stale); err != nil {
			return fmt.Errorf("stream %q: removing previous objects: %w", t.desc.Name, err)
		}
	}
	return nil
}

// holdsState reports whether a state must wait for complete: it belongs to
// an overwrite stream, or has no stream while the catalog has one.
func (w *syncWriter) holdsState(st synclet.StateMessage) bool {
	if st.Type == synclet.StateTypeStream && st.Stream != nil {
		t, ok := w.targets[st.Stream.StreamDescriptor]
		return ok && t.mode == synclet.DestinationSyncModeOverwrite
	}
	for _, t := range w.targets {
		if t.mode == synclet.DestinationSyncModeOverwrite {
			return true
		}
	}
	return false
}

// abort discards every open file without uploading it.
func (w *syncWriter) abort() error {
	var errs error
	for id, f := range w.open {
		errs = multierr.Combine(errs, f.tmp.Close(), os.Remove(f.tmp.Name()))
		delete(w.open, id)
	}
	return errs
}
