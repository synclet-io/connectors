package main

import (
	"archive/zip"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	exportPath = "2/export"
	// hourLayout is the start and end format of the Export API.
	hourLayout = "20060102T15"
	// eventTimeLayout is how Amplitude writes event timestamps, in UTC.
	eventTimeLayout = "2006-01-02 15:04:05.999999"
	// exportDelay is how long after an hour Amplitude may still add events
	// uploaded in that hour to the export. Only hours that ended at least
	// this long ago are read, so a saved hour is never read again.
	exportDelay = 2 * time.Hour
)

// eventsState is the STREAM state of events: every event uploaded before
// server_upload_time, an RFC 3339 hour, has been read.
type eventsState struct {
	ServerUploadTime string `json:"server_upload_time"`
}

// readEvents requests the Export API in windows of request_time_range hours
// from the start hour up to the last hour that ended exportDelay ago. The
// start is the hour of start_date or, in an incremental read, the saved hour
// if that is later. An incremental read saves the end of each window after
// its events.
func (s Source) readEvents(ctx context.Context, cfg Config, c *apiClient, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	start, err := cfg.start()
	if err != nil {
		return synclet.ConfigError(err)
	}
	from := start.Truncate(time.Hour)
	if incremental {
		var st eventsState
		if err := savedState(state, desc, &st); err != nil {
			return err
		}
		if st.ServerUploadTime != "" {
			saved, err := parseStateTime(st.ServerUploadTime)
			if err != nil {
				return err
			}
			if saved.After(from) {
				from = saved
			}
		}
	}
	end := s.clock().UTC().Add(-exportDelay).Truncate(time.Hour)
	step := time.Duration(cfg.timeRange()) * time.Hour
	for from.Before(end) {
		to := from.Add(step)
		if to.After(end) {
			to = end
		}
		if err := readEventWindow(ctx, c, desc, from, to, em); err != nil {
			return err
		}
		from = to
		if incremental {
			if err := em.StreamState(desc, eventsState{ServerUploadTime: from.Format(time.RFC3339)}); err != nil {
				return err
			}
		}
	}
	return nil
}

// parseStateTime returns the hour of a saved server_upload_time in UTC. It
// is an RFC 3339 time or Amplitude's event time layout; a time inside an hour
// resumes at the start of that hour.
func parseStateTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		if t, err = time.Parse(eventTimeLayout, s); err != nil {
			return time.Time{}, fmt.Errorf("state server_upload_time %q is not an RFC 3339 time", s)
		}
	}
	return t.UTC().Truncate(time.Hour), nil
}

// readEventWindow emits the events uploaded in [from, to). A 404 means the
// window has no events. A 400 is what the API answers when the export would
// be too large, so the window is split in half and each half requested; a
// 400 for a single hour fails.
func readEventWindow(ctx context.Context, c *apiClient, desc synclet.StreamDescriptor, from, to time.Time, em *synclet.Emitter) error {
	last := to.Add(-time.Hour)
	q := url.Values{}
	q.Set("start", from.Format(hourLayout))
	q.Set("end", last.Format(hourLayout))
	f, err := c.download(ctx, exportPath, q)
	switch {
	case hasStatus(err, http.StatusNotFound):
		return nil
	case hasStatus(err, http.StatusBadRequest) && to.Sub(from) > time.Hour:
		hours := int(to.Sub(from) / time.Hour)
		mid := from.Add(time.Duration(hours/2) * time.Hour)
		synclet.Logger(ctx).Info(fmt.Sprintf("export of %s to %s was rejected (HTTP 400); splitting it at %s",
			from.Format(hourLayout), last.Format(hourLayout), mid.Format(hourLayout)))
		if err := readEventWindow(ctx, c, desc, from, mid, em); err != nil {
			return err
		}
		return readEventWindow(ctx, c, desc, mid, to, em)
	case err != nil:
		return fmt.Errorf("export %s to %s: %w", from.Format(hourLayout), last.Format(hourLayout), err)
	}
	defer discard(f)
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if err := emitArchive(f, info.Size(), desc, em); err != nil {
		return fmt.Errorf("export %s to %s: %w", from.Format(hourLayout), last.Format(hourLayout), err)
	}
	return nil
}

// emitArchive emits the events of an export archive: a zip of gzipped JSON
// files, one event per line, read in hour order.
func emitArchive(r io.ReaderAt, size int64, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	zr, err := zip.NewReader(r, size)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	files := slices.DeleteFunc(slices.Clone(zr.File), func(f *zip.File) bool { return f.FileInfo().IsDir() })
	slices.SortFunc(files, func(a, b *zip.File) int { return compareArchiveNames(a.Name, b.Name) })
	for _, f := range files {
		if err := emitArchiveFile(f, desc, em); err != nil {
			return fmt.Errorf("%s: %w", f.Name, err)
		}
	}
	return nil
}

// archiveName splits an export file name, <project>/<project>_<YYYY-MM-DD>_<H>#<N>.json.gz,
// into its day, hour and part; ok is false for another name.
func archiveName(name string) (day string, hour, part int, ok bool) {
	base := path.Base(name)
	base = strings.TrimSuffix(strings.TrimSuffix(base, ".gz"), ".json")
	fields := strings.Split(base, "_")
	if len(fields) < 3 {
		return "", 0, 0, false
	}
	h, n, found := strings.Cut(fields[len(fields)-1], "#")
	if !found {
		return "", 0, 0, false
	}
	hour, err1 := strconv.Atoi(h)
	part, err2 := strconv.Atoi(n)
	if err1 != nil || err2 != nil {
		return "", 0, 0, false
	}
	return fields[len(fields)-2], hour, part, true
}

// compareArchiveNames orders export files by day, hour and part, and names
// of another form after them by name.
func compareArchiveNames(a, b string) int {
	da, ha, pa, oka := archiveName(a)
	db, hb, pb, okb := archiveName(b)
	switch {
	case oka && okb:
		return cmp.Or(strings.Compare(da, db), cmp.Compare(ha, hb), cmp.Compare(pa, pb), strings.Compare(a, b))
	case oka:
		return -1
	case okb:
		return 1
	}
	return strings.Compare(a, b)
}

func emitArchiveFile(f *zip.File, desc synclet.StreamDescriptor, em *synclet.Emitter) (rerr error) {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer func() { rerr = errors.Join(rerr, rc.Close()) }()
	var r io.Reader = rc
	if strings.HasSuffix(f.Name, ".gz") {
		gz, err := gzip.NewReader(rc)
		if err != nil {
			return err
		}
		defer func() { rerr = errors.Join(rerr, gz.Close()) }()
		r = gz
	}
	dec := json.NewDecoder(r)
	dec.UseNumber()
	for n := 1; ; n++ {
		var ev map[string]any
		if err := dec.Decode(&ev); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("event %d: %w", n, err)
		}
		normalizeEventTimes(ev)
		if err := em.Record(desc, ev); err != nil {
			return err
		}
	}
}

// normalizeEventTimes rewrites the timestamp fields of an event from
// Amplitude's "YYYY-MM-DD HH:MM:SS.ffffff" to RFC 3339 in UTC. A value in
// another form is left as it is.
func normalizeEventTimes(ev map[string]any) {
	for _, k := range eventTimestampFields {
		s, ok := ev[k].(string)
		if !ok {
			continue
		}
		if t, err := time.Parse(eventTimeLayout, s); err == nil {
			ev[k] = t.Format(time.RFC3339Nano)
		}
	}
}
