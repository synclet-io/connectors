package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// blockWalker reads the blocks under one page, depth first.
type blockWalker struct {
	r      *reader
	desc   synclet.StreamDescriptor
	def    streamDef
	pageID string
	since  time.Time
	count  int
}

// children emits the children of parent at depth and recurses into each
// child that has children of its own, except child pages and child
// databases, which are read as pages, and a synced block that copies
// another, whose children belong to the original's page. Blocks edited
// before since are walked but not emitted. A page with more blocks than
// max_blocks_per_page, or a block at max_block_depth with children, fails
// the read.
func (w *blockWalker) children(ctx context.Context, parent string, depth int) error {
	maxDepth, maxBlocks := w.r.cfg.maxBlockDepth(), w.r.cfg.maxBlocksPerPage()
	return w.r.c.getAll(ctx, "blocks/"+parent+"/children", func(raw json.RawMessage) (bool, error) {
		b, err := decodeObject(raw, "block")
		if err != nil {
			return false, fmt.Errorf("page %s: %w", w.pageID, err)
		}
		w.count++
		if w.count > maxBlocks {
			return false, synclet.ConfigError(fmt.Errorf("page %s has more than max_blocks_per_page (%d) blocks; block %s is over the limit: raise max_blocks_per_page", w.pageID, maxBlocks, b.id))
		}
		var kind string
		if err := json.Unmarshal(b.obj["type"], &kind); err != nil || kind == "" {
			return false, fmt.Errorf("page %s: block %s has no type", w.pageID, b.id)
		}
		var hasChildren bool
		if v, ok := b.obj["has_children"]; ok {
			if err := json.Unmarshal(v, &hasChildren); err != nil {
				return false, fmt.Errorf("page %s: block %s: has_children: %w", w.pageID, b.id, err)
			}
		}
		if !b.edited.Before(w.since) {
			rec := w.def.record(b.obj)
			pageID, err := json.Marshal(w.pageID)
			if err != nil {
				return false, err
			}
			rec["page_id"] = pageID
			rec["depth"] = json.RawMessage(strconv.Itoa(depth))
			if c, ok := b.obj[kind]; ok {
				rec["content"] = c
			}
			if err := w.r.em.Record(w.desc, rec); err != nil {
				return false, err
			}
		}
		if !hasChildren || !descends(kind, b.obj[kind]) {
			return true, nil
		}
		if depth >= maxDepth {
			return false, synclet.ConfigError(fmt.Errorf("page %s: block %s at depth %d has children below max_block_depth (%d): raise max_block_depth", w.pageID, b.id, depth, maxDepth))
		}
		return true, w.children(ctx, b.id, depth+1)
	})
}

// descends reports whether the children of a block of kind with content
// are read under its page.
func descends(kind string, content json.RawMessage) bool {
	switch kind {
	case "child_page", "child_database":
		return false
	case "synced_block":
		var sb struct {
			SyncedFrom json.RawMessage `json:"synced_from"`
		}
		if json.Unmarshal(content, &sb) != nil {
			return true
		}
		return len(sb.SyncedFrom) == 0 || bytes.Equal(sb.SyncedFrom, []byte("null"))
	}
	return true
}
