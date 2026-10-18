package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

const (
	defaultPollInterval = 5 * time.Second
	// cancelTimeout bounds the bulkOperationCancel sent after a read was
	// cancelled.
	cancelTimeout = 30 * time.Second
)

const (
	bulkRunDoc = `mutation BulkRun($query: String!) { bulkOperationRunQuery(query: $query) { bulkOperation { id status } userErrors { field message code } } }`
	// bulkPollDoc is the poll of one operation by id.
	bulkPollDoc   = `query BulkPoll($id: ID!) { bulkOperation(id: $id) { id status errorCode objectCount rootObjectCount url partialDataUrl } }`
	bulkCancelDoc = `mutation BulkCancel($id: ID!) { bulkOperationCancel(id: $id) { bulkOperation { id status } userErrors { field message } } }`
)

// bulkOperation is a BulkOperation as the poll reads it.
type bulkOperation struct {
	ID              string  `json:"id"`
	Status          string  `json:"status"`
	ErrorCode       *string `json:"errorCode"`
	ObjectCount     string  `json:"objectCount"`
	RootObjectCount string  `json:"rootObjectCount"`
	URL             *string `json:"url"`
	PartialDataURL  *string `json:"partialDataUrl"`
}

// bulkError is a bulk operation that did not deliver all of its objects.
type bulkError struct {
	label  string
	op     *bulkOperation
	reason string
}

func (e *bulkError) Error() string {
	s := e.label + ": bulk operation"
	if e.op != nil {
		s += " " + e.op.ID + " status " + e.op.Status
		if e.op.ErrorCode != nil {
			s += " errorCode " + *e.op.ErrorCode
		}
	}
	return s + ": " + e.reason
}

// bulkResult is the verified JSONL output of a completed bulk operation,
// kept in a temporary file until it is read.
type bulkResult struct {
	file *os.File
}

func (r *bulkResult) Close() error {
	if r.file == nil {
		return nil
	}
	return multierr.Combine(r.file.Close(), os.Remove(r.file.Name()))
}

// runBulk runs query as a bulk operation, waits for it and downloads and
// verifies its output. The operation fails unless it completed without
// partial data and its output holds exactly objectCount well-formed
// lines, rootObjectCount of them roots and every other one a child of the
// root before it. label names the stream and window in errors.
func (s Source) runBulk(ctx context.Context, c *apiClient, label, query string) (*bulkResult, error) {
	var run struct {
		BulkOperationRunQuery struct {
			BulkOperation *struct {
				ID string `json:"id"`
			} `json:"bulkOperation"`
			UserErrors []struct {
				Field   []string `json:"field"`
				Message string   `json:"message"`
				Code    string   `json:"code"`
			} `json:"userErrors"`
		} `json:"bulkOperationRunQuery"`
	}
	if err := c.query(ctx, "BulkRun", bulkRunDoc, map[string]any{"query": query}, &run); err != nil {
		return nil, fmt.Errorf("%s: start bulk operation: %w", label, err)
	}
	if ue := run.BulkOperationRunQuery.UserErrors; len(ue) > 0 {
		msgs := make([]string, len(ue))
		for i, e := range ue {
			msgs[i] = strings.TrimPrefix(e.Code+": ", ": ") + e.Message
		}
		return nil, fmt.Errorf("%s: bulk operation not started: %s", label, strings.Join(msgs, "; "))
	}
	started := run.BulkOperationRunQuery.BulkOperation
	if started == nil || started.ID == "" {
		return nil, fmt.Errorf("%s: bulk operation not started: response without a bulkOperation", label)
	}
	op, err := s.poll(ctx, c, label, started.ID)
	if err != nil {
		return nil, err
	}
	return s.download(ctx, c, label, op)
}

// poll waits until the operation id finishes and returns it when it
// completed. When ctx is cancelled first, it cancels the operation.
func (s Source) poll(ctx context.Context, c *apiClient, label, id string) (*bulkOperation, error) {
	interval := s.pollInterval
	if interval == 0 {
		interval = defaultPollInterval
	}
	for {
		if err := c.wait(ctx, interval); err != nil {
			cancelBulk(ctx, c, id)
			return nil, err
		}
		var res struct {
			BulkOperation *bulkOperation `json:"bulkOperation"`
		}
		if err := c.query(ctx, "BulkPoll", bulkPollDoc, map[string]any{"id": id}, &res); err != nil {
			if ctx.Err() != nil {
				cancelBulk(ctx, c, id)
			}
			return nil, fmt.Errorf("%s: poll bulk operation %s: %w", label, id, err)
		}
		op := res.BulkOperation
		if op == nil {
			return nil, &bulkError{label: label, reason: "bulk operation " + id + " not found"}
		}
		switch op.Status {
		case "CREATED", "RUNNING":
			continue
		case "COMPLETED":
			if op.PartialDataURL != nil {
				return nil, &bulkError{label: label, op: op, reason: "completed with partial data only"}
			}
			return op, nil
		}
		reason := "did not complete"
		if op.PartialDataURL != nil {
			reason += "; its partial data is not read"
		}
		return nil, &bulkError{label: label, op: op, reason: reason}
	}
}

// cancelBulk asks Shopify to cancel operation id after ctx was cancelled;
// a failure is only logged.
func cancelBulk(ctx context.Context, c *apiClient, id string) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelTimeout)
	defer cancel()
	var res struct{}
	if err := c.query(cctx, "BulkCancel", bulkCancelDoc, map[string]any{"id": id}, &res); err != nil {
		synclet.Logger(ctx).Warn("cancel bulk operation", "id", id, "error", err.Error())
	}
}

func parseCount(label string, op *bulkOperation, name, v string) (int64, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, &bulkError{label: label, op: op, reason: fmt.Sprintf("%s %q is not a count", name, v)}
	}
	return n, nil
}

// download fetches the output of op into a temporary file and verifies it.
// The URL is signed, so the request carries no Shopify token.
func (s Source) download(ctx context.Context, c *apiClient, label string, op *bulkOperation) (_ *bulkResult, rerr error) {
	objects, err := parseCount(label, op, "objectCount", op.ObjectCount)
	if err != nil {
		return nil, err
	}
	roots, err := parseCount(label, op, "rootObjectCount", op.RootObjectCount)
	if err != nil {
		return nil, err
	}
	if op.URL == nil || *op.URL == "" {
		if objects != 0 {
			return nil, &bulkError{label: label, op: op, reason: fmt.Sprintf("no output url for %d objects", objects)}
		}
		return &bulkResult{}, nil
	}
	f, err := os.CreateTemp("", "shopify-bulk-*.jsonl")
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	res := &bulkResult{file: f}
	defer func() {
		if rerr != nil {
			rerr = multierr.Append(rerr, res.Close())
		}
	}()
	delay := c.retryDelay
	for attempt := 0; ; attempt++ {
		err = fetchOutput(ctx, c.download, *op.URL, f)
		if err == nil {
			break
		}
		var r retryable
		if ctx.Err() != nil || !errors.As(err, &r) || attempt == retries {
			return nil, &bulkError{label: label, op: op, reason: "download output: " + err.Error()}
		}
		if err := c.wait(ctx, delay); err != nil {
			return nil, err
		}
		delay *= 2
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	lines, gotRoots, err := verify(bufio.NewReader(f))
	if err != nil {
		return nil, &bulkError{label: label, op: op, reason: err.Error()}
	}
	if lines != objects || gotRoots != roots {
		return nil, &bulkError{label: label, op: op, reason: fmt.Sprintf(
			"output has %d objects, %d of them roots; the operation reports objectCount %d and rootObjectCount %d", lines, gotRoots, objects, roots)}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return res, nil
}

// fetchOutput writes the body of url to f from its start.
func fetchOutput(ctx context.Context, hc *http.Client, url string, f *os.File) (rerr error) {
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return retryable{err}
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(resp.Body))
	if resp.StatusCode != http.StatusOK {
		he := &httpError{op: "GET output", status: resp.StatusCode}
		if resp.StatusCode >= 500 {
			return retryable{he}
		}
		return he
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		return retryable{err}
	}
	return nil
}

// bulkLine is the part of an output line verify reads.
type bulkLine struct {
	ID       string `json:"id"`
	ParentID string `json:"__parentId"`
}

// verify reads JSONL output and counts its lines and roots. Every line
// must be a JSON object ending in a newline; a child must follow its root.
func verify(r *bufio.Reader) (lines, roots int64, err error) {
	var root string
	for {
		raw, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if len(raw) > 0 {
				return 0, 0, fmt.Errorf("output line %d has no final newline; the output is truncated", lines+1)
			}
			return lines, roots, nil
		}
		if err != nil {
			return 0, 0, err
		}
		lines++
		var l bulkLine
		if err := json.Unmarshal(raw, &l); err != nil {
			return 0, 0, fmt.Errorf("output line %d is not a JSON object: %w", lines, err)
		}
		switch {
		case l.ParentID == "":
			roots++
			root = l.ID
		case l.ParentID != root:
			return 0, 0, fmt.Errorf("output line %d is a child of %s, which is not the root before it (%q)", lines, l.ParentID, root)
		}
	}
}

// each calls fn with every root of the output and its children, in order.
func (r *bulkResult) each(ctx context.Context, fn func(root map[string]json.RawMessage, children []map[string]json.RawMessage) error) error {
	if r.file == nil {
		return nil
	}
	dec := json.NewDecoder(bufio.NewReader(r.file))
	var root map[string]json.RawMessage
	var children []map[string]json.RawMessage
	flush := func() error {
		if root == nil {
			return nil
		}
		return fn(root, children)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var obj map[string]json.RawMessage
		if err := dec.Decode(&obj); errors.Is(err, io.EOF) {
			return flush()
		} else if err != nil {
			return err
		}
		if _, child := obj["__parentId"]; child {
			delete(obj, "__parentId")
			children = append(children, obj)
			continue
		}
		if err := flush(); err != nil {
			return err
		}
		root, children = obj, nil
	}
}
