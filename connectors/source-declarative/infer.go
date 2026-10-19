package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"

	"source-declarative/internal/engine"
)

const inferUsage = "usage: infer --config FILE [--streams a,b] [--limit 1..1000, default 100] [--pages 1..10, default 3]"

// runInfer runs the builder's infer command and returns the exit status: 0
// when the manifest and config load, 1 when the command failed, 2 on a usage
// error.
func runInfer(ctx context.Context, src synclet.Source[json.RawMessage], args []string, stdout, stderr io.Writer) int {
	req, err := parseInfer(args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%v\n%s\n", err, inferUsage)
		return 2
	}
	if err := engine.Infer(ctx, src, req, stdout); err != nil {
		return 1
	}
	return 0
}

func parseInfer(args []string) (engine.InferRequest, error) {
	fs := flag.NewFlagSet("infer", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	config := fs.String("config", "", "")
	streams := fs.String("streams", "", "")
	limit := fs.Int("limit", 100, "")
	pages := fs.Int("pages", 3, "")
	if err := fs.Parse(args); err != nil {
		return engine.InferRequest{}, err
	}
	switch {
	case fs.NArg() > 0:
		return engine.InferRequest{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	case *config == "":
		return engine.InferRequest{}, errors.New("--config is required")
	case *limit < 1 || *limit > 1000:
		return engine.InferRequest{}, fmt.Errorf("--limit %d is outside 1..1000", *limit)
	case *pages < 1 || *pages > 10:
		return engine.InferRequest{}, fmt.Errorf("--pages %d is outside 1..10", *pages)
	}
	req := engine.InferRequest{Limit: *limit, Pages: *pages}
	if *streams != "" {
		for _, s := range strings.Split(*streams, ",") {
			if s = strings.TrimSpace(s); s == "" {
				return engine.InferRequest{}, fmt.Errorf("--streams %q names an empty stream", *streams)
			}
			req.Streams = append(req.Streams, s)
		}
	}
	b, err := os.ReadFile(*config)
	if err != nil {
		return engine.InferRequest{}, fmt.Errorf("--config: %w", err)
	}
	req.Config = b
	return req, nil
}
