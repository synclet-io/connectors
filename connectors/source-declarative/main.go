// Command source-declarative runs a declarative HTTP source: the manifest
// baked into the image at $SYNCLET_MANIFEST_DIR/manifest.yaml (default
// /manifest), or, on the generic image without one, the manifest each
// config carries in __manifest. Besides the protocol commands, the generic
// image answers the builder's infer command.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/cli"

	"source-declarative/internal/engine"
	"source-declarative/internal/manifest"
)

const defaultManifestDir = "/manifest"

func main() {
	src := resolve(os.Getenv("SYNCLET_MANIFEST_DIR"))
	if len(os.Args) > 1 && os.Args[1] == "infer" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		code := runInfer(ctx, src, os.Args[2:], os.Stdout, os.Stderr)
		stop()
		os.Exit(code)
	}
	cli.RunSource(src)
}

// resolve returns the source of the baked manifest in dir, or the generic
// source when dir has no manifest.yaml.
func resolve(dir string) synclet.Source[json.RawMessage] {
	if dir == "" {
		dir = defaultManifestDir
	}
	path := filepath.Join(dir, manifest.FileName)
	_, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return engine.NewGeneric(engine.Options{})
	case err != nil:
		return engine.Failed(fmt.Errorf("baked manifest %s: %w", path, err))
	}
	m, err := manifest.Load(dir)
	if err != nil {
		return engine.Failed(fmt.Errorf("baked manifest %s: %w", path, err))
	}
	return engine.New(m, engine.Options{})
}
