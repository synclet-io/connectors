// Command manifest-lint loads a manifest connector's directory and prints
// every problem of its manifest with its path. With -spec it prints the
// connector specification the manifest maps to instead.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"source-declarative/internal/manifest"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("manifest-lint", flag.ContinueOnError)
	fs.SetOutput(stderr)
	spec := fs.Bool("spec", false, "print the spec JSON of the manifest")
	fs.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "usage: manifest-lint [-spec] <dir>")
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
	m, err := manifest.Load(fs.Arg(0))
	if err != nil {
		for _, e := range manifest.List(err) {
			_, _ = fmt.Fprintln(stderr, e)
		}
		return 1
	}
	if !*spec {
		return 0
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m.ConnectorSpec()); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
