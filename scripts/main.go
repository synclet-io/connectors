// Command registry validates connector metadata and builds registry.json.
//
//	go run ./scripts validate
//	go run ./scripts registry [--image-prefix ghcr.io/org/repo] [--out dist/registry.json] [--released]
//	go run ./scripts release --tag <dir>-v<version>   # JSON {connector, version, tag}; fails unless meta.json has that version
//	go run ./scripts entry --connector <dir> [--image-prefix ghcr.io/org/repo]   # one registry entry as JSON
//	go run ./scripts changed --base <git-ref>   # JSON list of connectors touched since <git-ref>
//
// registry builds from the working tree; with --released it builds from each
// connector's newest release tag whose image can be pulled anonymously.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: registry <validate|registry|release|entry|changed> [flags]")
		os.Exit(2)
	}

	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	root := fs.String("root", ".", "repository root")
	base := fs.String("base", "", "git ref to diff against")
	imagePrefix := fs.String("image-prefix", "", "image repository prefix (default ghcr.io/$GITHUB_REPOSITORY)")
	out := fs.String("out", "dist/registry.json", "output file")
	tag := fs.String("tag", "", "release tag <dir>-v<version>")
	connector := fs.String("connector", "", "connector directory")
	released := fs.Bool("released", false, "build registry.json from published release tags")

	if err := fs.Parse(args); err != nil {
		return err
	}

	switch cmd {
	case "validate":
		return validate(*root)
	case "registry":
		return writeRegistry(*root, *imagePrefix, *out, *released)
	case "release":
		return release(*root, *tag)
	case "entry":
		return entry(*root, *connector, *imagePrefix)
	case "changed":
		return changed(*root, *base)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func validate(root string) error {
	connectors, err := loadConnectors(root)
	if err != nil {
		return err
	}

	for _, c := range connectors {
		fmt.Printf("ok  %s %s\n", c.Dir, c.Meta.Version)
	}

	return checkReleasePlease(root, connectors)
}

func writeRegistry(root, imagePrefix, out string, released bool) error {
	imagePrefix, err := resolveImagePrefix(imagePrefix)
	if err != nil {
		return err
	}

	var connectors []Connector
	if released {
		connectors, err = loadReleased(root, imagePrefix, newAnonymousPull())
	} else {
		connectors, err = loadConnectors(root)
	}

	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(buildRegistry(connectors, imagePrefix), "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}

	if err := os.WriteFile(out, append(data, '\n'), 0o644); err != nil {
		return err
	}

	fmt.Printf("wrote %s (%d connectors)\n", out, len(connectors))

	return nil
}

// release resolves a <dir>-v<version> tag to its connector and fails when the
// connector's meta.json does not declare that version, so a tag can only
// release the code it points at.
func release(root, tag string) error {
	if tag == "" {
		return errors.New("--tag is required")
	}

	connectors, err := loadConnectors(root)
	if err != nil {
		return err
	}

	for _, c := range connectors {
		if c.ReleaseTag() == tag {
			return printJSON(map[string]string{"connector": c.Dir, "version": c.Meta.Version, "tag": tag})
		}
	}

	for _, c := range connectors {
		if strings.HasPrefix(tag, c.Dir+"-v") {
			return fmt.Errorf("tag %s does not match %s/meta.json version %s", tag, c.Dir, c.Meta.Version)
		}
	}

	return fmt.Errorf("tag %s matches no connector directory", tag)
}

func entry(root, dir, imagePrefix string) error {
	if dir == "" {
		return errors.New("--connector is required")
	}

	imagePrefix, err := resolveImagePrefix(imagePrefix)
	if err != nil {
		return err
	}

	schema, err := compileSchema(root)
	if err != nil {
		return err
	}

	c, err := loadConnector(root, dir, schema)
	if err != nil {
		return fmt.Errorf("%s: %w", dir, err)
	}

	return printJSON(toEntry(c, imagePrefix))
}

func changed(root, base string) error {
	if base == "" {
		return errors.New("--base is required")
	}

	byDir, err := changedFiles(root, base)
	if err != nil {
		return err
	}

	dirs := []string{}

	for _, dir := range sortedKeys(byDir) {
		if _, err := os.Stat(filepath.Join(root, connectorsDir, dir)); err == nil {
			dirs = append(dirs, dir)
		}
	}

	return printJSON(dirs)
}

func printJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	fmt.Println(string(data))

	return nil
}
