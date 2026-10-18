package main

import (
	"fmt"
	"os/exec"
	"path"
	"sort"
	"strings"
)

func git(root string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = root

	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, ee.Stderr)
		}

		return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}

	return strings.TrimSpace(string(out)), nil
}

func gitTags(root string) (map[string]bool, error) {
	out, err := git(root, "tag", "--list")
	if err != nil {
		return nil, err
	}

	tags := map[string]bool{}
	for _, t := range strings.Fields(out) {
		tags[t] = true
	}

	return tags, nil
}

// changedFiles groups files changed between base and HEAD by connector directory.
func changedFiles(root, base string) (map[string][]string, error) {
	out, err := git(root, "diff", "--name-only", base, "HEAD", "--", connectorsDir+"/")
	if err != nil {
		return nil, err
	}

	byDir := map[string][]string{}

	for _, f := range strings.Fields(out) {
		parts := strings.SplitN(f, "/", 3)
		if len(parts) < 3 {
			continue
		}

		byDir[parts[1]] = append(byDir[parts[1]], parts[2])
	}

	return byDir, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// affectsImage reports whether a changed file ends up in the connector image.
// Docs, metadata and the checked-in spec do not, so they never require a version bump.
func affectsImage(file string) bool {
	switch {
	case file == "meta.json", file == "spec.json":
		return false
	case strings.HasSuffix(file, ".md"):
		return false
	case strings.HasSuffix(path.Base(file), "_test.go"):
		return false
	}

	return true
}
