package main

import (
	"fmt"
	"os/exec"
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
