package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	releasePleaseConfig   = "release-please-config.json"
	releasePleaseManifest = ".release-please-manifest.json"
)

type releasePleasePackage struct {
	Component      string `json:"component"`
	InitialVersion string `json:"initial-version"`
	ExtraFiles     []struct {
		Type     string `json:"type"`
		Path     string `json:"path"`
		JSONPath string `json:"jsonpath"`
	} `json:"extra-files"`
}

// checkReleasePlease requires one release-please package per connector,
// released under the tag <dir>-v<version> with its meta.json version bumped,
// and a manifest whose versions equal the meta.json versions. A connector
// missing from the manifest is not released yet: release-please opens its
// first release at initial-version, so meta.json must declare that version.
func checkReleasePlease(root string, connectors []Connector) error {
	var config struct {
		InitialVersion string                          `json:"initial-version"`
		Packages       map[string]releasePleasePackage `json:"packages"`
	}
	if err := readJSON(filepath.Join(root, releasePleaseConfig), &config); err != nil {
		return err
	}

	var manifest map[string]string
	if err := readJSON(filepath.Join(root, releasePleaseManifest), &manifest); err != nil {
		return err
	}

	var errs []error

	known := map[string]bool{}

	for _, c := range connectors {
		path := connectorsDir + "/" + c.Dir
		known[path] = true

		pkg, ok := config.Packages[path]
		if !ok {
			errs = append(errs, fmt.Errorf("%s: no package %q", releasePleaseConfig, path))

			continue
		}

		if pkg.Component != c.Dir {
			errs = append(errs, fmt.Errorf("%s: package %q has component %q, want %q", releasePleaseConfig, path, pkg.Component, c.Dir))
		}

		bumpsMeta := false

		for _, f := range pkg.ExtraFiles {
			if f.Type == "json" && f.Path == "meta.json" && f.JSONPath == "$.version" {
				bumpsMeta = true
			}
		}

		if !bumpsMeta {
			errs = append(errs, fmt.Errorf("%s: package %q must list meta.json ($.version) in extra-files", releasePleaseConfig, path))
		}

		v, released := manifest[path]

		switch {
		case released && v != c.Meta.Version:
			errs = append(errs, fmt.Errorf("%s: %q is %q but %s/meta.json has version %s", releasePleaseManifest, path, v, path, c.Meta.Version))
		case !released:
			initial := pkg.InitialVersion
			if initial == "" {
				initial = config.InitialVersion
			}

			if initial == "" {
				errs = append(errs, fmt.Errorf("%s: %q is not released and %s sets no initial-version", releasePleaseManifest, path, releasePleaseConfig))
			} else if initial != c.Meta.Version {
				errs = append(errs, fmt.Errorf("%s: %q is not released, so %s/meta.json must have the initial-version %s, not %s", releasePleaseManifest, path, path, initial, c.Meta.Version))
			}
		}
	}

	for path := range config.Packages {
		if !known[path] {
			errs = append(errs, fmt.Errorf("%s: package %q is not a connector directory", releasePleaseConfig, path))
		}
	}

	for path := range manifest {
		if !known[path] {
			errs = append(errs, fmt.Errorf("%s: %q is not a connector directory", releasePleaseManifest, path))
		}
	}

	return errors.Join(errs...)
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("parsing %s: %w", path, err)
	}

	return nil
}
