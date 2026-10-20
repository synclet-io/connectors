package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"registry/scripts/apiwatch"
)

func apiWatch(root, connector, fixtures, out, summary string) error {
	connectors, err := loadConnectors(root)
	if err != nil {
		return err
	}

	targets := apiTargets(connectors, connector)
	if len(targets) == 0 {
		return fmt.Errorf("no api_versions declared%s", forConnector(connector))
	}

	client := apiwatch.NewClient()
	if fixtures != "" {
		client = &http.Client{Transport: apiwatch.FixtureTransport{Dir: fixtures}}
	}

	report := apiwatch.Run(context.Background(), client, targets, time.Now())
	report.Connector = connector

	for _, e := range report.Entries {
		detail := e.Error
		if detail == "" {
			detail = e.Note
		}

		fmt.Printf("%-14s %s %s: %s -> %s %s %s\n", e.Status, e.Connector, e.API, e.Pinned, e.Latest, e.Sunset, detail)
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}

	if err := os.WriteFile(out, append(data, '\n'), 0o644); err != nil {
		return err
	}

	if summary != "" {
		if err := appendFile(summary, apiwatch.Markdown(report)); err != nil {
			return err
		}
	}

	fmt.Printf("wrote %s (%d entries)\n", out, len(report.Entries))

	return nil
}

func apiTargets(connectors []Connector, only string) []apiwatch.Target {
	var targets []apiwatch.Target

	for _, c := range connectors {
		if only != "" && c.Dir != only {
			continue
		}

		for _, v := range c.Meta.APIVersions {
			targets = append(targets, apiwatch.Target{
				Connector: c.Dir, API: v.API, Pinned: v.Pinned, Checker: v.Checker, Params: v.Params, Docs: v.Docs,
				File: path.Join(connectorsDir, c.Dir, filepath.ToSlash(v.File)), Line: v.Line,
			})
		}
	}

	return targets
}

func apiWatchIssues(reportPath, repo, bumpsPath string, dryRun bool) error {
	if repo == "" {
		return errors.New("--repo is required")
	}

	data, err := os.ReadFile(reportPath)
	if err != nil {
		return err
	}

	var report apiwatch.Report
	if err := json.Unmarshal(data, &report); err != nil {
		return fmt.Errorf("parsing %s: %w", reportPath, err)
	}

	gh := apiwatch.NewGitHub(repo, os.Getenv("GH_TOKEN"))
	gh.DryRun = dryRun
	gh.Log = os.Stdout

	bumps, runErr := apiwatch.SyncIssues(context.Background(), gh, report)

	if bumpsPath != "" {
		out, err := json.Marshal(bumps)
		if err != nil {
			return err
		}

		if err := os.WriteFile(bumpsPath, append(out, '\n'), 0o644); err != nil {
			return err
		}
	}

	return runErr
}

func forConnector(dir string) string {
	if dir == "" {
		return ""
	}

	return " for " + dir
}

func appendFile(name, text string) (rerr error) {
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { rerr = errors.Join(rerr, f.Close()) }()

	_, err = f.WriteString(text)

	return err
}
