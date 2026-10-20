package apiwatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// GitHub is the part of the GitHub REST API the API watch needs. With DryRun
// it reads but logs writes instead of sending them.
type GitHub struct {
	BaseURL string
	Owner   string
	Repo    string
	Token   string
	Client  *http.Client
	DryRun  bool
	Log     io.Writer
}

func NewGitHub(repo, token string) *GitHub {
	owner, name, _ := strings.Cut(repo, "/")

	return &GitHub{BaseURL: "https://api.github.com", Owner: owner, Repo: name, Token: token, Client: NewClient(), Log: io.Discard}
}

type Issue struct {
	Number      int             `json:"number"`
	Title       string          `json:"title"`
	Body        string          `json:"body"`
	Labels      []issueLabel    `json:"labels"`
	PullRequest json.RawMessage `json:"pull_request,omitempty"`
}

type issueLabel struct {
	Name string `json:"name"`
}

func (is Issue) LabelNames() []string {
	names := make([]string, 0, len(is.Labels))
	for _, l := range is.Labels {
		names = append(names, l.Name)
	}

	return names
}

func (g *GitHub) RepoURL() string {
	return "https://github.com/" + g.Owner + "/" + g.Repo
}

func (g *GitHub) logf(format string, args ...any) {
	_, _ = fmt.Fprintf(g.Log, format+"\n", args...)
}

func (g *GitHub) repoPath(parts ...string) string {
	escaped := []string{"repos", url.PathEscape(g.Owner), url.PathEscape(g.Repo)}
	for _, p := range parts {
		escaped = append(escaped, url.PathEscape(p))
	}

	return "/" + strings.Join(escaped, "/")
}

// do sends one request and decodes a JSON answer into out. It returns the
// status; statuses outside ok are errors.
func (g *GitHub) do(ctx context.Context, method, path string, in, out any, ok ...int) (_ int, rerr error) {
	var body io.Reader

	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}

		body = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(g.BaseURL, "/")+path, body)
	if err != nil {
		return 0, err
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", userAgent)

	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if g.Token != "" {
		req.Header.Set("Authorization", "Bearer "+g.Token)
	}

	resp, err := g.Client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { rerr = errors.Join(rerr, resp.Body.Close()) }()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", method, path, err)
	}

	if !slices.Contains(ok, resp.StatusCode) {
		return resp.StatusCode, fmt.Errorf("%s %s: HTTP %d: %.200s", method, path, resp.StatusCode, data)
	}

	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: %w", method, path, err)
		}
	}

	return resp.StatusCode, nil
}

// OpenIssues lists the open issues with the label, pull requests left out.
func (g *GitHub) OpenIssues(ctx context.Context, label string) ([]Issue, error) {
	const perPage = 100

	var all []Issue

	for page := 1; ; page++ {
		var batch []Issue

		q := url.Values{"labels": {label}, "state": {"open"}, "per_page": {fmt.Sprint(perPage)}, "page": {fmt.Sprint(page)}}
		if _, err := g.do(ctx, http.MethodGet, g.repoPath("issues")+"?"+q.Encode(), nil, &batch, http.StatusOK); err != nil {
			return nil, err
		}

		for _, is := range batch {
			if len(is.PullRequest) == 0 {
				all = append(all, is)
			}
		}

		if len(batch) < perPage {
			return all, nil
		}
	}
}

// EnsureLabel creates the label unless it exists.
func (g *GitHub) EnsureLabel(ctx context.Context, name, color string) error {
	if g.DryRun {
		return nil
	}

	in := map[string]string{"name": name, "color": color, "description": "Managed by the API watch workflow"}
	_, err := g.do(ctx, http.MethodPost, g.repoPath("labels"), in, nil, http.StatusCreated, http.StatusUnprocessableEntity)

	return err
}

// Apply performs one planned action and returns the issue number.
func (g *GitHub) Apply(ctx context.Context, a Action) (int, error) {
	if g.DryRun {
		g.logf("dry run: would %s #%d %s %q %v", a.Kind, a.Number, a.Key, a.Title, a.Labels)

		return a.Number, nil
	}

	switch a.Kind {
	case "create":
		var created Issue

		in := map[string]any{"title": a.Title, "body": a.Body, "labels": a.Labels}
		if _, err := g.do(ctx, http.MethodPost, g.repoPath("issues"), in, &created, http.StatusCreated); err != nil {
			return 0, err
		}

		g.logf("created #%d %s", created.Number, a.Title)

		return created.Number, nil
	case "update":
		in := map[string]any{"title": a.Title, "body": a.Body, "labels": a.Labels}
		if _, err := g.do(ctx, http.MethodPatch, g.repoPath("issues", fmt.Sprint(a.Number)), in, nil, http.StatusOK); err != nil {
			return 0, err
		}

		g.logf("updated #%d %s", a.Number, a.Title)
	case "close":
		if err := g.Comment(ctx, a.Number, a.Comment); err != nil {
			return 0, err
		}

		in := map[string]string{"state": "closed", "state_reason": "completed"}
		if _, err := g.do(ctx, http.MethodPatch, g.repoPath("issues", fmt.Sprint(a.Number)), in, nil, http.StatusOK); err != nil {
			return 0, err
		}

		g.logf("closed #%d %s", a.Number, a.Key)
	default:
		return 0, fmt.Errorf("unknown action %q", a.Kind)
	}

	return a.Number, nil
}

func (g *GitHub) Comment(ctx context.Context, number int, body string) error {
	in := map[string]string{"body": body}
	_, err := g.do(ctx, http.MethodPost, g.repoPath("issues", fmt.Sprint(number), "comments"), in, nil, http.StatusCreated)

	return err
}

// BranchExists reports whether refs/heads/<branch> exists.
func (g *GitHub) BranchExists(ctx context.Context, branch string) (bool, error) {
	path := g.repoPath("git", "ref") + "/heads/" + escapeRef(branch)

	code, err := g.do(ctx, http.MethodGet, path, nil, nil, http.StatusOK, http.StatusNotFound)

	return code == http.StatusOK, err
}

// PullExists reports whether a pull request from branch exists in state
// open, closed or all.
func (g *GitHub) PullExists(ctx context.Context, branch, state string) (bool, error) {
	var pulls []json.RawMessage

	q := url.Values{"head": {g.Owner + ":" + branch}, "state": {state}, "per_page": {"1"}}
	_, err := g.do(ctx, http.MethodGet, g.repoPath("pulls")+"?"+q.Encode(), nil, &pulls, http.StatusOK)

	return len(pulls) > 0, err
}

func escapeRef(ref string) string {
	parts := strings.Split(ref, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}

	return strings.Join(parts, "/")
}
