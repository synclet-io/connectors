package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// imageChecker reports whether repo:tag can be pulled without credentials.
type imageChecker func(repo, tag string) (bool, error)

// loadReleased returns, for every connector directory, the connector as of its
// newest <dir>-v<version> tag whose image is published. A tag whose image is not
// pullable yet (its release is still running, or failed) is skipped in favour of
// the previous one, and so is a tag whose files do not load, with a GitHub
// warning annotation naming it. A connector with no published tag is left out;
// one whose published tags all fail to load is an error.
func loadReleased(root, imagePrefix string, published imageChecker, warn io.Writer) ([]Connector, error) {
	schema, err := compileSchema(root)
	if err != nil {
		return nil, err
	}

	dirs, err := connectorDirs(root)
	if err != nil {
		return nil, err
	}

	out, err := git(root, "tag", "--list")
	if err != nil {
		return nil, err
	}

	tags := strings.Fields(out)

	var (
		connectors []Connector
		errs       []error
	)

	for _, dir := range dirs {
		repo := strings.ToLower(strings.TrimSuffix(imagePrefix, "/") + "/" + dir)

		var failed []error

		loaded := false

		for _, v := range releasedVersions(tags, dir) {
			ok, err := published(repo, v)
			if err != nil {
				return nil, fmt.Errorf("%s:%s: %w", repo, v, err)
			}

			if !ok {
				if _, err := fmt.Fprintf(warn, "skip %s %s: image not published\n", dir, v); err != nil {
					return nil, err
				}

				continue
			}

			tag := dir + "-v" + v

			c, err := parseReleased(root, tag, dir, schema)
			if err != nil {
				failed = append(failed, fmt.Errorf("%s: %w", tag, err))

				continue
			}

			if c.Meta.Version != v {
				return nil, fmt.Errorf("%s: meta.json declares version %s", tag, c.Meta.Version)
			}

			if len(failed) > 0 {
				reasons := strings.ReplaceAll(errors.Join(failed...).Error(), "\n", "%0A")
				if _, err := fmt.Fprintf(warn, "::warning::%s: falling back to %s, newer published tags do not load:%%0A%s\n",
					dir, tag, reasons); err != nil {
					return nil, err
				}
			}

			connectors = append(connectors, c)
			loaded = true

			break
		}

		if !loaded && len(failed) > 0 {
			errs = append(errs, fmt.Errorf("%s: none of its published release tags load:\n%w", dir, errors.Join(failed...)))
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return connectors, nil
}

// parseReleased validates a tag's meta.json against the current schema and,
// when that fails, against the schema as of the tag, so tightening the schema
// does not invalidate releases that were valid when they were made.
func parseReleased(root, tag, dir string, schema *jsonschema.Schema) (Connector, error) {
	read := tagReader(root, tag, dir)

	c, err := parseConnector(dir, schema, read)
	if err == nil {
		return c, nil
	}

	tagSchema, serr := compileTagSchema(root, tag)
	if serr != nil {
		return Connector{}, errors.Join(err, serr)
	}

	c, terr := parseConnector(dir, tagSchema, read)
	if terr != nil {
		return Connector{}, fmt.Errorf("against the schema at %s: %w", tag, terr)
	}

	return c, nil
}

func compileTagSchema(root, tag string) (*jsonschema.Schema, error) {
	out, err := git(root, "show", tag+":"+schemaPath)
	if err != nil {
		return nil, err
	}

	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(out))
	if err != nil {
		return nil, fmt.Errorf("parsing %s at %s: %w", schemaPath, tag, err)
	}

	url := "tag:" + tag + "/" + schemaPath

	c := jsonschema.NewCompiler()
	c.AssertFormat()

	if err := c.AddResource(url, doc); err != nil {
		return nil, fmt.Errorf("%s at %s: %w", schemaPath, tag, err)
	}

	schema, err := c.Compile(url)
	if err != nil {
		return nil, fmt.Errorf("compiling %s at %s: %w", schemaPath, tag, err)
	}

	return schema, nil
}

func tagReader(root, tag, dir string) fileReader {
	return func(name string) ([]byte, error) {
		out, err := git(root, "show", tag+":"+connectorsDir+"/"+dir+"/"+name)

		return []byte(out), err
	}
}

// releasedVersions returns the versions of dir's release tags, newest first.
// Tags that are not <dir>-v<major>.<minor>.<patch> are ignored.
func releasedVersions(tags []string, dir string) []string {
	type release struct {
		version string
		parts   [3]int
	}

	var releases []release

	for _, t := range tags {
		v, ok := strings.CutPrefix(t, dir+"-v")
		if !ok {
			continue
		}

		parts, ok := parseSemver(v)
		if !ok {
			continue
		}

		releases = append(releases, release{version: v, parts: parts})
	}

	sort.Slice(releases, func(i, j int) bool {
		a, b := releases[i].parts, releases[j].parts
		for k := range a {
			if a[k] != b[k] {
				return a[k] > b[k]
			}
		}

		return false
	})

	versions := make([]string, len(releases))
	for i, r := range releases {
		versions[i] = r.version
	}

	return versions
}

func parseSemver(v string) ([3]int, bool) {
	var parts [3]int

	fields := strings.Split(v, ".")
	if len(fields) != 3 {
		return parts, false
	}

	for i, f := range fields {
		n, err := strconv.Atoi(f)
		if err != nil || n < 0 || strconv.Itoa(n) != f {
			return parts, false
		}

		parts[i] = n
	}

	return parts, true
}

// anonymousPull checks an OCI registry that issues anonymous pull tokens from
// https://<host>/token, as ghcr.io does. ghcr.io rejects a percent-encoded scope.
func anonymousPull(client *http.Client) imageChecker {
	return func(repo, tag string) (published bool, rerr error) {
		host, name, ok := strings.Cut(repo, "/")
		if !ok {
			return false, fmt.Errorf("image %q has no registry host", repo)
		}

		resp, err := client.Get("https://" + host + "/token?scope=repository:" + name + ":pull")
		if err != nil {
			return false, err
		}
		defer func() { rerr = errors.Join(rerr, resp.Body.Close()) }()

		if resp.StatusCode != http.StatusOK {
			return false, nil
		}

		var token struct {
			Token string `json:"token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
			return false, fmt.Errorf("decoding pull token: %w", err)
		}

		req, err := http.NewRequest(http.MethodHead, "https://"+host+"/v2/"+name+"/manifests/"+tag, nil)
		if err != nil {
			return false, err
		}

		req.Header.Set("Authorization", "Bearer "+token.Token)
		req.Header.Set("Accept", "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json")

		mresp, err := client.Do(req)
		if err != nil {
			return false, err
		}
		defer func() { rerr = errors.Join(rerr, mresp.Body.Close()) }()

		switch mresp.StatusCode {
		case http.StatusOK:
			return true, nil
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
			return false, nil
		default:
			return false, fmt.Errorf("manifest request: %s", mresp.Status)
		}
	}
}

func newAnonymousPull() imageChecker {
	return anonymousPull(&http.Client{Timeout: 30 * time.Second})
}

// newestPublished returns the newest version among dir's release tags whose
// image is published, or "" when none is.
func newestPublished(tags []string, dir, repo string, published imageChecker) (string, error) {
	for _, v := range releasedVersions(tags, dir) {
		ok, err := published(repo, v)
		if err != nil {
			return "", fmt.Errorf("%s:%s: %w", repo, v, err)
		}

		if ok {
			return v, nil
		}
	}

	return "", nil
}
