package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check resolves the project and requests the first record of every stream
// (the customer streams on the first customer, if there is one). A 401
// fails at once as an invalid or v1 key; a 404 of the project names an
// unknown project; 403s are collected, so the failure lists every stream
// whose permission the key lacks.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var denied []string
	collect := func(def streamDef, err error) error {
		var fb *forbidden
		if errors.As(err, &fb) {
			denied = append(denied, fmt.Sprintf("%s (%s)", def.name, def.permission))
			return nil
		}
		return err
	}

	projectsDef, _ := streamByName(streamProjects)
	ids, err := projectIDs(ctx, c)
	listed := err == nil
	if err := collect(projectsDef, err); err != nil {
		return err
	}
	project := cfg.ProjectID
	switch {
	case project == "" && !listed:
		return synclet.ConfigError(fmt.Errorf("project_id is empty and the key cannot list projects (needs %s)", projectsDef.permission))
	case project == "":
		if project, err = onlyProject(ids); err != nil {
			return err
		}
	case listed && !slices.Contains(ids, project):
		slices.Sort(ids)
		return synclet.ConfigError(fmt.Errorf("unknown project %q: the key can see projects %s", project, strings.Join(ids, ", ")))
	}

	var customer string
	for _, def := range streams {
		if def.name == streamProjects || def.child {
			continue
		}
		first, err := probe(ctx, c, def, project, "")
		if def.name == streamCustomers {
			customer = first
		}
		var he *httpError
		if errors.As(err, &he) && he.status == http.StatusNotFound {
			return synclet.ConfigError(fmt.Errorf("unknown project %q: %w", project, err))
		}
		if err := collect(def, err); err != nil {
			return err
		}
	}
	if customer != "" {
		for _, def := range streams {
			if !def.child {
				continue
			}
			_, err := probe(ctx, c, def, project, customer)
			var he *httpError
			if errors.As(err, &he) && he.status == http.StatusNotFound {
				continue
			}
			if err := collect(def, err); err != nil {
				return err
			}
		}
	}
	if len(denied) > 0 {
		return synclet.ConfigError(fmt.Errorf("key lacks permission for streams: %s", strings.Join(denied, ", ")))
	}
	return nil
}

// probe requests the first page of a stream with limit 1, or its document,
// and returns the id of the first item of a list.
func probe(ctx context.Context, c *apiClient, def streamDef, project, customer string) (string, error) {
	path := def.pathFor(project, customer)
	if def.key != "" {
		var body map[string]json.RawMessage
		return "", c.get(ctx, def.domain, path, nil, &body)
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := c.get(ctx, def.domain, path, url.Values{"limit": {"1"}}, &page); err != nil {
		return "", err
	}
	if len(page.Items) == 0 {
		return "", nil
	}
	return page.Items[0].ID, nil
}
