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

// configured is a stream of the catalog with its definition.
type configured struct {
	def  streamDef
	desc synclet.StreamDescriptor
}

// Read emits the configured streams in catalog order. The customer streams
// share one pass over the customer list, run where the first of them is in
// the catalog: each customer is emitted, then each selected child stream of
// that customer. All streams share one client, so the pacing of each rate
// limiting domain holds across streams.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, _ *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	var plan []configured
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		def, ok := streamByName(desc.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		if cs.SyncMode == synclet.SyncModeIncremental {
			return synclet.ConfigError(fmt.Errorf("stream %s supports full refresh only: the RevenueCat API has no updated-since filter", desc.Name))
		}
		plan = append(plan, configured{def: def, desc: desc})
	}
	r := reader{c: s.apiClient(cfg), cfg: cfg, em: em}
	customersDone := false
	for _, st := range plan {
		if st.def.name == streamCustomers || st.def.child {
			if customersDone {
				continue
			}
			customersDone = true
			if err := r.customers(ctx, plan); err != nil {
				return err
			}
			continue
		}
		if err := r.stream(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

// reader is one read. project is resolved on first use.
type reader struct {
	c       *apiClient
	cfg     Config
	em      *synclet.Emitter
	project string
}

func (r *reader) projectID(ctx context.Context) (string, error) {
	if r.project == "" {
		p, err := resolveProject(ctx, r.c, r.cfg)
		if err != nil {
			return "", err
		}
		r.project = p
	}
	return r.project, nil
}

// stream reads a stream that is not a customer stream.
func (r *reader) stream(ctx context.Context, st configured) error {
	var project string
	if st.def.projectScoped() {
		p, err := r.projectID(ctx)
		if err != nil {
			return err
		}
		project = p
	}
	path := st.def.pathFor(project, "")
	if st.def.key != "" {
		return streamError(st.def, project, r.document(ctx, st, path))
	}
	err := r.c.list(ctx, st.def.domain, path, nil, func(it item) error { return r.em.Record(st.desc, it) })
	return streamError(st.def, project, err)
}

// document reads a single-document stream: one request, one record per
// entry of the def.key array. A top-level currency is copied into every
// record that has none.
func (r *reader) document(ctx context.Context, st configured, path string) error {
	var body map[string]json.RawMessage
	if err := r.c.get(ctx, st.def.domain, path, nil, &body); err != nil {
		return err
	}
	raw, ok := body[st.def.key]
	if !ok {
		return fmt.Errorf("GET %s: response has no %q", path, st.def.key)
	}
	var recs []item
	if err := json.Unmarshal(raw, &recs); err != nil {
		return fmt.Errorf("GET %s: decode %s: %w", path, st.def.key, err)
	}
	for _, rec := range recs {
		if cur, ok := body["currency"]; ok {
			if _, has := rec["currency"]; !has {
				rec["currency"] = cur
			}
		}
		if err := r.em.Record(st.desc, rec); err != nil {
			return err
		}
	}
	return nil
}

// customers lists the customers once and, for each, emits the customer if
// customers is configured, then every configured child stream of that
// customer to its last page. A child 404 means the customer was deleted
// after it was listed: the read logs a warning and skips the customer's
// remaining child streams.
func (r *reader) customers(ctx context.Context, plan []configured) error {
	project, err := r.projectID(ctx)
	if err != nil {
		return err
	}
	var parent *configured
	var children []configured
	for _, st := range plan {
		switch {
		case st.def.name == streamCustomers:
			parent = &st
		case st.def.child:
			children = append(children, st)
		}
	}
	customersDef, _ := streamByName(streamCustomers)
	err = r.c.list(ctx, customerDomain, customersDef.pathFor(project, ""), nil, func(it item) error {
		var id string
		if err := json.Unmarshal(it["id"], &id); err != nil || id == "" {
			return fmt.Errorf("customers: customer without a string id: %s", it["id"])
		}
		if parent != nil {
			if err := r.em.Record(parent.desc, it); err != nil {
				return err
			}
		}
		for _, ch := range children {
			err := r.child(ctx, ch, project, id)
			var he *httpError
			if errors.As(err, &he) && he.status == http.StatusNotFound {
				synclet.Logger(ctx).Warn(fmt.Sprintf("customer %q was not found when reading %s (deleted after it was listed?); skipping its remaining customer streams: %s", id, ch.def.name, he))
				return nil
			}
			if err != nil {
				return childFailure{streamError(ch.def, project, err)}
			}
		}
		return nil
	})
	var cf childFailure
	if errors.As(err, &cf) {
		return cf.err
	}
	return streamError(customersDef, project, err)
}

// childFailure is the failure of a child stream inside the customer list,
// already described for its stream.
type childFailure struct{ err error }

func (e childFailure) Error() string { return e.err.Error() }
func (e childFailure) Unwrap() error { return e.err }

// child emits one child stream of one customer. Each record carries the
// customer's id in customer_id; a record that names another customer fails
// the read.
func (r *reader) child(ctx context.Context, st configured, project, customer string) error {
	var query url.Values
	if st.def.environment && r.cfg.Environment != "" {
		query = url.Values{"environment": {r.cfg.Environment}}
	}
	want, err := json.Marshal(customer)
	if err != nil {
		return err
	}
	return r.c.list(ctx, st.def.domain, st.def.pathFor(project, customer), query, func(it item) error {
		if got, ok := it["customer_id"]; ok && string(got) != "null" {
			var id string
			if err := json.Unmarshal(got, &id); err != nil || id != customer {
				return fmt.Errorf("%s of customer %q: record has customer_id %s", st.def.name, customer, got)
			}
		} else {
			it["customer_id"] = want
		}
		return r.em.Record(st.desc, it)
	})
}

// resolveProject returns the configured project id, or the only project
// the key can see when none is configured.
func resolveProject(ctx context.Context, c *apiClient, cfg Config) (string, error) {
	if cfg.ProjectID != "" {
		return cfg.ProjectID, nil
	}
	ids, err := projectIDs(ctx, c)
	var fb *forbidden
	if errors.As(err, &fb) {
		return "", synclet.ConfigError(fmt.Errorf("project_id is empty and the key cannot list projects (needs project_configuration:projects:read): %w", err))
	}
	if err != nil {
		return "", err
	}
	return onlyProject(ids)
}

func projectIDs(ctx context.Context, c *apiClient) ([]string, error) {
	var ids []string
	err := c.list(ctx, configDomain, "projects", nil, func(it item) error {
		var id string
		if err := json.Unmarshal(it["id"], &id); err != nil {
			return fmt.Errorf("projects: project without a string id: %s", it["id"])
		}
		ids = append(ids, id)
		return nil
	})
	return ids, err
}

func onlyProject(ids []string) (string, error) {
	switch len(ids) {
	case 1:
		return ids[0], nil
	case 0:
		return "", synclet.ConfigError(errors.New("project_id is empty and the key can see no project"))
	}
	slices.Sort(ids)
	return "", synclet.ConfigError(fmt.Errorf("project_id is empty and the key can see %d projects (%s); set project_id to one of them", len(ids), strings.Join(ids, ", ")))
}

// streamError names the stream in a permission error and turns a 404 of a
// project-scoped list into an unknown project.
func streamError(def streamDef, project string, err error) error {
	if err == nil {
		return nil
	}
	var fb *forbidden
	if errors.As(err, &fb) {
		return synclet.ConfigError(fmt.Errorf("stream %s: key lacks permission %s: %w", def.name, def.permission, err))
	}
	var he *httpError
	if !def.child && def.projectScoped() && errors.As(err, &he) && he.status == http.StatusNotFound {
		return synclet.ConfigError(fmt.Errorf("unknown project %q: %w", project, err))
	}
	return err
}
