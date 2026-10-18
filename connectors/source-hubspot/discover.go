package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// object is a CRM object type: its stream name, its type in API paths and
// the property its last modification is searched by.
type object struct {
	name     string
	path     string
	modified string
}

// standardObjects are the CRM objects every account has.
var standardObjects = []object{
	{name: "contacts", path: "contacts", modified: "lastmodifieddate"},
	{name: "companies", path: "companies", modified: "hs_lastmodifieddate"},
	{name: "deals", path: "deals", modified: "hs_lastmodifieddate"},
	{name: "tickets", path: "tickets", modified: "hs_lastmodifieddate"},
	{name: "line_items", path: "line_items", modified: "hs_lastmodifieddate"},
	{name: "products", path: "products", modified: "hs_lastmodifieddate"},
}

const schemasPath = "crm/v3/schemas"

// customObjects returns the custom objects of the account from the schemas
// API, named after the schema's name and requested by its objectTypeId.
func customObjects(ctx context.Context, c *apiClient) ([]object, error) {
	var body struct {
		Results []struct {
			Name         string `json:"name"`
			ObjectTypeID string `json:"objectTypeId"`
			Archived     bool   `json:"archived"`
		} `json:"results"`
	}
	if err := c.get(ctx, schemasPath, nil, &body); err != nil {
		return nil, err
	}
	var out []object
	for _, r := range body.Results {
		if r.Archived {
			continue
		}
		if r.Name == "" || r.ObjectTypeID == "" {
			return nil, fmt.Errorf("GET %s: custom object schema without a name or objectTypeId", schemasPath)
		}
		out = append(out, object{name: r.Name, path: r.ObjectTypeID, modified: "hs_lastmodifieddate"})
	}
	return out, nil
}

// allObjects returns the standard objects followed by the custom ones. A
// custom object whose name is taken by another stream fails.
func allObjects(ctx context.Context, c *apiClient) ([]object, error) {
	custom, err := customObjects(ctx, c)
	if err != nil {
		return nil, err
	}
	objs := slices.Clone(standardObjects)
	for _, o := range custom {
		if nameTaken(objs, o.name) {
			return nil, fmt.Errorf("custom object %q (%s) has the name of another stream", o.name, o.path)
		}
		objs = append(objs, o)
	}
	return objs, nil
}

// nameTaken reports whether name is the name of an object or a fixed
// stream.
func nameTaken(objs []object, name string) bool {
	for _, o := range objs {
		if o.name == name {
			return true
		}
	}
	for _, f := range fixedStreams {
		if f.name == name {
			return true
		}
	}
	return false
}

// pair is the association stream of two objects; rows go from a to b.
type pair struct{ a, b object }

func (p pair) name() string { return "associations_" + p.a.name + "_" + p.b.name }

// pairs returns one pair per unordered pair of objects, a before b in objs
// order, and one pair of each object with itself.
func pairs(objs []object) []pair {
	var out []pair
	for i, a := range objs {
		for _, b := range objs[i:] {
			out = append(out, pair{a: a, b: b})
		}
	}
	return out
}

func labelsPath(p pair) string {
	return "crm/v4/associations/" + p.a.path + "/" + p.b.path + "/labels"
}

// associated reports whether the objects of p can be associated: the v4
// labels API answers 200 for such a pair and 400 or 404 otherwise.
func associated(ctx context.Context, c *apiClient, p pair) (bool, error) {
	var body struct{}
	err := c.get(ctx, labelsPath(p), nil, &body)
	var ae *apiError
	switch {
	case err == nil:
		return true, nil
	case errors.As(err, &ae) && (ae.status == http.StatusBadRequest || ae.status == http.StatusNotFound):
		return false, nil
	}
	return false, err
}

func propertiesPath(o object) string { return "crm/v3/properties/" + o.path }

// objectSchema builds an object's record schema from the properties API:
// the top-level fields of a CRM record and one properties_<name> field per
// property.
func objectSchema(ctx context.Context, c *apiClient, o object) (map[synclet.PropertyName]synclet.PropertySpec, error) {
	var body struct {
		Results []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"results"`
	}
	if err := c.get(ctx, propertiesPath(o), nil, &body); err != nil {
		return nil, fmt.Errorf("stream %s: %w", o.name, err)
	}
	props := recordFields()
	for _, p := range body.Results {
		if p.Name == "" {
			return nil, fmt.Errorf("stream %s: GET %s: property without a name", o.name, propertiesPath(o))
		}
		props[propertyColumn(p.Name)] = kindOf(p.Type).spec()
	}
	return props, nil
}

// recordFields are the top-level fields of every CRM object record.
func recordFields() map[synclet.PropertyName]synclet.PropertySpec {
	return map[synclet.PropertyName]synclet.PropertySpec{
		"id":        {PropertyType: typed(synclet.String)},
		"createdAt": kindDateTime.spec(),
		"updatedAt": kindDateTime.spec(),
		"archived":  kindBool.spec(),
	}
}

// fixedStream is a full refresh stream whose schema is the same in every
// account.
type fixedStream struct {
	name  string
	path  string
	paged bool
	props map[synclet.PropertyName]synclet.PropertySpec
}

func fields(types map[propKind][]synclet.PropertyName, extra map[synclet.PropertyName]synclet.PropertySpec) map[synclet.PropertyName]synclet.PropertySpec {
	out := map[synclet.PropertyName]synclet.PropertySpec{"id": {PropertyType: typed(synclet.String)}}
	for k, names := range types {
		for _, n := range names {
			out[n] = k.spec()
		}
	}
	for n, p := range extra {
		out[n] = p
	}
	return out
}

func pipelineFields() map[synclet.PropertyName]synclet.PropertySpec {
	return fields(map[propKind][]synclet.PropertyName{
		kindString:   {"label"},
		kindBool:     {"archived"},
		kindDateTime: {"createdAt", "updatedAt", "archivedAt"},
	}, map[synclet.PropertyName]synclet.PropertySpec{
		"displayOrder": nullable(synclet.Integer),
		"stages":       nullable(synclet.Array),
	})
}

var fixedStreams = []fixedStream{
	{
		name: "owners", path: "crm/v3/owners", paged: true,
		props: fields(map[propKind][]synclet.PropertyName{
			kindString:   {"email", "type", "firstName", "lastName"},
			kindBool:     {"archived"},
			kindDateTime: {"createdAt", "updatedAt"},
		}, map[synclet.PropertyName]synclet.PropertySpec{
			"userId":                  nullable(synclet.Integer),
			"userIdIncludingInactive": nullable(synclet.Integer),
			"teams":                   nullable(synclet.Array),
		}),
	},
	{name: "deal_pipelines", path: "crm/v3/pipelines/deals", props: pipelineFields()},
	{name: "ticket_pipelines", path: "crm/v3/pipelines/tickets", props: pipelineFields()},
}

// associationFields is the schema of an association row: one association
// type (label) between two records.
func associationFields() map[synclet.PropertyName]synclet.PropertySpec {
	return map[synclet.PropertyName]synclet.PropertySpec{
		"from_id":  {PropertyType: typed(synclet.String)},
		"to_id":    {PropertyType: typed(synclet.String)},
		"type_id":  {PropertyType: typed(synclet.Integer)},
		"category": nullable(synclet.String),
		"label":    nullable(synclet.String),
	}
}

// Discover returns one stream per CRM object, standard or custom, with its
// properties from the properties API; owners and the deal and ticket
// pipelines; and one associations stream per pair of objects the v4
// labels API knows.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	objs, err := allObjects(ctx, c)
	if err != nil {
		return nil, err
	}
	catalog := &synclet.Catalog{}
	for _, o := range objs {
		props, err := objectSchema(ctx, c, o)
		if err != nil {
			return nil, err
		}
		catalog.Streams = append(catalog.Streams, synclet.Stream{
			Name:                    o.name,
			JSONSchema:              synclet.ObjectSchema(props),
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
			SourceDefinedCursor:     true,
			DefaultCursorField:      []string{"updatedAt"},
			SourceDefinedPrimaryKey: [][]string{{"id"}},
		})
	}
	for _, f := range fixedStreams {
		catalog.Streams = append(catalog.Streams, synclet.Stream{
			Name:                    f.name,
			JSONSchema:              synclet.ObjectSchema(f.props),
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
			SourceDefinedPrimaryKey: [][]string{{"id"}},
		})
	}
	names := map[string]bool{}
	for _, st := range catalog.Streams {
		names[st.Name] = true
	}
	for _, p := range pairs(objs) {
		ok, err := associated(ctx, c, p)
		if err != nil {
			return nil, fmt.Errorf("stream %s: %w", p.name(), err)
		}
		if !ok {
			continue
		}
		if names[p.name()] {
			return nil, fmt.Errorf("association stream %s of %s and %s has the name of another stream", p.name(), p.a.path, p.b.path)
		}
		names[p.name()] = true
		catalog.Streams = append(catalog.Streams, synclet.Stream{
			Name:                    p.name(),
			JSONSchema:              synclet.ObjectSchema(associationFields()),
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
			SourceDefinedPrimaryKey: [][]string{{"from_id"}, {"to_id"}, {"type_id"}},
		})
	}
	return catalog, nil
}
