package main

import (
	"context"
	"fmt"
	"net/url"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// cursorFields are the fields an incremental read can follow, in order of
// preference: SystemModstamp also changes on system updates that leave
// LastModifiedDate as it is.
var cursorFields = []string{"SystemModstamp", "LastModifiedDate"}

// deniedObjects are queryable objects that reject a query without a filter
// on a specific field or id, so they cannot be read as a whole.
var deniedObjects = []string{
	"AppTabMember", "CollaborationGroupRecord", "ColorDefinition", "ContentDocumentLink", "ContentFolderItem",
	"ContentFolderMember", "DataStatistics", "DatacloudDandBCompany", "EntityParticle", "FieldDefinition",
	"FieldHistoryArchive", "FlexQueueItem", "FlowVariableView", "FlowVersionView", "IconDefinition", "IdeaComment",
	"NetworkUserHistoryRecent", "OwnerChangeOptionInfo", "PicklistValueInfo", "PlatformAction", "RelationshipDomain",
	"RelationshipInfo", "SearchLayout", "SiteDetail", "UserEntityAccess", "UserFieldAccess", "Vote",
}

type sobject struct {
	Name      string `json:"name"`
	Queryable bool   `json:"queryable"`
}

type describeField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// sobjects lists the org's objects.
func (c *apiClient) sobjects(ctx context.Context) ([]sobject, error) {
	var body struct {
		SObjects []sobject `json:"sobjects"`
	}
	if err := c.get(ctx, "sobjects", nil, &body); err != nil {
		return nil, err
	}
	return body.SObjects, nil
}

// describe returns the fields of object name.
func (c *apiClient) describe(ctx context.Context, name string) ([]describeField, error) {
	var body struct {
		Fields []describeField `json:"fields"`
	}
	if err := c.get(ctx, "sobjects/"+url.PathEscape(name)+"/describe", nil, &body); err != nil {
		return nil, err
	}
	return body.Fields, nil
}

// Discover returns one stream per queryable object that is not denied and
// that streams_include and streams_exclude keep, with the schema of its
// describe. Objects are described one after another, in name order.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	c, err := s.apiClient(cfg)
	if err != nil {
		return nil, err
	}
	c.warnRotation()
	objects, err := c.sobjects(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, o := range objects {
		if o.Queryable && !slices.Contains(deniedObjects, o.Name) && cfg.selected(o.Name) {
			names = append(names, o.Name)
		}
	}
	slices.Sort(names)
	catalog := &synclet.Catalog{Streams: []synclet.Stream{}}
	for _, name := range names {
		fields, err := c.describe(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("describe %s: %w", name, err)
		}
		catalog.Streams = append(catalog.Streams, stream(name, fields))
	}
	return catalog, nil
}

// stream builds the stream of object name from its describe fields. An
// object with Id and a cursor field supports incremental reads.
func stream(name string, fields []describeField) synclet.Stream {
	props := map[synclet.PropertyName]synclet.PropertySpec{}
	for _, f := range fields {
		if p, ok := property(f.Type); ok {
			props[synclet.PropertyName(f.Name)] = p
		}
	}
	s := synclet.Stream{
		Name:               name,
		JSONSchema:         synclet.ObjectSchema(props),
		SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh},
	}
	if _, ok := props["Id"]; !ok {
		return s
	}
	s.SourceDefinedPrimaryKey = [][]string{{"Id"}}
	for _, cf := range cursorFields {
		if _, ok := props[synclet.PropertyName(cf)]; ok {
			s.SupportedSyncModes = append(s.SupportedSyncModes, synclet.SyncModeIncremental)
			s.SourceDefinedCursor = true
			s.DefaultCursorField = []string{cf}
			break
		}
	}
	return s
}
