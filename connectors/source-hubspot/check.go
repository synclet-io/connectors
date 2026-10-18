package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// probe is a request check sends for a stream.
type probe struct {
	stream string
	path   string
	query  url.Values
}

func objectProbe(o object) probe {
	return probe{stream: o.name, path: objectsPath(o), query: url.Values{"limit": {"1"}}}
}

// Check requests contacts first, so a wrong token fails with one clear
// error, then every stream's endpoint and the schemas API. Each 403
// MISSING_SCOPES adds the stream and the scopes HubSpot asks for; check
// fails with all of them, so one round of fixing the private app's scopes
// is enough.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var missing []string
	// scope records a MISSING_SCOPES error of what and swallows it; any other
	// error is returned.
	scope := func(what string, err error) error {
		var se scopeError
		if errors.As(err, &se) {
			missing = append(missing, fmt.Sprintf("missing scope for %s: %s", what, strings.Join(se.scopes, ", ")))
			return nil
		}
		return err
	}
	run := func(p probe) error {
		var body struct{}
		return scope("stream "+p.stream, c.get(ctx, p.path, p.query, &body))
	}
	for _, o := range standardObjects {
		if err := run(objectProbe(o)); err != nil {
			return err
		}
	}
	for _, f := range fixedStreams {
		p := probe{stream: f.name, path: f.path}
		if f.paged {
			p.query = url.Values{"limit": {"1"}}
		}
		if err := run(p); err != nil {
			return err
		}
	}
	custom, err := customObjects(ctx, c)
	if err := scope("custom objects", err); err != nil {
		return err
	}
	for _, o := range custom {
		if err := run(objectProbe(o)); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return synclet.ConfigError(errors.New(strings.Join(missing, "; ")))
	}
	return nil
}
