// Package manifest loads and validates the YAML manifest of a declarative
// HTTP source: the format of README.md.
package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Manifest is a loaded, validated manifest. Every template, path and field
// in it is parsed, and every stream's schema is resolved to JSON.
type Manifest struct {
	Version   int                 `json:"version"`
	Connector Connector           `json:"connector"`
	BaseURL   Template            `json:"base_url"`
	Spec      Spec                `json:"spec"`
	Auth      Auth                `json:"auth"`
	Headers   map[string]Template `json:"headers"`
	Params    map[string]Param    `json:"params"`
	Timeout   *Duration           `json:"timeout"`
	Errors    Errors              `json:"errors"`
	Retry     Retry               `json:"retry"`
	Check     Check               `json:"check"`
	Streams   []Stream            `json:"streams"`
}

// Connector names the connector.
type Connector struct {
	Name             string `json:"name"`
	DocumentationURL string `json:"documentation_url"`
}

// Spec is the connector's configuration form.
type Spec struct {
	Title       string              `json:"title"`
	Description string              `json:"description"`
	Required    []string            `json:"required"`
	Properties  map[string]Property `json:"properties"`
}

// Property is one configuration property.
type Property struct {
	Type        string            `json:"type"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Default     json.RawMessage   `json:"default"`
	Examples    []json.RawMessage `json:"examples"`
	Enum        []json.RawMessage `json:"enum"`
	Format      string            `json:"format"`
	Pattern     string            `json:"pattern"`
	Secret      bool              `json:"secret"`
	Order       *int              `json:"order"`
	Items       *Items            `json:"items"`
}

// Items is the item type of an array property.
type Items struct {
	Type string `json:"type"`
}

// Auth types.
const (
	AuthNone          = "none"
	AuthAPIKey        = "api_key"
	AuthBearer        = "bearer"
	AuthBasic         = "basic"
	AuthOAuth2Refresh = "oauth2_refresh"
)

// Auth says how requests are authenticated.
type Auth struct {
	Type string `json:"type"`
	// api_key
	In    string   `json:"in"`
	Name  string   `json:"name"`
	Value Template `json:"value"`
	// bearer
	Token Template `json:"token"`
	// basic
	Username Template `json:"username"`
	Password Template `json:"password"`
	// oauth2_refresh
	TokenURL        Template `json:"token_url"`
	ClientID        Template `json:"client_id"`
	ClientSecret    Template `json:"client_secret"`
	RefreshToken    Template `json:"refresh_token"`
	Scopes          []string `json:"scopes"`
	Body            string   `json:"body"`
	AccessTokenPath *Path    `json:"access_token_path"`
	ExpiresInPath   *Path    `json:"expires_in_path"`
}

// Errors says where an error body carries the API's message.
type Errors struct {
	Message *Path `json:"message"`
}

// Retry holds the retry and rate-limit rules.
type Retry struct {
	Transient Transient `json:"transient"`
	RateLimit RateLimit `json:"rate_limit"`
}

// Transient is the retry rule for transient failures.
type Transient struct {
	Statuses []int    `json:"statuses"`
	Attempts int      `json:"attempts"`
	Backoff  Duration `json:"backoff"`
}

// RateLimit is the wait rule for rate-limited responses.
type RateLimit struct {
	Statuses []int     `json:"statuses"`
	AlsoWhen *AlsoWhen `json:"also_when"`
	Wait     []Wait    `json:"wait"`
	Attempts int       `json:"attempts"`
}

// AlsoWhen makes one more status a rate limit when a header has a value.
type AlsoWhen struct {
	Status int    `json:"status"`
	Header string `json:"header"`
	Equals string `json:"equals"`
}

// Wait sources.
const (
	WaitHeader      = "header"
	WaitHeaderEpoch = "header-epoch"
	WaitBody        = "body"
	WaitFixed       = "fixed"
)

// Wait is one entry of rate_limit.wait.
type Wait struct {
	Raw   string
	Kind  string
	Name  string        // header name
	Path  *Path         // body path
	Fixed time.Duration // fixed duration
}

// UnmarshalJSON parses a wait entry.
func (w *Wait) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*w = Wait{Raw: s}
	return nil
}

func (w *Wait) parse() error {
	switch {
	case strings.HasPrefix(w.Raw, "header-epoch:"):
		w.Kind, w.Name = WaitHeaderEpoch, strings.TrimPrefix(w.Raw, "header-epoch:")
	case strings.HasPrefix(w.Raw, "header:"):
		w.Kind, w.Name = WaitHeader, strings.TrimPrefix(w.Raw, "header:")
	case strings.HasPrefix(w.Raw, "body:"):
		p, err := ParsePath(strings.TrimPrefix(w.Raw, "body:"))
		if err != nil {
			return err
		}
		w.Kind, w.Path = WaitBody, p
	default:
		d, err := parseDuration(w.Raw)
		if err != nil {
			return fmt.Errorf("%q is neither header:<name>, header-epoch:<name>, body:<path> nor a duration", w.Raw)
		}
		w.Kind, w.Fixed = WaitFixed, d
	}
	if (w.Kind == WaitHeader || w.Kind == WaitHeaderEpoch) && w.Name == "" {
		return fmt.Errorf("%q names no header", w.Raw)
	}
	return nil
}

// Check names the stream whose first request check sends.
type Check struct {
	Stream string `json:"stream"`
}

// On404 values.
const (
	On404Fail = "fail"
	On404Skip = "skip"
)

// Stream is one stream of the catalog.
type Stream struct {
	Name        string              `json:"name"`
	Path        Template            `json:"path"`
	Params      map[string]Param    `json:"params"`
	Headers     map[string]Template `json:"headers"`
	PrimaryKey  []Field             `json:"primary_key"`
	Schema      json.RawMessage     `json:"schema"`
	Records     Records             `json:"records"`
	Pagination  *Pagination         `json:"pagination"`
	Incremental *Incremental        `json:"incremental"`
	Partition   *Partition          `json:"partition"`
	On404       string              `json:"on_404"`
}

// Record modes.
const (
	ModeArray   = "array"
	ModeObject  = "object"
	ModeEntries = "entries"
)

// Records says which part of a response body is the records.
type Records struct {
	Path       *Path                `json:"path"`
	Mode       string               `json:"mode"`
	KeyField   *Field               `json:"key_field"`
	ValueField *Field               `json:"value_field"`
	Add        map[string]AddSource `json:"add"`
}

// AddSource is the value records.add puts into a field: a response path, a
// template or a constant.
type AddSource struct {
	Path     *Path
	Template *Template
	Const    any
}

// UnmarshalJSON decodes a string starting with $ as a path, one with {{ as
// a template, and anything else as a constant.
func (a *AddSource) UnmarshalJSON(b []byte) error {
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return err
	}
	*a = AddSource{}
	s, ok := v.(string)
	switch {
	case ok && strings.HasPrefix(s, "$"):
		a.Path = &Path{Raw: s}
	case ok && strings.Contains(s, "{{"):
		a.Template = &Template{Raw: s}
	default:
		a.Const = v
	}
	return nil
}

// Pagination types.
const (
	PageNumber = "page"
	PageOffset = "offset"
	PageCursor = "cursor"
	PageNext   = "next_url"
	PageLink   = "link"
)

// Pagination says how a stream pages.
type Pagination struct {
	Type        string `json:"type"`
	Size        int    `json:"size"`
	SizeParam   string `json:"size_param"`
	PageParam   string `json:"page_param"`
	First       *int   `json:"first"`
	OffsetParam string `json:"offset_param"`
	CursorFrom  string `json:"cursor_from"`
	CursorParam string `json:"cursor_param"`
	URLFrom     *Path  `json:"url_from"`
	HasMore     *Path  `json:"has_more"`
	IsLast      *Path  `json:"is_last"`
	Total       *Path  `json:"total"`
	ShortPage   bool   `json:"short_page"`
	MaxPages    int    `json:"max_pages"`

	// Cursor is the parsed cursor_from.
	Cursor CursorSource `json:"-"`
}

// Cursor sources.
const (
	CursorBody   = "body"
	CursorHeader = "header"
	CursorLast   = "last"
)

// CursorSource is a parsed cursor_from.
type CursorSource struct {
	Kind   string
	Path   *Path
	Header string
	Field  Field
}

// Incremental says how a stream reads only what changed.
type Incremental struct {
	CursorField  Field     `json:"cursor_field"`
	CursorFormat string    `json:"cursor_format"`
	Start        *Template `json:"start"`
	SinceFormat  string    `json:"since_format"`
	Lookback     *Duration `json:"lookback"`
	Window       *Window   `json:"window"`
}

// Window splits an incremental read into time windows.
type Window struct {
	Step       Duration `json:"step"`
	ClosedOnly bool     `json:"closed_only"`
}

// Partition runs a stream once per value or per parent record.
type Partition struct {
	Values *Values `json:"values"`
	Parent string  `json:"parent"`
}

// Values is partition.values: one template naming a config array, or literal
// strings.
type Values struct {
	Template *Template
	List     []string
}

// UnmarshalJSON decodes a string as a template and an array as literals.
func (v *Values) UnmarshalJSON(b []byte) error {
	*v = Values{}
	if len(b) > 0 && b[0] == '[' {
		return json.Unmarshal(b, &v.List)
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v.Template = &Template{Raw: s}
	return nil
}

// Param is a query parameter: one value, or a list sent as a repeated
// parameter.
type Param struct {
	Values []Template
	List   bool
}

// UnmarshalJSON decodes a scalar or a list of scalars; numbers and booleans
// become literal templates.
func (p *Param) UnmarshalJSON(b []byte) error {
	*p = Param{}
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return err
	}
	if list, ok := v.([]any); ok {
		p.List = true
		for _, e := range list {
			t, err := scalarTemplate(e)
			if err != nil {
				return err
			}
			p.Values = append(p.Values, t)
		}
		return nil
	}
	t, err := scalarTemplate(v)
	if err != nil {
		return err
	}
	p.Values = []Template{t}
	return nil
}

func scalarTemplate(v any) (Template, error) {
	switch x := v.(type) {
	case string:
		return Template{Raw: x}, nil
	case json.Number:
		return Template{Raw: x.String()}, nil
	case bool:
		return Template{Raw: strconv.FormatBool(x)}, nil
	}
	return Template{}, fmt.Errorf("parameter value %v is not a string, number or boolean", v)
}

// Field is a dotted record field name.
type Field struct {
	Raw   string
	Parts []string
}

// UnmarshalJSON decodes a field name.
func (f *Field) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*f = Field{Raw: s, Parts: strings.Split(s, ".")}
	return nil
}

// Last returns the field's last segment.
func (f Field) Last() string { return f.Parts[len(f.Parts)-1] }

// Duration is a Go duration that also takes Nd for N days.
type Duration time.Duration

var durationPart = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)d`)

// UnmarshalJSON decodes a duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := parseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// parseDuration parses a Go duration whose first unit may be d (24h).
func parseDuration(s string) (time.Duration, error) {
	var days time.Duration
	rest := s
	if m := durationPart.FindStringSubmatch(s); m != nil {
		n, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, err
		}
		days = time.Duration(n * float64(24*time.Hour))
		rest = s[len(m[0]):]
		if rest == "" {
			return days, nil
		}
	}
	v, err := time.ParseDuration(rest)
	if err != nil {
		return 0, fmt.Errorf("%q is not a duration such as 30s, 1h or 1d", s)
	}
	return days + v, nil
}
