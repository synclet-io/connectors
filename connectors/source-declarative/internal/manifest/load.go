package manifest

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"go.yaml.in/yaml/v3"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// FileName is the manifest's file name in a connector directory.
const FileName = "manifest.yaml"

//go:embed manifest.schema.json
var formatSchemaJSON []byte

// FormatSchema returns the JSON Schema of the manifest format.
func FormatSchema() []byte { return bytes.Clone(formatSchemaJSON) }

var formatSchema = func() *jsonschema.Schema {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(formatSchemaJSON))
	if err != nil {
		panic(fmt.Sprintf("manifest.schema.json: %v", err))
	}
	c := jsonschema.NewCompiler()
	const url = "mem://manifest.schema.json"
	if err := c.AddResource(url, doc); err != nil {
		panic(fmt.Sprintf("manifest.schema.json: %v", err))
	}
	s, err := c.Compile(url)
	if err != nil {
		panic(fmt.Sprintf("manifest.schema.json: %v", err))
	}
	return s
}()

// Error is one problem of a manifest, at a path such as
// streams[2].pagination.cursor_from.
type Error struct {
	Path    string
	Message string
}

func (e *Error) Error() string {
	if e.Path == "" {
		return e.Message
	}
	return e.Path + ": " + e.Message
}

func errorf(path, format string, args ...any) *Error {
	return &Error{Path: path, Message: fmt.Sprintf(format, args...)}
}

// List returns every *Error in err, which Load and LoadDocument return
// joined.
func List(err error) []*Error {
	var out []*Error
	var walk func(error)
	walk = func(err error) {
		if err == nil {
			return
		}
		var e *Error
		if errors.As(err, &e) && e == err {
			out = append(out, e)
			return
		}
		if j, ok := err.(interface{ Unwrap() []error }); ok {
			for _, x := range j.Unwrap() {
				walk(x)
			}
			return
		}
		out = append(out, &Error{Message: err.Error()})
	}
	walk(err)
	return out
}

// Load reads dir/manifest.yaml and the schema files it names.
func Load(dir string) (*Manifest, error) {
	return LoadFS(os.DirFS(dir))
}

// LoadFS reads manifest.yaml at the root of fsys and the schema files it
// names, relative to that root.
func LoadFS(fsys fs.FS) (*Manifest, error) {
	b, err := fs.ReadFile(fsys, FileName)
	if err != nil {
		return nil, err
	}
	doc, err := DecodeYAML(b)
	if err != nil {
		return nil, err
	}
	return LoadDocument(doc, fsys)
}

// DecodeYAML decodes a manifest document with its anchors and merge keys
// resolved.
func DecodeYAML(b []byte) (map[string]any, error) {
	var v any
	if err := yaml.Unmarshal(b, &v); err != nil {
		return nil, &Error{Message: FileName + ": " + err.Error()}
	}
	if v == nil {
		return nil, &Error{Message: FileName + " is empty"}
	}
	jv, err := jsonValue(v, "")
	if err != nil {
		return nil, err
	}
	doc, ok := jv.(map[string]any)
	if !ok {
		return nil, &Error{Message: FileName + " is not a mapping"}
	}
	return doc, nil
}

// jsonValue turns a decoded YAML value into the value JSON would decode to.
func jsonValue(v any, at string) (any, error) {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			jv, err := jsonValue(e, joinKey(at, k))
			if err != nil {
				return nil, err
			}
			out[k] = jv
		}
		return out, nil
	case map[any]any:
		return nil, errorf(at, "mapping keys must be strings")
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			jv, err := jsonValue(e, at+"["+strconv.Itoa(i)+"]")
			if err != nil {
				return nil, err
			}
			out[i] = jv
		}
		return out, nil
	case int:
		return json.Number(strconv.Itoa(x)), nil
	case int64:
		return json.Number(strconv.FormatInt(x, 10)), nil
	case uint64:
		return json.Number(strconv.FormatUint(x, 10)), nil
	case float64:
		return json.Number(strconv.FormatFloat(x, 'g', -1, 64)), nil
	case string, bool, nil, json.Number:
		return x, nil
	}
	return nil, errorf(at, "unsupported YAML value %v (%T)", v, v)
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func joinKey(at, k string) string {
	if !identifier.MatchString(k) {
		return at + "['" + k + "']"
	}
	if at == "" {
		return k
	}
	return at + "." + k
}

// LoadDocument validates a manifest document, decodes it strictly and lints
// it. A stream's schema that is a file path is read from files; with files
// nil (an injected manifest) a file path is an error, so the schemas must be
// inline. Every problem is returned, joined, as an *Error with its path.
func LoadDocument(doc map[string]any, files fs.FS) (*Manifest, error) {
	jv, err := jsonValue(doc, "")
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(jv)
	if err != nil {
		return nil, &Error{Message: err.Error()}
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, &Error{Message: err.Error()}
	}
	if err := formatSchema.Validate(inst); err != nil {
		var ve *jsonschema.ValidationError
		if !errors.As(err, &ve) {
			return nil, &Error{Message: err.Error()}
		}
		return nil, joinErrors(schemaErrors(ve, inst))
	}
	var m Manifest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return nil, &Error{Message: "decode: " + err.Error()}
	}
	errs := resolveSchemas(&m, files)
	errs = append(errs, lint(&m)...)
	if len(errs) > 0 {
		return nil, joinErrors(errs)
	}
	return &m, nil
}

func joinErrors(errs []*Error) error {
	slices.SortStableFunc(errs, func(a, b *Error) int { return strings.Compare(a.Path, b.Path) })
	out := make([]error, len(errs))
	for i, e := range errs {
		out[i] = e
	}
	return errors.Join(out...)
}

var printer = message.NewPrinter(language.English)

// schemaErrors flattens a validation error into one *Error per failed
// keyword, at the path of the value it concerns.
func schemaErrors(ve *jsonschema.ValidationError, inst any) []*Error {
	var out []*Error
	seen := map[string]bool{}
	add := func(e *Error) {
		if k := e.Error(); !seen[k] {
			seen[k] = true
			out = append(out, e)
		}
	}
	var walk func(*jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) > 0 {
			for _, c := range e.Causes {
				walk(c)
			}
			return
		}
		at := instancePath(e.InstanceLocation, inst)
		switch k := e.ErrorKind.(type) {
		case *kind.AdditionalProperties:
			for _, p := range k.Properties {
				add(errorf(joinKey(at, p), "unknown key"))
			}
		case *kind.Required:
			for _, p := range k.Missing {
				add(errorf(joinKey(at, p), "required key is missing"))
			}
		case *kind.Type:
			add(errorf(at, "wrong type: got %s, want %s", k.Got, strings.Join(k.Want, " or ")))
		case *kind.Group, *kind.AllOf:
		default:
			add(errorf(at, "%s", e.ErrorKind.LocalizedString(printer)))
		}
	}
	walk(ve)
	return out
}

// instancePath renders a JSON pointer as streams[2].pagination; tokens that
// index an array become [n].
func instancePath(loc []string, inst any) string {
	at := ""
	v := inst
	for _, tok := range loc {
		switch x := v.(type) {
		case []any:
			i, _ := strconv.Atoi(tok)
			at += "[" + tok + "]"
			if i < len(x) {
				v = x[i]
			} else {
				v = nil
			}
		case map[string]any:
			at = joinKey(at, tok)
			v = x[tok]
		default:
			at = joinKey(at, tok)
			v = nil
		}
	}
	return at
}

// resolveSchemas replaces every stream's schema by the JSON Schema it names
// or holds, canonically encoded, and checks it is an object schema with
// properties.
func resolveSchemas(m *Manifest, files fs.FS) []*Error {
	var errs []*Error
	for i := range m.Streams {
		st := &m.Streams[i]
		at := fmt.Sprintf("streams[%d].schema", i)
		raw := []byte(st.Schema)
		var file string
		if err := json.Unmarshal(raw, &file); err == nil {
			if files == nil {
				errs = append(errs, errorf(at, "a file path is not allowed in an injected manifest; inline the schema"))
				st.Schema = nil
				continue
			}
			clean := path.Clean(file)
			if path.IsAbs(file) || clean == ".." || strings.HasPrefix(clean, "../") {
				errs = append(errs, errorf(at, "schema file %q must be a path inside the connector directory", file))
				st.Schema = nil
				continue
			}
			b, err := fs.ReadFile(files, clean)
			if err != nil {
				errs = append(errs, errorf(at, "schema file %q: %v", file, err))
				st.Schema = nil
				continue
			}
			raw = b
			at += " (" + file + ")"
		}
		canon, err := checkSchema(raw)
		if err != nil {
			errs = append(errs, errorf(at, "%v", err))
			st.Schema = nil
			continue
		}
		st.Schema = canon
	}
	return errs
}

func checkSchema(raw []byte) (json.RawMessage, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("not JSON: %w", err)
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil, errors.New("not a JSON Schema object")
	}
	if !isObjectType(obj["type"]) {
		return nil, errors.New(`not an object schema: "type" must be "object"`)
	}
	if props, ok := obj["properties"].(map[string]any); !ok || len(props) == 0 {
		return nil, errors.New(`an object schema needs "properties"`)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft7)
	c.UseLoader(noLoader{})
	const url = "mem://stream.schema.json"
	if err := c.AddResource(url, doc); err != nil {
		return nil, err
	}
	if _, err := c.Compile(url); err != nil {
		return nil, fmt.Errorf("not a valid JSON Schema: %w", err)
	}
	canon, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return canon, nil
}

// noLoader refuses every URL, so a stream schema resolves only its own
// definitions.
type noLoader struct{}

func (noLoader) Load(string) (any, error) {
	return nil, errors.New("a stream schema may not reference an external schema")
}

func isObjectType(t any) bool {
	switch x := t.(type) {
	case string:
		return x == "object"
	case []any:
		return slices.Contains(x, any("object"))
	}
	return false
}
