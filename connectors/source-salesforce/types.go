package main

import (
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// nullable returns a property of type t that may be null.
func nullable(t synclet.PropType) synclet.PropertySpec {
	return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}}
}

// property returns the JSON schema of a describe field type. Every property
// is nullable. base64 fields are left out (ok is false): their values are a
// URL to the blob, not its content, and reading the blob takes a request per
// record. A type this table does not know is passed through untyped.
func property(fieldType string) (_ synclet.PropertySpec, ok bool) {
	switch fieldType {
	case "base64":
		return synclet.PropertySpec{}, false
	case "id", "reference", "string", "picklist", "multipicklist", "textarea", "email", "phone", "url",
		"combobox", "encryptedstring":
		return nullable(synclet.String), true
	case "boolean":
		return nullable(boolean), true
	case "int", "long":
		return nullable(synclet.Integer), true
	case "double", "currency", "percent":
		return nullable(synclet.Number), true
	case "date":
		p := nullable(synclet.String)
		p.Format = synclet.Date
		return p, true
	case "datetime":
		p := nullable(synclet.String)
		p.Format = synclet.DateTime
		p.AirbyteType = synclet.TimestampWithTZ
		return p, true
	case "time":
		p := nullable(synclet.String)
		p.Format = synclet.Time
		return p, true
	case "address", "location":
		return nullable(synclet.Object), true
	}
	return synclet.PropertySpec{}, true
}
