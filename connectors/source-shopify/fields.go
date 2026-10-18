package main

import (
	"encoding/json"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// boolean is the JSON schema type the SDK has no constant for.
const boolean synclet.PropType = "boolean"

// field is a record field: its name, the GraphQL selection that fetches
// it and its JSON schema.
type field struct {
	name string
	sel  string
	spec synclet.PropertySpec
}

func nullable(t synclet.PropType) synclet.PropertySpec {
	return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Null, t}}}
}

func scalar(name string, spec synclet.PropertySpec) field {
	return field{name: name, sel: name, spec: spec}
}

func id() field {
	return field{name: "id", sel: "id", spec: synclet.PropertySpec{PropertyType: typed(synclet.String)}}
}

func str(name string) field { return scalar(name, nullable(synclet.String)) }

func integer(name string) field { return scalar(name, nullable(synclet.Integer)) }

func flag(name string) field { return scalar(name, nullable(boolean)) }

// decimal is a Decimal or Money scalar: Shopify sends it as a string, the
// record carries it as a JSON number with the same digits.
func decimal(name string) field { return scalar(name, nullable(synclet.Number)) }

func datetime(name string) field {
	p := nullable(synclet.String)
	p.Format = synclet.DateTime
	return scalar(name, p)
}

func stringList(name string) field {
	p := nullable(synclet.Array)
	p.Items = map[string]any{"type": "string"}
	return scalar(name, p)
}

func properties(sub []field) map[synclet.PropertyName]synclet.PropertySpec {
	props := make(map[synclet.PropertyName]synclet.PropertySpec, len(sub))
	for _, f := range sub {
		props[synclet.PropertyName(f.name)] = f.spec
	}
	return props
}

func selection(sub []field) string {
	sels := make([]string, len(sub))
	for i, f := range sub {
		sels[i] = f.sel
	}
	return strings.Join(sels, " ")
}

// object is a nullable object field with the given sub-fields.
func object(name string, sub ...field) field {
	p := nullable(synclet.Object)
	p.Properties = properties(sub)
	return field{name: name, sel: name + " { " + selection(sub) + " }", spec: p}
}

// list is a nullable array field of objects with the given sub-fields.
// args, when set, are the field's GraphQL arguments.
func list(name, args string, sub ...field) field {
	item := synclet.PropertySpec{PropertyType: typed(synclet.Object), Properties: properties(sub)}
	p := nullable(synclet.Array)
	p.Items = specMap(item)
	sel := name
	if args != "" {
		sel += "(" + args + ")"
	}
	return field{name: name, sel: sel + " { " + selection(sub) + " }", spec: p}
}

// ref is an object field holding only the id of another object.
func ref(name string) field { return object(name, id()) }

func money(name string) field { return object(name, decimal("amount"), str("currencyCode")) }

func moneyBag(name string) field {
	return object(name,
		object("shopMoney", decimal("amount"), str("currencyCode")),
		object("presentmentMoney", decimal("amount"), str("currencyCode")))
}

func count(name string) field { return object(name, integer("count")) }

func address(name string) field {
	return object(name, str("address1"), str("address2"), str("city"), str("province"), str("provinceCode"),
		str("country"), str("countryCode"), str("zip"), str("phone"))
}

// companyAddress is a CompanyAddress, which has a zoneCode where other
// addresses have a provinceCode.
func companyAddress(name string) field {
	return object(name, str("address1"), str("address2"), str("city"), str("province"), str("zoneCode"),
		str("country"), str("countryCode"), str("zip"), str("phone"))
}

// specMap is spec as the map PropertySpec.Items takes.
func specMap(spec synclet.PropertySpec) map[string]any {
	raw, err := json.Marshal(spec)
	if err != nil {
		panic(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		panic(err)
	}
	return m
}
