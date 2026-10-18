package main

import (
	"fmt"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// shape is how a stream's records come out of the objects a query returns.
type shape int

const (
	// shapeRoot records are the root objects, each with the children of its
	// nested connection embedded as an array.
	shapeRoot shape = iota
	// shapeList records are the elements of a list field of each order,
	// with the order's id and update time.
	shapeList
	// shapeChildren records are the children of each root's nested
	// connection, with the root's id.
	shapeChildren
)

// stream is a stream of the connector and the query that reads it.
type stream struct {
	name string
	// scopes are the access scopes the stream needs, all of them.
	scopes []string
	// bulk streams run as bulk operations, the others page through a
	// connection, or read one object when single.
	bulk   bool
	single bool
	// root is the root query field; args its fixed arguments.
	root string
	args string
	// op is the operation name of a paged stream's query.
	op string
	// windowed streams are filtered by updated_at and read incrementally.
	windowed bool
	shape    shape
	fields   []field
	// nested is a nested connection of each root: embedded as an array for
	// shapeRoot, the records for shapeChildren.
	nested *connection
	// list is the list field of each order whose elements are the records
	// of a shapeList stream.
	list string
	// parent names the parent id field of shapeList and shapeChildren
	// records: order or location.
	parent string
}

var orderFields = []field{
	id(), str("name"), str("email"), str("phone"), datetime("createdAt"), datetime("updatedAt"), datetime("processedAt"),
	datetime("closedAt"), datetime("cancelledAt"), str("cancelReason"), str("displayFinancialStatus"),
	str("displayFulfillmentStatus"), str("currencyCode"), str("presentmentCurrencyCode"), flag("test"),
	flag("confirmed"), stringList("tags"), str("note"), str("sourceName"), ref("customer"), ref("app"),
	moneyBag("subtotalPriceSet"), moneyBag("totalPriceSet"), moneyBag("totalTaxSet"), moneyBag("totalDiscountsSet"),
	moneyBag("totalShippingPriceSet"), moneyBag("totalRefundedSet"), moneyBag("currentTotalPriceSet"),
	flag("taxesIncluded"), address("shippingAddress"), address("billingAddress"),
}

var lineItemFields = []field{
	id(), str("name"), str("title"), str("variantTitle"), str("sku"), str("vendor"), integer("quantity"),
	integer("currentQuantity"), flag("taxable"), flag("requiresShipping"), ref("variant"), ref("product"),
	moneyBag("originalUnitPriceSet"), moneyBag("originalTotalSet"), moneyBag("discountedTotalSet"), moneyBag("totalDiscountSet"),
}

var streams = []stream{
	{
		name: "shop", single: true, root: "shop", op: "Shop",
		fields: []field{
			id(), str("name"), str("email"), str("contactEmail"), str("myshopifyDomain"), object("primaryDomain", str("host"), str("url")),
			str("currencyCode"), str("ianaTimezone"), str("weightUnit"), flag("taxesIncluded"),
			object("plan", str("publicDisplayName"), flag("partnerDevelopment"), flag("shopifyPlus")),
			datetime("createdAt"), datetime("updatedAt"),
		},
	},
	{
		name: "products", scopes: []string{"read_products"}, bulk: true, root: "products", windowed: true,
		fields: []field{
			id(), str("legacyResourceId"), str("title"), str("handle"), str("status"), str("vendor"), str("productType"),
			stringList("tags"), str("descriptionHtml"), integer("totalInventory"), flag("tracksInventory"),
			list("options", "", id(), str("name"), integer("position"), stringList("values")),
			datetime("createdAt"), datetime("updatedAt"), datetime("publishedAt"),
		},
	},
	{
		name: "product_variants", scopes: []string{"read_products"}, bulk: true, root: "productVariants", windowed: true,
		fields: []field{
			id(), str("legacyResourceId"), str("title"), str("displayName"), str("sku"), str("barcode"), decimal("price"),
			decimal("compareAtPrice"), integer("position"), integer("inventoryQuantity"), flag("taxable"),
			flag("availableForSale"), ref("product"), ref("inventoryItem"), list("selectedOptions", "", str("name"), str("value")),
			datetime("createdAt"), datetime("updatedAt"),
		},
	},
	{
		name: "customers", scopes: []string{"read_customers"}, bulk: true, root: "customers", windowed: true,
		fields: []field{
			id(), str("legacyResourceId"), str("firstName"), str("lastName"), str("displayName"),
			object("defaultEmailAddress", str("emailAddress"), str("marketingState")),
			object("defaultPhoneNumber", str("phoneNumber"), str("marketingState")),
			str("state"), stringList("tags"), str("note"), flag("verifiedEmail"), flag("taxExempt"), str("locale"),
			str("numberOfOrders"), money("amountSpent"), datetime("createdAt"), datetime("updatedAt"),
		},
	},
	{
		name: "orders", scopes: []string{"read_orders"}, bulk: true, root: "orders", windowed: true,
		fields: orderFields,
		nested: &connection{name: "lineItems", fields: lineItemFields},
	},
	{
		name: "refunds", scopes: []string{"read_orders"}, bulk: true, root: "orders", windowed: true,
		shape: shapeList, list: "refunds", parent: "order",
		fields: []field{id(), str("note"), datetime("createdAt"), datetime("updatedAt"), moneyBag("totalRefundedSet")},
	},
	{
		name: "fulfillments", scopes: []string{"read_orders"}, bulk: true, root: "orders", windowed: true,
		shape: shapeList, list: "fulfillments", parent: "order",
		fields: []field{
			id(), str("name"), str("status"), str("displayStatus"), datetime("createdAt"), datetime("updatedAt"),
			datetime("deliveredAt"), datetime("estimatedDeliveryAt"), ref("location"),
			list("trackingInfo", "", str("company"), str("number"), str("url")),
		},
	},
	{
		name: "transactions", scopes: []string{"read_orders"}, bulk: true, root: "orders", windowed: true,
		shape: shapeList, list: "transactions", parent: "order",
		fields: []field{
			id(), str("kind"), str("status"), str("gateway"), flag("test"), str("errorCode"), str("authorizationCode"),
			datetime("createdAt"), datetime("processedAt"), moneyBag("amountSet"), ref("parentTransaction"),
		},
	},
	{
		name: "inventory_items", scopes: []string{"read_inventory"}, bulk: true, root: "inventoryItems", windowed: true,
		fields: []field{
			id(), str("legacyResourceId"), str("sku"), flag("tracked"), flag("requiresShipping"), str("countryCodeOfOrigin"),
			str("provinceCodeOfOrigin"), str("harmonizedSystemCode"), money("unitCost"), ref("variant"),
			datetime("createdAt"), datetime("updatedAt"),
		},
	},
	{
		name: "inventory_levels", scopes: []string{"read_inventory", "read_locations"}, bulk: true, root: "locations",
		args: "includeInactive: true, includeLegacy: true", shape: shapeChildren, parent: "location",
		nested: &connection{name: "inventoryLevels", fields: []field{
			id(), ref("item"),
			list("quantities", `names: ["available", "committed", "damaged", "incoming", "on_hand", "quality_control", "reserved", "safety_stock"]`,
				str("name"), integer("quantity")),
			datetime("createdAt"), datetime("updatedAt"),
		}},
	},
	{
		name: "locations", scopes: []string{"read_locations"}, root: "locations", op: "Locations",
		args: "includeInactive: true, includeLegacy: true",
		fields: []field{
			id(), str("legacyResourceId"), str("name"), flag("isActive"), flag("fulfillsOnlineOrders"), flag("hasActiveInventory"),
			flag("shipsInventory"), address("address"), datetime("createdAt"), datetime("updatedAt"),
		},
	},
	{
		name: "collections", scopes: []string{"read_products"}, root: "collections", op: "Collections", windowed: true,
		fields: []field{
			id(), str("legacyResourceId"), str("title"), str("handle"), str("descriptionHtml"), str("sortOrder"),
			object("ruleSet", flag("appliedDisjunctively"), list("rules", "", str("column"), str("relation"), str("condition"))),
			count("productsCount"), datetime("updatedAt"),
		},
	},
	{
		name: "companies", scopes: []string{"read_customers"}, root: "companies", op: "Companies", windowed: true,
		fields: []field{
			id(), str("name"), str("externalId"), str("note"), datetime("customerSince"), ref("mainContact"),
			count("contactsCount"), count("locationsCount"), count("ordersCount"), money("totalSpent"),
			datetime("createdAt"), datetime("updatedAt"),
		},
	},
	{
		name: "company_locations", scopes: []string{"read_customers"}, root: "companyLocations", op: "CompanyLocations", windowed: true,
		fields: []field{
			id(), str("name"), str("externalId"), str("note"), str("currency"), str("locale"), str("phone"), ref("company"),
			companyAddress("billingAddress"), companyAddress("shippingAddress"), count("ordersCount"), money("totalSpent"),
			datetime("createdAt"), datetime("updatedAt"),
		},
	},
}

// connection is a nested connection of each root of a bulk query.
type connection struct {
	name   string
	fields []field
}

// b2b reports whether a stream reads B2B data, which only Shopify Plus
// stores have.
func (s stream) b2b() bool { return s.root == "companies" || s.root == "companyLocations" }

func findStream(name string) (stream, bool) {
	for _, s := range streams {
		if s.name == name {
			return s, true
		}
	}
	return stream{}, false
}

// cursor is the record field an incremental read of the stream is
// ordered by, empty for full-refresh-only streams.
func (s stream) cursor() string {
	switch {
	case !s.windowed:
		return ""
	case s.shape == shapeList:
		return "order_updated_at"
	}
	return "updatedAt"
}

// recordFields are the fields of a record of the stream.
func (s stream) recordFields() []field {
	var out []field
	switch s.shape {
	case shapeRoot:
		out = append(out, s.fields...)
		if s.nested != nil {
			out = append(out, list(s.nested.name, "", s.nested.fields...))
		}
	case shapeList:
		out = append(out, s.fields...)
		out = append(out, scalar("order_id", synclet.PropertySpec{PropertyType: typed(synclet.String)}), datetime("order_updated_at"))
	case shapeChildren:
		out = append(out, s.nested.fields...)
		out = append(out, scalar("location_id", synclet.PropertySpec{PropertyType: typed(synclet.String)}))
	}
	return out
}

func (s stream) schema() map[synclet.PropertyName]synclet.PropertySpec {
	return properties(s.recordFields())
}

// window is a range [from, to) of update times; to is zero for a window
// without an upper bound.
type window struct{ from, to time.Time }

const filterLayout = "2006-01-02T15:04:05Z"

func (w window) String() string {
	if w.to.IsZero() {
		return "[" + w.from.Format(filterLayout) + ", now]"
	}
	return "[" + w.from.Format(filterLayout) + ", " + w.to.Format(filterLayout) + ")"
}

// filter is the search query of the updates in w.
func (w window) filter() string {
	if w.to.IsZero() {
		return fmt.Sprintf("updated_at:>='%s'", w.from.Format(filterLayout))
	}
	return fmt.Sprintf("updated_at:>='%s' AND updated_at:<'%s'", w.from.Format(filterLayout), w.to.Format(filterLayout))
}

// bulkQuery is the query a bulk operation of the stream runs, filtered to
// w when it is not nil. Nested connections are selected as edges, as bulk
// queries require.
func (s stream) bulkQuery(w *window) string {
	var args []string
	if s.args != "" {
		args = append(args, s.args)
	}
	if w != nil {
		args = append(args, fmt.Sprintf("query: %q", w.filter()))
	}
	root := s.root
	if len(args) > 0 {
		root += "(" + strings.Join(args, ", ") + ")"
	}
	var node string
	switch s.shape {
	case shapeRoot:
		node = selection(s.fields)
		if s.nested != nil {
			node += " " + s.nested.name + " { edges { node { " + selection(s.nested.fields) + " } } }"
		}
	case shapeList:
		node = "id updatedAt " + s.list + " { " + selection(s.fields) + " }"
	case shapeChildren:
		node = "id " + s.nested.name + " { edges { node { " + selection(s.nested.fields) + " } } }"
	}
	return "{ " + root + " { edges { node { " + node + " } } } }"
}
