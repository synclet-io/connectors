package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// boolean is the JSON schema type the SDK has no constant for.
const boolean synclet.PropType = "boolean"

const (
	streamProjects                   = "projects"
	streamApps                       = "apps"
	streamProducts                   = "products"
	streamEntitlements               = "entitlements"
	streamOfferings                  = "offerings"
	streamCustomers                  = "customers"
	streamCustomerSubscriptions      = "customer_subscriptions"
	streamCustomerPurchases          = "customer_purchases"
	streamCustomerActiveEntitlements = "customer_active_entitlements"
	streamMetricsOverview            = "metrics_overview"
)

// streamDef is one stream. path holds {project} and {customer} for the
// project and parent customer ids. A child stream is read once per
// customer; key names the array of records in a single-document response,
// empty for a list. environment marks the endpoints that take the
// environment filter.
type streamDef struct {
	name        string
	path        string
	domain      domain
	permission  string
	pk          [][]string
	child       bool
	key         string
	environment bool
	props       map[synclet.PropertyName]synclet.PropertySpec
}

// pathFor returns the stream's path with the escaped project and customer
// ids.
func (d streamDef) pathFor(project, customer string) string {
	return strings.NewReplacer("{project}", url.PathEscape(project), "{customer}", url.PathEscape(customer)).Replace(d.path)
}

// projectScoped reports whether the stream's path names the project.
func (d streamDef) projectScoped() bool { return strings.Contains(d.path, "{project}") }

func prop(t synclet.PropType, description string) synclet.PropertySpec {
	return synclet.PropertySpec{Description: description, PropertyType: synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}}
}

func key(description string) synclet.PropertySpec {
	return synclet.PropertySpec{Description: description, PropertyType: typed(synclet.String)}
}

func epochMS(description string) synclet.PropertySpec {
	return prop(synclet.Integer, description+", in milliseconds since the Unix epoch")
}

func str(description string) synclet.PropertySpec { return prop(synclet.String, description) }
func obj(description string) synclet.PropertySpec { return prop(synclet.Object, description) }

var streams = []streamDef{
	{
		name: streamProjects, path: "projects", domain: configDomain,
		permission: "project_configuration:projects:read", pk: [][]string{{"id"}},
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":         str("Always project"),
			"id":             key("ID of the project"),
			"name":           str("Name of the project"),
			"created_at":     epochMS("When the project was created"),
			"icon_url":       str("URL of the project's small icon"),
			"icon_url_large": str("URL of the project's large icon"),
		},
	},
	{
		name: streamApps, path: "projects/{project}/apps", domain: configDomain,
		permission: "project_configuration:apps:read", pk: [][]string{{"id"}},
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":            str("Always app"),
			"id":                key("ID of the app"),
			"name":              str("Name of the app"),
			"created_at":        epochMS("When the app was created"),
			"type":              str("Platform of the app, such as app_store, play_store, stripe or rc_billing"),
			"project_id":        str("ID of the project"),
			"custom_url_scheme": str("Custom URL scheme of the app"),
			"amazon":            obj("Amazon details, for type amazon"),
			"app_store":         obj("App Store details, for type app_store"),
			"mac_app_store":     obj("Legacy Mac App Store details, for type mac_app_store"),
			"play_store":        obj("Google Play details, for type play_store"),
			"stripe":            obj("Stripe details, for type stripe"),
			"rc_billing":        obj("Web Billing details, for type rc_billing"),
			"roku":              obj("Roku details, for type roku"),
			"paddle":            obj("Paddle details, for type paddle"),
			"test_store":        obj("Test Store details, for type test_store"),
		},
	},
	{
		name: streamProducts, path: "projects/{project}/products", domain: configDomain,
		permission: "project_configuration:products:read", pk: [][]string{{"id"}},
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":           str("Always product"),
			"id":               key("ID of the product"),
			"state":            str("active or inactive (archived)"),
			"store_identifier": str("Product identifier in the store"),
			"type":             str("subscription, one_time, consumable, non_consumable or non_renewing_subscription"),
			"subscription":     obj("Durations of a subscription product, ISO 8601"),
			"one_time":         obj("Details of a one-time product"),
			"created_at":       epochMS("When the product was created"),
			"app_id":           str("ID of the app"),
			"app":              obj("The app, when expanded"),
			"indicative_price": obj("Indicative price, when expanded"),
			"display_name":     str("Display name of the product"),
		},
	},
	{
		name: streamEntitlements, path: "projects/{project}/entitlements", domain: configDomain,
		permission: "project_configuration:entitlements:read", pk: [][]string{{"id"}},
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":       str("Always entitlement"),
			"id":           key("ID of the entitlement"),
			"state":        str("active or inactive (archived)"),
			"project_id":   str("ID of the project"),
			"lookup_key":   str("Custom identifier of the entitlement"),
			"display_name": str("Display name of the entitlement"),
			"created_at":   epochMS("When the entitlement was created"),
			"products":     obj("List of the products attached to the entitlement, when expanded"),
		},
	},
	{
		name: streamOfferings, path: "projects/{project}/offerings", domain: configDomain,
		permission: "project_configuration:offerings:read", pk: [][]string{{"id"}},
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":       str("Always offering"),
			"id":           key("ID of the offering"),
			"state":        str("active or inactive (archived)"),
			"lookup_key":   str("Custom identifier of the offering"),
			"display_name": str("Display name of the offering"),
			"is_current":   prop(boolean, "Whether the offering is the current offering"),
			"created_at":   epochMS("When the offering was created"),
			"project_id":   str("ID of the project"),
			"paywall_id":   str("ID of the paywall attached to the offering"),
			"metadata":     obj("Custom metadata of the offering"),
			"packages":     obj("List of the offering's packages, when expanded"),
		},
	},
	{
		name: streamCustomers, path: "projects/{project}/customers", domain: customerDomain,
		permission: "customer_information:customers:read", pk: [][]string{{"id"}},
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":                     str("Always customer"),
			"id":                         key("ID of the customer (app user ID)"),
			"project_id":                 str("ID of the project"),
			"first_seen_at":              epochMS("Earliest date known for the customer"),
			"last_seen_at":               epochMS("When the customer was last seen"),
			"last_seen_app_version":      str("App version the customer was last seen on"),
			"last_seen_country":          str("Country the customer was last seen in"),
			"last_seen_platform":         str("Platform the customer was last seen on"),
			"last_seen_platform_version": str("Platform version the customer was last seen on"),
			"active_entitlements":        obj("List of the customer's active entitlements, when the API includes it"),
			"experiment":                 obj("Experiment enrollment of the customer"),
			"attributes":                 obj("List of the customer's attributes, when the API includes it"),
		},
	},
	{
		name: streamCustomerSubscriptions, path: "projects/{project}/customers/{customer}/subscriptions", domain: customerDomain,
		permission: "customer_information:subscriptions:read", pk: [][]string{{"id"}}, child: true, environment: true,
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":                        str("Always subscription"),
			"id":                            key("ID of the subscription, generated by RevenueCat"),
			"customer_id":                   str("ID of the customer"),
			"original_customer_id":          str("ID of the original customer, for a transferred subscription"),
			"product_id":                    str("ID of the product; null for promotional subscriptions"),
			"starts_at":                     epochMS("When the subscription started"),
			"current_period_starts_at":      epochMS("When the current billing period started"),
			"current_period_ends_at":        epochMS("When the current billing period is expected to end"),
			"ends_at":                       epochMS("When the latest billing period is expected to end"),
			"gives_access":                  prop(boolean, "Whether the customer currently has access to the subscription's entitlements"),
			"pending_payment":               prop(boolean, "Whether a payment is pending"),
			"auto_renewal_status":           str("Auto renewal status, such as will_renew or will_not_renew"),
			"status":                        str("Status, such as trialing, active, expired, in_grace_period or paused"),
			"total_revenue_in_usd":          obj("Total revenue of the subscription in USD: currency, gross, commission, tax, proceeds"),
			"presented_offering_id":         str("ID of the offering the customer saw when subscribing"),
			"entitlements":                  obj("List of the entitlements the subscription grants"),
			"environment":                   str("production or sandbox"),
			"store":                         str("Store of the subscription, such as app_store, play_store or stripe"),
			"store_subscription_identifier": str("Subscription identifier in the store"),
			"ownership":                     str("Ownership of the subscription, such as purchased or family_shared"),
			"pending_changes":               obj("Changes expected at the end of the current period"),
			"country":                       str("Country of the subscription"),
			"management_url":                str("URL to manage the subscription"),
			"redemption":                    obj("Most recent redemption of the purchase's redemption link"),
			"product_change":                obj("Outcome of a product change request"),
		},
	},
	{
		name: streamCustomerPurchases, path: "projects/{project}/customers/{customer}/purchases", domain: customerDomain,
		permission: "customer_information:purchases:read", pk: [][]string{{"id"}}, child: true, environment: true,
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":                    str("Always purchase"),
			"id":                        key("ID of the purchase"),
			"customer_id":               str("ID of the customer"),
			"original_customer_id":      str("ID of the original customer"),
			"product_id":                str("ID of the product purchased"),
			"purchased_at":              epochMS("When the purchase was made"),
			"revenue_in_usd":            obj("Revenue of the purchase in USD: currency, gross, commission, tax, proceeds"),
			"quantity":                  prop(synclet.Integer, "Quantity purchased in one transaction"),
			"status":                    str("owned or refunded"),
			"presented_offering_id":     str("ID of the offering the customer saw when purchasing"),
			"entitlements":              obj("List of the entitlements the purchase grants"),
			"environment":               str("production or sandbox"),
			"store":                     str("Store of the purchase, such as app_store, play_store or stripe"),
			"store_purchase_identifier": str("Purchase identifier in the store"),
			"ownership":                 str("Ownership of the purchase, such as purchased or family_shared"),
			"country":                   str("Country of the purchase"),
			"redemption":                obj("Most recent redemption of the purchase's redemption link"),
		},
	},
	{
		name: streamCustomerActiveEntitlements, path: "projects/{project}/customers/{customer}/active_entitlements", domain: customerDomain,
		permission: "customer_information:customers:read", pk: [][]string{{"customer_id"}, {"entitlement_id"}}, child: true,
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":         str("Always customer.active_entitlement"),
			"customer_id":    key("ID of the customer, from the customer the entitlement was listed for"),
			"entitlement_id": key("ID of the entitlement"),
			"expires_at":     epochMS("When access to the entitlement expires"),
		},
	},
	{
		name: streamMetricsOverview, path: "projects/{project}/metrics/overview", domain: metricsDomain,
		permission: "charts_metrics:overview:read", pk: [][]string{{"id"}}, key: "metrics",
		props: map[synclet.PropertyName]synclet.PropertySpec{
			"object":                  str("Always overview_metric"),
			"id":                      key("ID of the metric, such as active_trials or mrr"),
			"name":                    str("Display name of the metric"),
			"description":             str("Description of the metric"),
			"unit":                    str("Unit of the metric"),
			"period":                  str("Period the metric covers, ISO 8601: P0D for now, P28D for the last 28 days"),
			"value":                   prop(synclet.Number, "Value of the metric"),
			"last_updated_at":         epochMS("When the metric was last updated"),
			"last_updated_at_iso8601": str("When the metric was last updated, as the API formats it (such as 2026-10-18 09:45:00.000000+00:00)"),
			"currency":                str("Currency of the monetary metrics, from the response"),
		},
	},
}

func streamByName(name string) (streamDef, bool) {
	for _, d := range streams {
		if d.name == name {
			return d, true
		}
	}
	return streamDef{}, false
}

// schema returns the stream's JSON schema. additionalProperties is true:
// records are emitted as the API sends them, with fields it adds later.
func (d streamDef) schema() json.RawMessage {
	raw, err := json.Marshal(struct {
		Type                 string                                        `json:"type"`
		Properties           map[synclet.PropertyName]synclet.PropertySpec `json:"properties"`
		AdditionalProperties bool                                          `json:"additionalProperties"`
	}{Type: "object", Properties: d.props, AdditionalProperties: true})
	if err != nil {
		panic(fmt.Sprintf("stream %s: schema: %v", d.name, err))
	}
	return raw
}

// Discover returns the ten streams, all full refresh only: the API has no
// filter by update time. Every project has the same routes, so nothing is
// requested.
func (Source) Discover(_ context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	catalog := &synclet.Catalog{}
	for _, d := range streams {
		catalog.Streams = append(catalog.Streams, synclet.Stream{
			Name:                    d.name,
			JSONSchema:              d.schema(),
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
			SourceDefinedPrimaryKey: d.pk,
		})
	}
	return catalog, nil
}
