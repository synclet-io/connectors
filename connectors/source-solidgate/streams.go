package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	streamSubscriptions        = "subscriptions"
	streamSubscriptionInvoices = "subscription_invoices"
	streamProducts             = "products"
	streamCardOrders           = "card_orders"
	streamAPMOrders            = "apm_orders"
	streamChargebacks          = "chargebacks"
	streamPayPalDisputes       = "paypal_disputes"
	streamCardFraudAlerts      = "card_fraud_alerts"
	streamFinancialEntries     = "financial_entries"

	accountField = "solid_account"
	// apiTimeLayout is how Solidgate writes timestamps, in UTC.
	apiTimeLayout = "2006-01-02 15:04:05"
	// utcTimeLayout is how records carry a UTC timestamp.
	utcTimeLayout = "2006-01-02T15:04:05+00:00"
	// localTimeLayout is how records carry a timestamp without a zone.
	localTimeLayout = "2006-01-02T15:04:05"
)

// kind is the JSON type of a record field and how its value is taken from
// the API object. A field the API object lacks is null whatever its kind;
// only key and cursor fields are declared non-null.
type kind int

const (
	kString        kind = iota // string: a key field
	kStringNull                // string or null
	kIntegerNull               // integer or null
	kNumberNull                // number or null
	kBooleanNull               // boolean or null
	kDateTime                  // date-time string from an API timestamp: a cursor field
	kDateTimeNull              // date-time string or null
	kNested                    // array or object of the field's nested schema, passed through
	kLocalTimeNull             // CSV timestamp without a zone, or null; no date-time format, which needs an offset
	kUTCTimeNull               // CSV timestamp in UTC, emitted with +00:00, or null
	kDateNull                  // CSV date, or null
)

// field is one record field: its name and the dotted path of its value in
// the API object.
type field struct {
	name   string
	path   string
	kind   kind
	nested jsonSchema
}

func f(name string, k kind) field          { return field{name: name, path: name, kind: k} }
func from(name, path string, k kind) field { return field{name: name, path: path, kind: k} }

// nested is a field whose array or object value is passed through as the API
// sends it, described by schema.
func nested(name string, schema jsonSchema) field {
	return field{name: name, path: name, kind: kNested, nested: schema}
}

// jsonSchema is a JSON schema object.
type jsonSchema = map[string]any

// typeOf is the "type" keyword: a plain string for one type, a list for
// several.
func typeOf(types ...string) any {
	if len(types) == 1 {
		return types[0]
	}
	return types
}

func nullable(t string) jsonSchema { return jsonSchema{"type": typeOf("null", t)} }

// objectOf is a non-null object with props.
func objectOf(props jsonSchema) jsonSchema { return jsonSchema{"type": "object", "properties": props} }

// nullableObject is an object with props, or null.
func nullableObject(props jsonSchema) jsonSchema {
	return jsonSchema{"type": typeOf("null", "object"), "properties": props}
}

// nullableArrayOf is an array of objects with props, or null.
func nullableArrayOf(props jsonSchema) jsonSchema {
	return jsonSchema{"type": typeOf("null", "array"), "items": objectOf(props)}
}

func (fd field) schema() jsonSchema {
	switch fd.kind {
	case kString:
		return jsonSchema{"type": "string"}
	case kStringNull:
		return nullable("string")
	case kIntegerNull:
		return nullable("integer")
	case kNumberNull:
		return nullable("number")
	case kBooleanNull:
		return nullable("boolean")
	case kDateTime:
		return jsonSchema{"type": "string", "format": "date-time"}
	case kDateTimeNull:
		return jsonSchema{"type": typeOf("null", "string"), "format": "date-time"}
	case kNested:
		return fd.nested
	case kLocalTimeNull:
		return jsonSchema{"type": typeOf("null", "string"), "format": "date-time", "airbyte_type": string(synclet.TimestampWOTZ)}
	case kUTCTimeNull:
		return jsonSchema{"type": typeOf("null", "string"), "format": "date-time", "airbyte_type": string(synclet.TimestampWithTZ)}
	case kDateNull:
		return jsonSchema{"type": typeOf("null", "string"), "format": "date"}
	}
	panic(fmt.Sprintf("field %s: unknown kind %d", fd.name, fd.kind))
}

// value returns the record value of fd in obj, nil when obj lacks it.
func (fd field) value(obj map[string]any) (any, error) {
	v := lookup(obj, fd.path)
	switch fd.kind {
	case kDateTime, kDateTimeNull, kUTCTimeNull:
		return convertTime(fd.name, v, apiTimeLayout, utcTimeLayout)
	case kLocalTimeNull:
		return convertTime(fd.name, v, apiTimeLayout, localTimeLayout)
	case kDateNull:
		return convertTime(fd.name, v, dateLayout, dateLayout)
	}
	return v, nil
}

// convertTime parses v strictly with layout and formats it with out; an
// error names the field but never the value.
func convertTime(name string, v any, layout, out string) (any, error) {
	if v == nil || v == "" {
		return nil, nil
	}
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("field %s: value is not a string", name)
	}
	t, err := time.Parse(layout, s)
	if err != nil {
		return nil, fmt.Errorf("field %s: value does not match %s", name, layoutName[layout])
	}
	return t.Format(out), nil
}

var layoutName = map[string]string{apiTimeLayout: "YYYY-MM-DD HH:MM:SS", dateLayout: "YYYY-MM-DD"}

// lookup returns the value at the dotted path in obj, nil when any part of
// it is missing or null.
func lookup(obj map[string]any, path string) any {
	var cur any = obj
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[part]
	}
	return cur
}

// record builds the record of obj for account.
func record(fields []field, obj map[string]any, account string) (map[string]any, error) {
	rec := make(map[string]any, len(fields))
	for _, fd := range fields {
		if fd.name == accountField {
			rec[fd.name] = account
			continue
		}
		v, err := fd.value(obj)
		if err != nil {
			return nil, err
		}
		rec[fd.name] = v
	}
	return rec, nil
}

func idAccountKey() [][]string { return [][]string{{"id"}, {accountField}} }

// streamDef is one stream of the catalog.
type streamDef struct {
	name   string
	fields []field
	pk     [][]string
	// cursor is empty for a full-refresh-only stream.
	cursor string
	// report is the reports API path and the extraction of its objects; nil
	// for products and financial_entries.
	report *report
	// additional sets additionalProperties: true on the stream schema.
	additional bool
}

var (
	nullString  = nullable("string")
	nullInteger = nullable("integer")

	// metadataSchema is the merchant's key-value metadata of an order or a
	// product.
	metadataSchema = jsonSchema{"type": typeOf("null", "object"), "additionalProperties": nullString}

	// billingPeriodSchema is the billing period of a product or of its trial.
	billingPeriodSchema = nullableObject(jsonSchema{"unit": nullString, "value": nullInteger})

	productTrialSchema = nullableObject(jsonSchema{
		"billing_period": billingPeriodSchema, "payment_action": nullString, "settle_interval": nullInteger,
	})

	productTaxSchema = nullableObject(jsonSchema{"category_id": nullString, "behavior": nullString})

	routingSchema = nullableObject(jsonSchema{
		"cascade_steps": nullableArrayOf(jsonSchema{
			"mid": nullString, "mid_descriptor": nullString, "route_id": nullString,
			"cascade_number": nullInteger, "segment_id": nullString, "connector_account_id": nullString,
		}),
	})

	cardTransactionsSchema = nullableArrayOf(jsonSchema{
		"id": nullString, "created_at": nullString, "updated_at": nullString, "amount": nullInteger,
		"currency": nullString, "operation": nullString, "status": nullString, "authorization_type": nullString,
		"original_payment_method": nullString, "descriptor": nullString, "refund_reason": nullString,
		"refund_reason_code": nullString,
		"card": nullableObject(jsonSchema{
			"bank": nullString, "bin": nullString, "brand": nullString, "card_exp_month": nullString,
			"card_exp_year": nullInteger, "card_holder": nullString, "card_type": nullString,
			"card_id": nullString, "country": nullString, "number": nullString,
		}),
	})

	apmTransactionsSchema = nullableArrayOf(jsonSchema{
		"id": nullString, "psp_transaction_id": nullString, "created_at": nullString, "updated_at": nullString,
		"method": nullString, "amount": nullInteger, "currency": nullString, "status": nullString, "type": nullString,
	})

	chargebackFlowsSchema = nullableArrayOf(jsonSchema{
		"id": nullString, "date": nullString, "created_at": nullString, "updated_at": nullString,
		"settlement_date": nullString, "deadline_date": nullString, "amount": nullInteger,
		"dispute_amount": nullInteger, "currency": nullString, "type": nullString, "status": nullString,
		"represented_by": nullString, "arn_code": nullString,
	})

	invoiceOrdersSchema = nullableArrayOf(jsonSchema{
		"id": nullString, "status": nullString, "created_at": nullString, "processed_at": nullString,
		"amount": nullInteger, "operation": nullString, "retry_attempt": nullInteger,
		"payment_details": nullableObject(jsonSchema{
			"invoice_id": nullString, "payer_email": nullString, "retry_attempt": nullInteger,
		}),
	})
)

var subscriptionFields = []field{
	f("id", kString), f(accountField, kString), f("status", kStringNull), f("updated_at", kDateTime),
	f("started_at", kDateTimeNull), f("expired_at", kDateTimeNull), f("next_charge_at", kDateTimeNull),
	f("payment_type", kStringNull), f("trial", kBooleanNull), f("cancelled_at", kDateTimeNull),
	f("cancel_code", kStringNull), f("cancel_message", kStringNull),
	from("customer_account_id", "customer.customer_account_id", kStringNull),
	from("customer_email", "customer.customer_email", kStringNull),
	from("product_id", "product.id", kStringNull), from("product_name", "product.name", kStringNull),
	from("product_payment_action", "product.payment_action", kStringNull),
	from("product_amount", "product.amount", kNumberNull), from("product_currency", "product.currency", kStringNull),
	from("product_trial", "product.trial", kBooleanNull), from("product_trial_period", "product.trial_period", kIntegerNull),
	from("pause_from_date", "pause.from_date", kDateTimeNull), from("pause_to_date", "pause.to_date", kDateTimeNull),
}

var subscriptionInvoiceFields = []field{
	f("id", kString), f(accountField, kString), f("subscription_id", kStringNull), f("status", kStringNull),
	f("created_at", kDateTimeNull), f("updated_at", kDateTime), f("amount", kNumberNull),
	f("billing_period_started_at", kDateTimeNull), f("billing_period_ended_at", kDateTimeNull),
	f("subscription_term_number", kIntegerNull), nested("orders", invoiceOrdersSchema),
}

var productFields = []field{
	f("id", kString), f(accountField, kString), f("name", kStringNull), f("type", kStringNull),
	f("description", kStringNull), f("public_description", kStringNull), f("status", kStringNull),
	f("created_at", kDateTimeNull), f("updated_at", kDateTimeNull), f("term_length", kIntegerNull),
	f("payment_action", kStringNull), f("settle_interval", kIntegerNull), nested("billing_period", billingPeriodSchema),
	nested("trial", productTrialSchema), f("retry_mode", kStringNull), f("retry_strategy_id", kStringNull),
	from("tax_profile_id", "tax.profile_id", kStringNull), from("tax_category_id", "tax.category_id", kStringNull),
	from("tax_is_included", "tax.is_included", kBooleanNull), nested("tax", productTaxSchema),
	nested("metadata", metadataSchema),
}

var cardOrderFields = []field{
	from("id", "order_id", kString), f(accountField, kString), f("order_description", kStringNull),
	f("psp_order_id", kStringNull), f("provider_payment_id", kStringNull), f("product_id", kStringNull),
	f("product_name", kStringNull), f("product_type", kStringNull), f("subscription_id", kStringNull),
	f("created_at", kDateTimeNull), f("updated_at", kDateTime), f("amount", kIntegerNull), f("currency", kStringNull),
	f("processing_amount", kIntegerNull), f("processing_currency", kStringNull),
	f("settled_amount", kIntegerNull), f("settled_currency", kStringNull),
	f("refunded_amount", kIntegerNull), f("refunded_currency", kStringNull), f("status", kStringNull),
	f("refund", kStringNull), f("payment_type", kStringNull), f("payment_method", kStringNull), f("type", kStringNull),
	f("is_secured", kBooleanNull), f("customer_account_id", kStringNull), f("customer_email", kStringNull),
	f("customer_first_name", kStringNull), f("customer_last_name", kStringNull), f("ip_address", kStringNull),
	f("mid", kStringNull), f("traffic_source", kStringNull), f("platform", kStringNull), f("geo_country", kStringNull),
	f("error_code", kStringNull), nested("routing", routingSchema), nested("transactions", cardTransactionsSchema),
	nested("order_metadata", metadataSchema), f("fraudulent", kBooleanNull),
}

var apmOrderFields = []field{
	from("id", "order_id", kString), f(accountField, kString), f("order_description", kStringNull),
	f("product_id", kStringNull), f("product_name", kStringNull), f("product_type", kStringNull),
	f("subscription_id", kStringNull), f("created_at", kDateTimeNull), f("updated_at", kDateTime),
	f("method", kStringNull), f("amount", kIntegerNull), f("currency", kStringNull),
	f("processing_amount", kIntegerNull), f("processing_currency", kStringNull),
	f("settled_amount", kIntegerNull), f("settled_currency", kStringNull),
	f("refunded_amount", kIntegerNull), f("refunded_currency", kStringNull), f("status", kStringNull),
	f("refund", kStringNull), f("customer_account_id", kStringNull), f("customer_email", kStringNull),
	f("ip_address", kStringNull), f("geo_country", kStringNull), f("error_code", kStringNull),
	f("merchant_account_id", kStringNull), nested("transactions", apmTransactionsSchema),
	nested("order_metadata", metadataSchema),
}

var chargebackFields = []field{
	f("id", kString), f(accountField, kString), f("order_id", kStringNull),
	f("created_at", kDateTimeNull), f("updated_at", kDateTime), f("dispute_date", kDateTimeNull),
	f("settlement_date", kDateTimeNull), f("amount", kIntegerNull), f("currency", kStringNull),
	f("type", kStringNull), f("status", kStringNull), f("reason_group", kStringNull),
	f("reason_code", kStringNull), f("reason_description", kStringNull), nested("flows", chargebackFlowsSchema),
}

var paypalDisputeFields = []field{
	from("id", "dispute_id", kString), f(accountField, kString), f("order_id", kStringNull),
	f("subscription_product_id", kStringNull), f("subscription_service_id", kStringNull),
	f("created_at", kDateTimeNull), f("updated_at", kDateTime), f("seller_response_due_date", kDateTimeNull),
	f("dispute_amount", kIntegerNull), f("dispute_currency", kStringNull), f("dispute_life_cycle_stage", kStringNull),
	f("dispute_channel", kStringNull), f("dispute_create_time", kDateTimeNull), f("dispute_update_time", kDateTimeNull),
	f("dispute_outcome", kStringNull), f("reason", kStringNull), f("status", kStringNull),
	f("represented_by", kStringNull), f("customer_account_id", kStringNull), f("customer_email", kStringNull),
}

var cardFraudAlertFields = []field{
	f("id", kIntegerNull), f("order_id", kString), f(accountField, kString),
	f("created_at", kDateTimeNull), f("updated_at", kDateTimeNull),
	f("fraud_amount", kIntegerNull), f("fraud_currency", kStringNull), f("fraud_amount_usd", kIntegerNull),
	f("fraud_report_date", kDateTimeNull), f("fraud_type", kStringNull), f("card_scheme", kStringNull),
	f("reason_code_description", kStringNull),
}

var streamDefs = []streamDef{
	{name: streamSubscriptions, fields: subscriptionFields, pk: idAccountKey(), cursor: "updated_at",
		report: &report{path: "subscriptions", objects: subscriptionObjects}},
	{name: streamSubscriptionInvoices, fields: subscriptionInvoiceFields, pk: idAccountKey(), cursor: "updated_at",
		report: &report{path: "subscriptions", objects: invoiceObjects}},
	{name: streamProducts, fields: productFields, pk: idAccountKey()},
	{name: streamCardOrders, fields: cardOrderFields, pk: idAccountKey(), cursor: "updated_at",
		report: &report{path: "card-orders", objects: listObjects("orders")}},
	{name: streamAPMOrders, fields: apmOrderFields, pk: idAccountKey(), cursor: "updated_at",
		report: &report{path: "apm-orders", objects: listObjects("orders")}},
	{name: streamChargebacks, fields: chargebackFields, pk: idAccountKey(), cursor: "updated_at",
		report: &report{path: "card-orders/chargebacks", objects: chargebackObjects}},
	{name: streamPayPalDisputes, fields: paypalDisputeFields, pk: idAccountKey(), cursor: "updated_at",
		report: &report{path: "apm-orders/paypal-disputes", objects: listObjects("disputes")}},
	{name: streamCardFraudAlerts, fields: cardFraudAlertFields, pk: [][]string{{"order_id"}, {"created_at"}, {accountField}}, cursor: "updated_at",
		report: &report{path: "card-orders/fraud-alerts", objects: listObjects("alerts")}},
	{name: streamFinancialEntries, fields: financialEntryFields, pk: idAccountKey(), cursor: "created_at", additional: true},
}

func streamDefByName(name string) (streamDef, bool) {
	for _, d := range streamDefs {
		if d.name == name {
			return d, true
		}
	}
	return streamDef{}, false
}

func (d streamDef) stream() synclet.Stream {
	props := make(jsonSchema, len(d.fields))
	for _, fd := range d.fields {
		props[fd.name] = fd.schema()
	}
	schema := objectOf(props)
	if d.additional {
		schema["additionalProperties"] = true
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		panic(fmt.Sprintf("stream %s: schema: %v", d.name, err))
	}
	s := synclet.Stream{
		Name:                    d.name,
		JSONSchema:              raw,
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: d.pk,
	}
	if d.cursor != "" {
		s.SupportedSyncModes = append(s.SupportedSyncModes, synclet.SyncModeIncremental)
		s.SourceDefinedCursor = true
		s.DefaultCursorField = []string{d.cursor}
	}
	return s
}
