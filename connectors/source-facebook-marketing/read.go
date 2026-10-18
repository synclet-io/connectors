package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Read emits every configured stream in catalog order, reading accounts and
// businesses one after another.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		d, ok := streamByName(desc.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		var err error
		switch d.kind {
		case kindInsights:
			err = s.readInsights(ctx, c, cfg, d, cs, state, em)
		case kindAdAccounts:
			err = readAdAccounts(ctx, c, cfg, d, desc, em)
		case kindEdge:
			err = readEdge(ctx, c, cfg, d, desc, em)
		case kindBusinessCredits:
			err = s.readBusinessCredits(ctx, c, cfg, d, desc, em)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func fieldsQuery(names []string) url.Values {
	return url.Values{"fields": {strings.Join(names, ",")}}
}

func readAdAccounts(ctx context.Context, c *apiClient, cfg Config, d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	for _, acct := range cfg.accounts() {
		var obj map[string]any
		if err := c.get(ctx, "act_"+acct, fieldsQuery(d.requestFields()), &obj); err != nil {
			return fmt.Errorf("%s: %w", d.name, err)
		}
		if err := em.Record(desc, record(d.fields, obj)); err != nil {
			return err
		}
	}
	return nil
}

func readEdge(ctx context.Context, c *apiClient, cfg Config, d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	for _, acct := range cfg.accounts() {
		err := c.list(ctx, "act_"+acct+"/"+d.edge, fieldsQuery(d.requestFields()), func(obj map[string]any) error {
			return em.Record(desc, record(d.fields, obj))
		})
		if err != nil {
			return fmt.Errorf("%s: %w", d.name, err)
		}
	}
	return nil
}

// creditRequestFields are the ExtendedCredit fields business_extended_credits
// asks for; amounts and owner_business are objects that become flat fields.
var creditRequestFields = append([]string{
	"id", "credit_type", "legal_entity_name", "liable_biz_name", "owner_business", "owner_business_name",
	"send_bill_to_biz_name", "is_automated_experience", "is_access_revoked",
}, creditAmountPrefixes...)

func (s Source) readBusinessCredits(ctx context.Context, c *apiClient, cfg Config, d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	readAt := now().UTC().Format(time.RFC3339)
	for _, biz := range cfg.BusinessIDs {
		err := c.list(ctx, biz+"/extendedcredits", fieldsQuery(creditRequestFields), func(obj map[string]any) error {
			flat := flattenCredit(obj)
			flat["date_created"] = readAt
			return em.Record(desc, record(d.fields, flat))
		})
		if err != nil {
			return fmt.Errorf("%s: business %s: %w", d.name, biz, err)
		}
	}
	return nil
}

// flattenCredit turns the CurrencyAmount objects and owner_business of an
// ExtendedCredit into flat fields; amounts may use a comma decimal separator.
func flattenCredit(obj map[string]any) map[string]any {
	flat := map[string]any{}
	for k, v := range obj {
		flat[k] = v
	}
	if owner, ok := obj["owner_business"].(map[string]any); ok {
		flat["owner_business_id"] = owner["id"]
		if _, set := flat["owner_business_name"]; !set {
			flat["owner_business_name"] = owner["name"]
		}
	}
	for _, p := range creditAmountPrefixes {
		amount, ok := obj[p].(map[string]any)
		if !ok {
			continue
		}
		if a, ok := numberText(amount["amount"]); ok {
			if n, ok := parseAmount(a); ok {
				flat[p+"_amount"] = n
			}
		}
		flat[p+"_currency"] = amount["currency"]
		flat[p+"_amount_in_hundredths"] = amount["amount_in_hundredths"]
		flat[p+"_offsetted_amount"] = amount["offsetted_amount"]
	}
	return flat
}
