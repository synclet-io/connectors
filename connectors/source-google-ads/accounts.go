package main

import (
	"context"
	"fmt"
	"slices"
)

// accountsQuery lists a customer's account hierarchy: the customer itself at
// level 0 and, for a manager, every account below it.
const accountsQuery = "SELECT customer_client.id, customer_client.manager, customer_client.status, customer_client.level FROM customer_client"

// account is a customer a stream reads and the login-customer-id header its
// requests send, empty for none.
type account struct {
	id    string
	login string
}

// resolveAccounts returns the accounts the streams read: each configured
// customer that is not a manager, and the enabled non-manager accounts below
// each configured manager, narrowed to child_customer_ids when it is set.
// Requests for an account found under a manager send login_customer_id, or
// that manager when it is not set.
func resolveAccounts(ctx context.Context, c *apiClient, cfg Config) ([]account, error) {
	var out []account
	seen := map[string]bool{}
	add := func(a account) {
		if !seen[a.id] {
			seen[a.id] = true
			out = append(out, a)
		}
	}
	fs := []field{
		{path: "customer_client.id", typ: int64Type},
		{path: "customer_client.manager", typ: boolType},
		{path: "customer_client.status", typ: enumType},
		{path: "customer_client.level", typ: int64Type},
	}
	children := cfg.children()
	for _, cid := range cfg.customers() {
		type client struct {
			id      string
			manager bool
			status  string
			level   string
		}
		var clients []client
		err := c.search(ctx, cid, cfg.login(), accountsQuery, func(row map[string]any) error {
			var cl client
			for _, f := range fs {
				v, ok := lookup(row, f.path)
				if !ok {
					continue
				}
				switch f.path {
				case "customer_client.id":
					cl.id = fmt.Sprint(v)
				case "customer_client.manager":
					cl.manager, _ = v.(bool)
				case "customer_client.status":
					cl.status = fmt.Sprint(v)
				case "customer_client.level":
					cl.level = fmt.Sprint(v)
				}
			}
			clients = append(clients, cl)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("accounts of customer %s: %w", cid, err)
		}
		manager := false
		for _, cl := range clients {
			if cl.id == cid {
				manager = cl.manager
			}
		}
		if !manager {
			add(account{id: cid, login: cfg.login()})
			continue
		}
		login := cfg.login()
		if login == "" {
			login = cid
		}
		for _, cl := range clients {
			if cl.id == cid || cl.manager || cl.status != "ENABLED" {
				continue
			}
			if len(children) > 0 && !slices.Contains(children, cl.id) {
				continue
			}
			add(account{id: cl.id, login: login})
		}
	}
	return out, nil
}
