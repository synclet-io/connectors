package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	financialEntriesPath   = "finance/financial_entries"
	defaultPollDelay       = 2 * time.Second
	maxPollDelay           = 60 * time.Second
	defaultPollTimeout     = 15 * time.Minute
	financialEntriesFilter = "created_at"
)

var financialEntryFields = []field{
	f("id", kStringNull), f(accountField, kString), f("order_id", kStringNull),
	f("external_psp_order_id", kStringNull), f("transaction_id", kStringNull), f("chargeback_id", kStringNull),
	f("order_description", kStringNull), f("created_at", kLocalTimeNull),
	f("transaction_datetime_provider", kLocalTimeNull), f("transaction_datetime_utc", kUTCTimeNull),
	f("accounting_date", kDateNull), f("amount", kStringNull), f("amount_in_major_units", kStringNull),
	f("currency", kStringNull), f("currency_minor_units", kStringNull), f("payout_amount", kStringNull),
	f("payout_amount_in_major_units", kStringNull), f("payout_currency", kStringNull),
	f("payout_currency_minor_units", kStringNull), f("record_type_key", kStringNull), f("provider", kStringNull),
	f("payment_method", kStringNull), f("card_brand", kStringNull), f("geo_country", kStringNull),
	f("issuing_country", kStringNull), f("legal_entity", kStringNull), f("product_id", kStringNull),
	f("product_name", kStringNull),
}

// errReport marks a report that cannot be downloaded.
var errReport = errors.New("financial entries report")

// financialWindow creates the financial entries report of one day, waits
// for its file, parses the whole CSV and only then emits its records, so a
// day whose file fails to parse emits nothing and saves no state.
func (s Source) financialWindow(d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) windowReader {
	return func(ctx context.Context, c *apiClient, w time.Time) (string, error) {
		req := reportRequest{
			DateFrom: w.Format(apiTimeLayout),
			DateTo:   w.AddDate(0, 0, 1).Format(apiTimeLayout),
			Filter:   financialEntriesFilter,
		}
		var created struct {
			ReportURL string `json:"report_url"`
		}
		if err := c.postJSON(ctx, c.reportsURL, financialEntriesPath, req, &created); err != nil {
			return "", err
		}
		reportURL, err := checkReportURL(c.reportsURL, created.ReportURL)
		if err != nil {
			return "", err
		}
		fileURL, err := s.waitForReport(ctx, c, reportURL)
		if err != nil {
			return "", err
		}
		data, err := c.download(ctx, fileURL)
		if err != nil {
			return "", err
		}
		recs, err := parseFinancialEntries(d.fields, data, c.account.Name)
		if err != nil {
			return "", err
		}
		var top string
		for _, rec := range recs {
			if cur, _ := rec[d.cursor].(string); cur > top {
				top = cur
			}
			if err := em.Record(desc, rec); err != nil {
				return "", err
			}
		}
		return top, nil
	}
}

// checkReportURL accepts only a report_url on the reports API host: polling
// it is signed, so the keys must not reach another host.
func checkReportURL(base, raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("%w was created without report_url", errReport)
	}
	b, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u, err := b.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: report_url is not a URL", errReport)
	}
	if u.Scheme != b.Scheme || u.Host != b.Host {
		return "", fmt.Errorf("%w: report_url is not on the reports API host", errReport)
	}
	return u.String(), nil
}

// waitForReport polls reportURL until it redirects to the report file and
// returns the file URL: 204 means not ready yet, 302 carries the file, 404
// and 410 mean the report is gone. The redirect is not followed, so the
// signed headers stay with Solidgate.
func (s Source) waitForReport(ctx context.Context, c *apiClient, reportURL string) (string, error) {
	delay, timeout := s.pollDelay, s.pollTimeout
	if delay == 0 {
		delay = defaultPollDelay
	}
	if timeout == 0 {
		timeout = defaultPollTimeout
	}
	noRedirect := *c.http
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	deadline := time.Now().Add(timeout)
	for {
		req, err := c.newRequest(ctx, http.MethodGet, reportURL, nil)
		if err != nil {
			return "", err
		}
		resp, data, err := c.roundTrip(&noRedirect, req)
		var r retryable
		switch {
		case errors.As(err, &r):
		case err != nil:
			return "", err
		case resp.StatusCode == http.StatusFound:
			loc, err := resp.Location()
			if err != nil {
				return "", fmt.Errorf("%w: redirect without a file location", errReport)
			}
			return loc.String(), nil
		case resp.StatusCode == http.StatusNoContent:
		case resp.StatusCode == http.StatusNotFound:
			return "", fmt.Errorf("%w not found (HTTP 404)", errReport)
		case resp.StatusCode == http.StatusGone:
			return "", fmt.Errorf("%w expired (HTTP 410)", errReport)
		case resp.StatusCode == http.StatusOK:
			err := errorInBody(req, data)
			if err == nil {
				return "", fmt.Errorf("%w: unexpected HTTP 200 without a file", errReport)
			}
			if !errors.As(err, &r) {
				return "", err
			}
		default:
			if err := statusError(req, resp.StatusCode, data); !errors.As(err, &r) {
				return "", err
			}
		}
		if time.Now().Add(delay).After(deadline) {
			return "", fmt.Errorf("%w was not ready after %s", errReport, timeout)
		}
		if err := sleep(ctx, delay); err != nil {
			return "", err
		}
		delay = min(delay*2, maxPollDelay)
	}
}

// download fetches the report file without Solidgate headers; its URL is
// pre-signed, so it never appears in an error.
func (c *apiClient) download(ctx context.Context, fileURL string) ([]byte, error) {
	delay := c.retryDelay
	for attempt := 1; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: file location is not a URL", errReport)
		}
		resp, err := c.http.Do(req)
		var data []byte
		status := 0
		if err == nil {
			status = resp.StatusCode
			data, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		switch {
		case err == nil && status == http.StatusOK:
			return data, nil
		case err == nil && status < 500:
			return nil, fmt.Errorf("%w: file download failed (HTTP %d)", errReport, status)
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case attempt == maxAttempts:
			if err != nil {
				return nil, fmt.Errorf("%w: file download failed", errReport)
			}
			return nil, fmt.Errorf("%w: file download failed (HTTP %d)", errReport, status)
		}
		if err := sleep(ctx, delay); err != nil {
			return nil, err
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

// parseFinancialEntries parses the whole report file: every row must have as
// many values as the header and every date column must match its format.
// Columns the schema does not list are dropped; empty values are null.
func parseFinancialEntries(fields []field, data []byte, account string) ([]map[string]any, error) {
	r := csv.NewReader(bytes.NewReader(bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))))
	header, err := r.Read()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: header: %w", errReport, err)
	}
	var recs []map[string]any
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			return recs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errReport, err)
		}
		obj := make(map[string]any, len(row))
		for i, col := range header {
			if row[i] != "" {
				obj[col] = row[i]
			}
		}
		rec, err := record(fields, obj, account)
		if err != nil {
			return nil, fmt.Errorf("%w: entry %v: %w", errReport, obj["id"], err)
		}
		recs = append(recs, rec)
	}
}
