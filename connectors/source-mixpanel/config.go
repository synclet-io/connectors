package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	dateLayout = "2006-01-02"

	authServiceAccount = "Service Account"
	authProjectSecret  = "Project Secret"

	regionUS = "US"
	regionEU = "EU"

	defaultTimezone          = "US/Pacific"
	defaultAttributionWindow = 5
	defaultDateWindowSize    = 30
	defaultPageSize          = 1000
	maxPageSize              = 1000
)

// Credentials is a service account with the project it reads, or the
// legacy secret of one project.
type Credentials struct {
	OptionTitle string `json:"option_title"`
	Username    string `json:"username,omitempty"`
	Secret      string `json:"secret,omitempty"`
	ProjectID   int64  `json:"project_id,omitempty"`
	APISecret   string `json:"api_secret,omitempty"`
}

// Config is the connector configuration.
type Config struct {
	Credentials       Credentials `json:"credentials"`
	Region            string      `json:"region,omitempty"`
	ProjectTimezone   string      `json:"project_timezone,omitempty"`
	StartDate         string      `json:"start_date,omitempty"`
	EndDate           string      `json:"end_date,omitempty"`
	AttributionWindow *int        `json:"attribution_window,omitempty"`
	DateWindowSize    *int        `json:"date_window_size,omitempty"`
	PageSize          *int        `json:"page_size,omitempty"`
}

// kind returns option_title, or for a configuration without it, as
// Airbyte allows, Project Secret when api_secret is set and Service Account
// otherwise.
func (c Credentials) kind() string {
	switch {
	case c.OptionTitle != "":
		return c.OptionTitle
	case c.APISecret != "":
		return authProjectSecret
	case c.Username != "" || c.Secret != "" || c.ProjectID != 0:
		return authServiceAccount
	}
	return ""
}

func (c Credentials) validate() error {
	switch c.kind() {
	case authServiceAccount:
		var errs []error
		if strings.TrimSpace(c.Username) == "" {
			errs = append(errs, errors.New("credentials.username is required"))
		}
		if strings.TrimSpace(c.Secret) == "" {
			errs = append(errs, errors.New("credentials.secret is required"))
		}
		if c.ProjectID <= 0 {
			errs = append(errs, errors.New("credentials.project_id is required for a service account"))
		}
		return errors.Join(errs...)
	case authProjectSecret:
		if strings.TrimSpace(c.APISecret) == "" {
			return errors.New("credentials.api_secret is required")
		}
		return nil
	case "":
		return errors.New("credentials need username, secret and project_id, or api_secret")
	}
	return fmt.Errorf("credentials.option_title %q is neither %q nor %q", c.OptionTitle, authServiceAccount, authProjectSecret)
}

func (c Config) validate() error {
	var errs []error
	if err := c.Credentials.validate(); err != nil {
		errs = append(errs, err)
	}
	switch c.Region {
	case "", regionUS, regionEU:
	default:
		errs = append(errs, fmt.Errorf("region %q is neither %s nor %s", c.Region, regionUS, regionEU))
	}
	if _, err := c.location(); err != nil {
		errs = append(errs, err)
	}
	start, err := optionalDate("start_date", c.StartDate)
	if err != nil {
		errs = append(errs, err)
	}
	end, err := optionalDate("end_date", c.EndDate)
	switch {
	case err != nil:
		errs = append(errs, err)
	case !start.IsZero() && !end.IsZero() && end.Before(start):
		errs = append(errs, fmt.Errorf("end_date %s is before start_date %s", c.EndDate, c.StartDate))
	}
	if c.AttributionWindow != nil && *c.AttributionWindow < 0 {
		errs = append(errs, fmt.Errorf("attribution_window %d is negative", *c.AttributionWindow))
	}
	if c.DateWindowSize != nil && *c.DateWindowSize < 1 {
		errs = append(errs, fmt.Errorf("date_window_size %d is less than 1", *c.DateWindowSize))
	}
	if c.PageSize != nil && (*c.PageSize < 1 || *c.PageSize > maxPageSize) {
		errs = append(errs, fmt.Errorf("page_size %d is not between 1 and %d", *c.PageSize, maxPageSize))
	}
	return errors.Join(errs...)
}

func (c Config) region() string {
	if c.Region == "" {
		return regionUS
	}
	return c.Region
}

// location returns project_timezone, the zone Mixpanel uses for the days of
// from_date and to_date.
func (c Config) location() (*time.Location, error) {
	name := c.ProjectTimezone
	if name == "" {
		name = defaultTimezone
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("project_timezone %q is not an IANA time zone", name)
	}
	return loc, nil
}

func (c Config) attributionWindow() int {
	if c.AttributionWindow == nil {
		return defaultAttributionWindow
	}
	return *c.AttributionWindow
}

func (c Config) dateWindowSize() int {
	if c.DateWindowSize == nil {
		return defaultDateWindowSize
	}
	return *c.DateWindowSize
}

func (c Config) pageSize() int {
	if c.PageSize == nil {
		return defaultPageSize
	}
	return *c.PageSize
}

// days is the range of project days a sync reads: start_date, else a year
// before today, up to end_date, else today, both in project_timezone.
type days struct {
	today, start, end time.Time
}

func (c Config) days(now time.Time) (days, error) {
	loc, err := c.location()
	if err != nil {
		return days{}, err
	}
	local := now.In(loc)
	d := days{today: civil(local.Year(), local.Month(), local.Day())}
	d.start = d.today.AddDate(-1, 0, 0)
	d.end = d.today
	start, err := optionalDate("start_date", c.StartDate)
	if err != nil {
		return d, err
	}
	if !start.IsZero() {
		d.start = start
	}
	end, err := optionalDate("end_date", c.EndDate)
	if err != nil {
		return d, err
	}
	if !end.IsZero() && end.Before(d.end) {
		d.end = end
	}
	return d, nil
}

// civil returns a day as midnight UTC, the form every day of this connector
// takes so that AddDate never meets a daylight saving change.
func civil(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

// optionalDate parses a YYYY-MM-DD date, or the date of a YYYY-MM-DDTHH:MM:SSZ
// timestamp, as a day. An empty value is the zero time.
func optionalDate(key, v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	day := v
	if len(v) > len(dateLayout) {
		if _, err := time.Parse(time.RFC3339, v); err != nil {
			return time.Time{}, fmt.Errorf("%s %q is neither YYYY-MM-DD nor an RFC 3339 timestamp", key, v)
		}
		day = v[:len(dateLayout)]
	}
	d, err := time.Parse(dateLayout, day)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s %q is neither YYYY-MM-DD nor an RFC 3339 timestamp", key, v)
	}
	return d, nil
}

// window is an inclusive range of project days.
type window struct{ from, to time.Time }

func (w window) fromDate() string { return w.from.Format(dateLayout) }
func (w window) toDate() string   { return w.to.Format(dateLayout) }

// windows splits [from, to] into ranges of size days; none when from is
// after to.
func windows(from, to time.Time, size int) []window {
	var ws []window
	for d := from; !d.After(to); d = d.AddDate(0, 0, size) {
		ws = append(ws, window{from: d, to: minTime(d.AddDate(0, 0, size-1), to)})
	}
	return ws
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
