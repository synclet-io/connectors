package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	neg, zero, big := -1, 0, 1001
	sa := Credentials{OptionTitle: authServiceAccount, Username: "u", Secret: "s", ProjectID: 1}
	cases := []struct {
		cfg  Config
		want []string
	}{
		{cfg: Config{Credentials: sa}},
		{cfg: Config{Credentials: Credentials{OptionTitle: authProjectSecret, APISecret: "x"}, Region: regionEU, StartDate: "2026-01-01T00:00:00Z"}},
		{cfg: Config{}, want: []string{"credentials.option_title is required"}},
		{cfg: Config{Credentials: Credentials{OptionTitle: "OAuth"}}, want: []string{`"OAuth" is neither`}},
		{cfg: Config{Credentials: Credentials{OptionTitle: authServiceAccount}}, want: []string{"username is required", "secret is required", "project_id is required"}},
		{cfg: Config{Credentials: Credentials{OptionTitle: authProjectSecret}}, want: []string{"api_secret is required"}},
		{cfg: Config{Credentials: sa, StartDate: "10/01/2026"}, want: []string{`start_date "10/01/2026"`}},
		{cfg: Config{Credentials: sa, StartDate: "2026-10-02", EndDate: "2026-10-01"}, want: []string{"end_date 2026-10-01 is before start_date 2026-10-02"}},
		{
			cfg:  Config{Credentials: sa, AttributionWindow: &neg, DateWindowSize: &zero, PageSize: &big},
			want: []string{"attribution_window -1 is negative", "date_window_size 0 is less than 1", "page_size 1001 is not between 1 and 1000"},
		},
	}
	for _, tc := range cases {
		err := tc.cfg.validate()
		if len(tc.want) == 0 {
			assert.NoError(t, err, "%+v", tc.cfg)
			continue
		}
		require.Error(t, err, "%+v", tc.cfg)
		for _, w := range tc.want {
			assert.Contains(t, err.Error(), w)
		}
	}
}

func TestDays(t *testing.T) {
	now := time.Date(2026, 10, 18, 5, 0, 0, 0, time.UTC)
	d, err := Config{}.days(now)
	require.NoError(t, err)
	assert.Equal(t, "2026-10-17", d.today.Format(dateLayout), "05:00Z is the previous day in US/Pacific")
	assert.Equal(t, "2025-10-17", d.start.Format(dateLayout), "a year back without start_date")
	assert.Equal(t, d.today, d.end)

	d, err = Config{ProjectTimezone: "UTC", StartDate: "2026-10-01", EndDate: "2027-01-01"}.days(now)
	require.NoError(t, err)
	assert.Equal(t, "2026-10-01", d.start.Format(dateLayout))
	assert.Equal(t, "2026-10-18", d.end.Format(dateLayout), "end_date after today reads to today")
}

func TestWindows(t *testing.T) {
	day := func(s string) time.Time { d, _ := time.Parse(dateLayout, s); return d }
	var got []string
	for _, w := range windows(day("2026-10-30"), day("2026-11-03"), 2) {
		got = append(got, w.fromDate()+".."+w.toDate())
	}
	assert.Equal(t, []string{"2026-10-30..2026-10-31", "2026-11-01..2026-11-02", "2026-11-03..2026-11-03"}, got)
	assert.Empty(t, windows(day("2026-11-04"), day("2026-11-03"), 2))
}
