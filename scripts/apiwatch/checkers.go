package apiwatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	longDate  = "January 2, 2006"
	monthDate = "January 2006"
)

func parseTime(layout, s string) (*time.Time, error) {
	t, err := time.Parse(layout, s)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

// semverish compares with compareVersions; most checkers need nothing else.
type semverish struct{}

func (semverish) Compare(a, b string) int { return compareVersions(a, b) }

func maxVersion(versions []string) string {
	var best string
	for _, v := range versions {
		if best == "" || compareVersions(v, best) > 0 {
			best = v
		}
	}

	return best
}

// metaGraph reads the "Available <table> Versions" table of the Graph API
// changelog: Version, Date Introduced, Available Until.
type metaGraph struct{ semverish }

const metaGraphURL = "https://developers.facebook.com/docs/graph-api/changelog"

var (
	reMetaRow     = regexp.MustCompile(`\b(v\d+\.\d+) ([A-Z][a-z]+ \d{1,2}, \d{4}) (TBD|[A-Z][a-z]+ \d{1,2}, \d{4})`)
	reMetaHeading = regexp.MustCompile(`Available [A-Za-z ]+ Versions|Out-Of-Cycle Changes`)
)

func (metaGraph) ID() string { return "meta-graph" }

func (metaGraph) Check(ctx context.Context, client *http.Client, pinned string, params map[string]string) (Result, error) {
	page, err := getOK(ctx, client, metaGraphURL)
	if err != nil {
		return Result{}, err
	}

	table := params["table"]
	if table == "" {
		table = "Graph API"
	}

	text := pageText(page)
	heading := "Available " + table + " Versions"

	i := strings.Index(text, heading)
	if i < 0 {
		return Result{}, fmt.Errorf("%q not found on %s", heading, metaGraphURL)
	}

	section := text[i+len(heading):]
	if loc := reMetaHeading.FindStringIndex(section); loc != nil {
		section = section[:loc[0]]
	}

	r := Result{Source: metaGraphURL}

	var (
		versions []string
		found    bool
	)

	for _, m := range reMetaRow.FindAllStringSubmatch(section, -1) {
		versions = append(versions, m[1])

		if m[1] != pinned {
			continue
		}

		found = true

		if m[3] != "TBD" {
			if r.Sunset, err = parseTime(longDate, m[3]); err != nil {
				return Result{}, err
			}
		}
	}

	if !found {
		return Result{}, fmt.Errorf("%s table has no row for %s", table, pinned)
	}

	r.Latest = maxVersion(versions)

	return r, nil
}

// googleAds reads the sunset-dates table (released versions only) and probes
// the Discovery document of the next major version, which is published on
// release day, in case the table lags behind.
type googleAds struct{ semverish }

const (
	googleAdsURL       = "https://developers.google.com/google-ads/api/docs/sunset-dates"
	googleAdsDiscovery = "https://googleads.googleapis.com/$discovery/rest?version="
)

var reAdsRow = regexp.MustCompile(`\bv(\d+)(\.\d+)?\*?(?: \(Latest\))? ([A-Z][a-z]+ \d{1,2}, \d{4}) ([A-Z][a-z]+ \d{4})`)

func (googleAds) ID() string { return "google-ads" }

func (googleAds) Check(ctx context.Context, client *http.Client, pinned string, _ map[string]string) (Result, error) {
	page, err := getOK(ctx, client, googleAdsURL)
	if err != nil {
		return Result{}, err
	}

	text := pageText(page)

	i := strings.Index(text, "Released versions")
	if i < 0 {
		return Result{}, fmt.Errorf(`"Released versions" not found on %s`, googleAdsURL)
	}

	released := text[i:]
	if j := strings.Index(released, "Upcoming versions"); j >= 0 {
		released = released[:j]
	}

	r := Result{Source: googleAdsURL}
	latest := 0

	for _, m := range reAdsRow.FindAllStringSubmatch(released, -1) {
		if m[2] != "" {
			continue
		}

		n, _ := strconv.Atoi(m[1])
		latest = max(latest, n)

		if "v"+m[1] == pinned {
			if r.Sunset, err = parseTime(monthDate, m[4]); err != nil {
				return Result{}, err
			}
		}
	}

	if r.Sunset == nil {
		return Result{}, fmt.Errorf("sunset table has no released row for %s", pinned)
	}

	r.Latest = fmt.Sprintf("v%d", latest)

	next := fmt.Sprintf("v%d", latest+1)

	_, code, err := get(ctx, client, googleAdsDiscovery+next)
	if err != nil {
		return Result{}, err
	}

	if code == http.StatusOK {
		r.Latest = next
		r.Note = next + " Discovery document is published; the sunset table does not list it yet"
	}

	return r, nil
}

// linkedIn reads the "Latest Version" table of the Marketing versioning page
// and the deprecation notice above it. LinkedIn supports each monthly version
// for at least a year, so without a notice the sunset is computed.
type linkedIn struct{ semverish }

const linkedInURL = "https://learn.microsoft.com/en-us/linkedin/marketing/versioning"

var (
	reLinkedInLatest = regexp.MustCompile(`Version Header [A-Z][a-z]+ \d{4} (\d{6})\b`)
	reLinkedInSunset = regexp.MustCompile(`Version (\d{6}) \([^)]*\) will be sunset on ([A-Z][a-z]+ \d{1,2}, \d{4})`)
)

func (linkedIn) ID() string { return "linkedin" }

func (linkedIn) Check(ctx context.Context, client *http.Client, pinned string, _ map[string]string) (Result, error) {
	page, err := getOK(ctx, client, linkedInURL)
	if err != nil {
		return Result{}, err
	}

	text := pageText(page)

	m := reLinkedInLatest.FindStringSubmatch(text)
	if m == nil {
		return Result{}, fmt.Errorf("latest version table not found on %s", linkedInURL)
	}

	r := Result{Latest: m[1], Source: linkedInURL}

	for _, n := range reLinkedInSunset.FindAllStringSubmatch(text, -1) {
		if n[1] == pinned {
			r.Sunset, err = parseTime(longDate, n[2])

			return r, err
		}
	}

	released, err := time.Parse("200601", pinned)
	if err != nil {
		return Result{}, fmt.Errorf("pinned %q is not YYYYMM: %w", pinned, err)
	}

	sunset := released.AddDate(1, 0, 0)
	r.Sunset = &sunset
	r.Note = "sunset computed: supported at least 12 months"

	return r, nil
}

// shopify reads the release schedule table of the versioning page; the
// release candidate never counts as the latest version.
type shopify struct{ semverish }

const shopifyURL = "https://shopify.dev/docs/api/usage/versioning"

var reShopifyRow = regexp.MustCompile(`\b(\d{4}-\d{2}) [A-Z][a-z]+ \d{1,2}, \d{4} ([A-Z][a-z]+ \d{1,2}, \d{4}) \d{2}:\d{2} UTC (Latest stable|Stable|Unsupported|Release candidate)`)

func (shopify) ID() string { return "shopify" }

func (shopify) Check(ctx context.Context, client *http.Client, pinned string, _ map[string]string) (Result, error) {
	page, err := getOK(ctx, client, shopifyURL)
	if err != nil {
		return Result{}, err
	}

	r := Result{Source: shopifyURL}

	var stable []string

	for _, m := range reShopifyRow.FindAllStringSubmatch(pageText(page), -1) {
		switch m[3] {
		case "Latest stable":
			r.Latest = m[1]
		case "Stable":
			stable = append(stable, m[1])
		}

		if m[1] == pinned && r.Sunset == nil {
			if r.Sunset, err = parseTime(longDate, m[2]); err != nil {
				return Result{}, err
			}
		}
	}

	if r.Latest == "" {
		r.Latest = maxVersion(stable)
	}

	if r.Latest == "" {
		return Result{}, fmt.Errorf("release schedule table not found on %s", shopifyURL)
	}

	if r.Sunset == nil {
		released, err := time.Parse("2006-01", pinned)
		if err != nil {
			return Result{}, fmt.Errorf("pinned %q is not YYYY-MM: %w", pinned, err)
		}

		sunset := released.AddDate(1, 0, 15)
		r.Sunset = &sunset
		r.Note = "not in the release schedule; sunset computed: accessible 12 months and 15 days"
	}

	return r, nil
}

// tiktokProbe asks the Business API for the next minor versions without a
// token: an unknown version answers code 40006 ("API version is not
// available"), a known one complains about the missing access token instead.
// Major versions are not probed: v2.x paths answer from another gateway that
// rejects the missing token before it looks at the version.
type tiktokProbe struct{ semverish }

const (
	tiktokBase         = "https://business-api.tiktok.com/open_api/"
	tiktokNoVersion    = 40006
	tiktokProbeMaxNext = 10
)

func (tiktokProbe) ID() string { return "tiktok-probe" }

func (tiktokProbe) Check(ctx context.Context, client *http.Client, pinned string, _ map[string]string) (Result, error) {
	var major, minor int
	if _, err := fmt.Sscanf(pinned, "v%d.%d", &major, &minor); err != nil {
		return Result{}, fmt.Errorf("pinned %q is not vMAJOR.MINOR: %w", pinned, err)
	}

	r := Result{Latest: pinned, Source: tiktokBase + pinned + "/advertiser/info/"}

	ok, err := tiktokAvailable(ctx, client, pinned)
	if err != nil {
		return Result{}, err
	}

	if !ok {
		r.Gone = true
		r.Note = "the pinned version answers: API version is not available"

		return r, nil
	}

	for range tiktokProbeMaxNext {
		next := fmt.Sprintf("v%d.%d", major, minor+1)

		ok, err := tiktokAvailable(ctx, client, next)
		if err != nil {
			return Result{}, err
		}

		if !ok {
			break
		}

		r.Latest = next
		minor++
	}

	return r, nil
}

func tiktokAvailable(ctx context.Context, client *http.Client, version string) (bool, error) {
	target := tiktokBase + version + "/advertiser/info/"

	body, err := getOK(ctx, client, target)
	if err != nil {
		return false, err
	}

	var resp struct {
		Code *int `json:"code"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Code == nil {
		return false, fmt.Errorf("GET %s: no JSON code in %.80q", target, body)
	}

	return *resp.Code != tiktokNoVersion, nil
}

// stripe takes the newest YYYY-MM-DD.<release> version on the changelog;
// .preview versions are betas.
type stripe struct{ semverish }

const stripeURL = "https://docs.stripe.com/changelog"

var reStripeVersion = regexp.MustCompile(`\b(\d{4}-\d{2}-\d{2})\.([a-z]+)\b`)

func (stripe) ID() string { return "stripe" }

func (stripe) Check(ctx context.Context, client *http.Client, _ string, _ map[string]string) (Result, error) {
	page, err := getOK(ctx, client, stripeURL)
	if err != nil {
		return Result{}, err
	}

	var versions []string

	for _, m := range reStripeVersion.FindAllStringSubmatch(string(page), -1) {
		if m[2] != "preview" {
			versions = append(versions, m[0])
		}
	}

	if len(versions) == 0 {
		return Result{}, fmt.Errorf("no API version found on %s", stripeURL)
	}

	return Result{Latest: maxVersion(versions), Source: stripeURL}, nil
}

// klaviyo reads info.version of the stable OpenAPI document, decoding only
// as far as that field. Klaviyo retires a revision two years after release.
type klaviyo struct{ semverish }

const klaviyoURL = "https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable.json"

func (klaviyo) ID() string { return "klaviyo" }

func (klaviyo) Check(ctx context.Context, client *http.Client, pinned string, _ map[string]string) (_ Result, rerr error) {
	resp, err := open(ctx, client, klaviyoURL)
	if err != nil {
		return Result{}, err
	}
	defer func() { rerr = errors.Join(rerr, resp.Body.Close()) }()

	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("GET %s: HTTP %d", klaviyoURL, resp.StatusCode)
	}

	version, err := openAPIVersion(json.NewDecoder(resp.Body))
	if err != nil {
		return Result{}, fmt.Errorf("%s: %w", klaviyoURL, err)
	}

	released, err := time.Parse(time.DateOnly, pinned)
	if err != nil {
		return Result{}, fmt.Errorf("pinned %q is not a revision date: %w", pinned, err)
	}

	sunset := released.AddDate(2, 0, 0)

	return Result{Latest: version, Sunset: &sunset, Source: klaviyoURL, Note: "sunset computed: retired 2 years after release"}, nil
}

// openAPIVersion returns info.version of an OpenAPI document without reading
// past the info object.
func openAPIVersion(dec *json.Decoder) (string, error) {
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return "", errors.New("not a JSON object")
	}

	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return "", err
		}

		if key != "info" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return "", err
			}

			continue
		}

		var info struct {
			Version string `json:"version"`
		}
		if err := dec.Decode(&info); err != nil {
			return "", err
		}

		if info.Version == "" {
			return "", errors.New("info.version is empty")
		}

		return info.Version, nil
	}

	return "", errors.New("no info object")
}

// intercom lists the version directories of Intercom's OpenAPI repository.
type intercom struct{ semverish }

const intercomURL = "https://api.github.com/repos/intercom/Intercom-OpenAPI/contents/descriptions"

var reIntercomVersion = regexp.MustCompile(`^\d+\.\d+$`)

func (intercom) ID() string { return "intercom" }

func (intercom) Check(ctx context.Context, client *http.Client, _ string, _ map[string]string) (Result, error) {
	body, err := getOK(ctx, client, intercomURL)
	if err != nil {
		return Result{}, err
	}

	var entries []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(body, &entries); err != nil {
		return Result{}, fmt.Errorf("%s: %w", intercomURL, err)
	}

	var versions []string

	for _, e := range entries {
		if e.Type == "dir" && reIntercomVersion.MatchString(e.Name) {
			versions = append(versions, e.Name)
		}
	}

	if len(versions) == 0 {
		return Result{}, fmt.Errorf("no version directory in %s", intercomURL)
	}

	return Result{Latest: maxVersion(versions), Source: "https://github.com/intercom/Intercom-OpenAPI/tree/main/descriptions"}, nil
}

// notion takes the newest "New API version: YYYY-MM-DD" of the changelog,
// which is rendered by a script, so the raw page is searched.
type notion struct{ semverish }

const notionURL = "https://developers.notion.com/page/changelog"

var reNotionVersion = regexp.MustCompile(`(?s)New API version:.{0,80}?(\d{4}-\d{2}-\d{2})`)

func (notion) ID() string { return "notion" }

func (notion) Check(ctx context.Context, client *http.Client, _ string, _ map[string]string) (Result, error) {
	page, err := getOK(ctx, client, notionURL)
	if err != nil {
		return Result{}, err
	}

	var versions []string
	for _, m := range reNotionVersion.FindAllSubmatch(page, -1) {
		versions = append(versions, string(m[1]))
	}

	if len(versions) == 0 {
		return Result{}, fmt.Errorf(`no "New API version" on %s`, notionURL)
	}

	return Result{Latest: maxVersion(versions), Source: notionURL}, nil
}

// github reads the supported REST API versions. GitHub supports a version at
// least 24 months after the next one is released.
type github struct{ semverish }

const githubURL = "https://api.github.com/versions"

func (github) ID() string { return "github" }

func (github) Check(ctx context.Context, client *http.Client, pinned string, _ map[string]string) (Result, error) {
	body, err := getOK(ctx, client, githubURL)
	if err != nil {
		return Result{}, err
	}

	var versions []string
	if err := json.Unmarshal(body, &versions); err != nil || len(versions) == 0 {
		return Result{}, fmt.Errorf("%s: want a JSON list of versions, got %.80q", githubURL, body)
	}

	r := Result{Latest: maxVersion(versions), Source: githubURL}

	listed := false

	var next string

	for _, v := range versions {
		listed = listed || v == pinned

		if compareVersions(v, pinned) > 0 && (next == "" || compareVersions(v, next) < 0) {
			next = v
		}
	}

	switch {
	case !listed:
		r.Gone = true
		r.Note = "the pinned version is no longer supported"
	case next != "":
		released, err := time.Parse(time.DateOnly, next)
		if err != nil {
			return Result{}, fmt.Errorf("version %q is not a date: %w", next, err)
		}

		sunset := released.AddDate(2, 0, 0)
		r.Sunset = &sunset
		r.Note = "sunset computed: supported 24 months after " + next
	}

	return r, nil
}

// salesforce lists the REST API versions an instance serves without
// authentication. Salesforce ships three versions a year and retires old ones
// in announced batches, so params.max_behind tolerates that many newer ones.
type salesforce struct{ semverish }

const salesforceURL = "https://na1.salesforce.com/services/data/"

var reSalesforceVersion = regexp.MustCompile(`^\d+\.\d+$`)

func (salesforce) ID() string { return "salesforce" }

func (salesforce) Check(ctx context.Context, client *http.Client, pinned string, params map[string]string) (Result, error) {
	body, err := getOK(ctx, client, salesforceURL)
	if err != nil {
		return Result{}, err
	}

	var list []struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &list); err != nil || len(list) == 0 {
		return Result{}, fmt.Errorf("%s: want a JSON list of versions, got %.80q", salesforceURL, body)
	}

	var (
		versions []string
		listed   bool
		behind   int
	)

	seen := map[string]bool{}

	for _, v := range list {
		if !reSalesforceVersion.MatchString(v.Version) || seen[v.Version] {
			continue
		}

		seen[v.Version] = true
		version := "v" + v.Version

		versions = append(versions, version)
		listed = listed || version == pinned

		if compareVersions(version, pinned) > 0 {
			behind++
		}
	}

	r := Result{Latest: maxVersion(versions), Source: salesforceURL}

	if !listed {
		r.Gone = true
		r.Note = "the pinned version is retired"

		return r, nil
	}

	maxBehind, _ := strconv.Atoi(params["max_behind"])
	if behind > 0 {
		r.Note = fmt.Sprintf("%d versions behind", behind)
		r.Hold = behind < maxBehind
	}

	return r, nil
}

// googleDiscovery reads the preferred version of an API from the Discovery
// directory. A pin written as name/version ("webmasters/v3") names its API; a
// pinned API missing from the directory is reported against params.successor.
type googleDiscovery struct{}

const googleDiscoveryURL = "https://discovery.googleapis.com/discovery/v1/apis"

func (googleDiscovery) ID() string { return "google-discovery" }

// Compare treats a version of a different API, the successor, as newer.
func (googleDiscovery) Compare(a, b string) int {
	na, va := splitAPI(a)
	nb, vb := splitAPI(b)

	if na != nb {
		return -1
	}

	return compareVersions(va, vb)
}

func splitAPI(v string) (string, string) {
	if name, version, ok := strings.Cut(v, "/"); ok {
		return name, version
	}

	return "", v
}

func (googleDiscovery) Check(ctx context.Context, client *http.Client, pinned string, params map[string]string) (Result, error) {
	body, err := getOK(ctx, client, googleDiscoveryURL)
	if err != nil {
		return Result{}, err
	}

	var dir struct {
		Items []struct {
			Name      string `json:"name"`
			Version   string `json:"version"`
			Preferred bool   `json:"preferred"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &dir); err != nil || len(dir.Items) == 0 {
		return Result{}, fmt.Errorf("%s: no items", googleDiscoveryURL)
	}

	preferred := func(name string) string {
		for _, it := range dir.Items {
			if it.Name == name && it.Preferred {
				return it.Version
			}
		}

		return ""
	}

	name, _ := splitAPI(pinned)
	if name == "" {
		name = params["api"]
	}

	if name == "" {
		return Result{}, errors.New("params.api is required when the pin has no api/ prefix")
	}

	format := func(api, version string) string {
		if strings.Contains(pinned, "/") {
			return api + "/" + version
		}

		return version
	}

	r := Result{Source: googleDiscoveryURL + "?name=" + name}

	if v := preferred(name); v != "" {
		r.Latest = format(name, v)

		return r, nil
	}

	successor := params["successor"]
	if successor == "" {
		return Result{}, fmt.Errorf("%s is not in the Discovery directory", name)
	}

	v := preferred(successor)
	if v == "" {
		return Result{}, fmt.Errorf("neither %s nor its successor %s is in the Discovery directory", name, successor)
	}

	r.Latest = successor + "/" + v
	r.Source = googleDiscoveryURL + "?name=" + successor
	r.Note = name + " is no longer in the Discovery directory; " + successor + " replaces it"

	return r, nil
}
