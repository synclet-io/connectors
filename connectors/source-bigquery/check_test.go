package main

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const selectOneResponse = `{"kind":"bigquery#queryResponse","jobReference":{"projectId":"p","jobId":"j","location":"US"},"jobComplete":true,` +
	`"schema":{"fields":[{"name":"f0_","type":"INTEGER","mode":"NULLABLE"}]},"rows":[{"f":[{"v":"1"}]}],"totalRows":"1"}`

func checkStatus(t *testing.T, src Source, cfg Config) *synclet.ConnectionStatus {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: mustJSON(t, cfg)})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	st := out.ConnectionStatus()
	require.NotNil(t, st)
	return st
}

func TestCheck(t *testing.T) {
	queryOK := fakeRoute{http.MethodPost, "p/queries", http.StatusOK, selectOneResponse}
	cases := []struct {
		name   string
		src    func(t *testing.T) Source
		cfg    Config
		failed string
	}{
		{
			name:   "invalid service account JSON",
			src:    func(*testing.T) Source { return Source{} },
			cfg:    Config{ProjectID: "p", Credentials: CredentialsConfig{AuthType: authService, ServiceAccountInfo: "{not json"}},
			failed: "service_account_info is not a valid service account key",
		},
		{
			name: "missing project",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, fakeRoute{http.MethodPost, "nope/queries", http.StatusNotFound, googleError(404, "notFound", "Not found: Project nope")})
			},
			cfg:    Config{ProjectID: "nope"},
			failed: `project "nope" not found`,
		},
		{
			name: "no job user role",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, fakeRoute{http.MethodPost, "p/queries", http.StatusForbidden,
					googleError(403, "accessDenied", "Access Denied: Project p: User does not have bigquery.jobs.create permission in project p.")})
			},
			cfg:    Config{ProjectID: "p"},
			failed: `needs the BigQuery Job User role (bigquery.jobs.create) on project "p"`,
		},
		{
			name: "missing dataset",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, queryOK, fakeRoute{http.MethodGet, "p/datasets/d", http.StatusNotFound, googleError(404, "notFound", "Not found: Dataset p:d")})
			},
			cfg:    Config{ProjectID: "p", Datasets: []string{"d"}},
			failed: `dataset "p.d" not found`,
		},
		{
			name: "no data viewer role on the dataset",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, queryOK, fakeRoute{http.MethodGet, "p/datasets/d", http.StatusForbidden,
					googleError(403, "accessDenied", "Access Denied: Dataset p:d: Permission bigquery.datasets.get denied on dataset p:d")})
			},
			cfg:    Config{ProjectID: "p", Datasets: []string{"d"}},
			failed: `needs the BigQuery Data Viewer role on dataset "p.d"`,
		},
		{
			name: "no permission to list tables",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, queryOK,
					fakeRoute{http.MethodGet, "p/datasets/d", http.StatusOK, `{"datasetReference":{"projectId":"p","datasetId":"d"},"location":"US"}`},
					fakeRoute{http.MethodGet, "p/datasets/d/tables", http.StatusForbidden,
						googleError(403, "accessDenied", "Access Denied: Dataset p:d: Permission bigquery.tables.list denied on dataset p:d")})
			},
			cfg:    Config{ProjectID: "p", Datasets: []string{"d"}},
			failed: `needs the BigQuery Data Viewer role on dataset "p.d"`,
		},
	}
	cases = append(cases, []struct {
		name   string
		src    func(t *testing.T) Source
		cfg    Config
		failed string
	}{
		{
			name: "quota error on the query is not a missing role",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, fakeRoute{http.MethodPost, "p/queries", http.StatusForbidden,
					googleError(403, "quotaExceeded", "Quota exceeded: Your project exceeded quota for free query bytes scanned.")})
			},
			cfg:    Config{ProjectID: "p"},
			failed: `run a test query in project "p": googleapi: Error 403: Quota exceeded`,
		},
		{
			name: "no dataset visible",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, queryOK, fakeRoute{http.MethodGet, "p/datasets", http.StatusOK, `{"kind":"bigquery#datasetList"}`})
			},
			cfg:    Config{ProjectID: "p"},
			failed: `no dataset of project "p" is visible: the account needs the BigQuery Data Viewer role on the project or on a dataset`,
		},
		{
			name: "no permission to list datasets",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, queryOK, fakeRoute{http.MethodGet, "p/datasets", http.StatusForbidden,
					googleError(403, "accessDenied", "Access Denied: Project p: Permission bigquery.datasets.list denied")})
			},
			cfg:    Config{ProjectID: "p"},
			failed: `needs the BigQuery Data Viewer role (bigquery.datasets.list) on project "p"`,
		},
		{
			name: "metadata viewer only: no table data access",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, append(datasetWithTable(),
					fakeRoute{http.MethodGet, "p/datasets/d/tables/t/data", http.StatusForbidden,
						googleError(403, "accessDenied", "Access Denied: Table p:d.t: Permission bigquery.tables.getData denied on table p:d.t")})...)
			},
			cfg:    Config{ProjectID: "p", Datasets: []string{"d"}},
			failed: `needs the BigQuery Data Viewer role on dataset "p.d"`,
		},
		{
			name: "listed dataset without table data access",
			src: func(t *testing.T) Source {
				return fakeBigQuery(t, append(datasetWithTable(),
					fakeRoute{http.MethodGet, "p/datasets", http.StatusOK, `{"datasets":[{"datasetReference":{"projectId":"p","datasetId":"d"}}]}`},
					fakeRoute{http.MethodGet, "p/datasets/d/tables/t/data", http.StatusForbidden,
						googleError(403, "accessDenied", "Access Denied: Table p:d.t: Permission bigquery.tables.getData denied on table p:d.t")})...)
			},
			cfg:    Config{ProjectID: "p"},
			failed: `needs the BigQuery Data Viewer role on dataset "p.d"`,
		},
	}...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := checkStatus(t, tc.src(t), tc.cfg)
			require.Equal(t, synclet.ConnectionStatusFailed, st.Status)
			require.Contains(t, st.Message, tc.failed)
		})
	}

	t.Run("succeeded", func(t *testing.T) {
		src := fakeBigQuery(t, queryOK,
			fakeRoute{http.MethodGet, "p/datasets/d", http.StatusOK, `{"datasetReference":{"projectId":"p","datasetId":"d"},"location":"US"}`},
			fakeRoute{http.MethodGet, "p/datasets/d/tables", http.StatusOK, `{"tables":[],"totalItems":0}`})
		st := checkStatus(t, src, Config{ProjectID: "p", Datasets: []string{"d"}})
		require.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
	})

	t.Run("succeeded reading table data of every listed dataset", func(t *testing.T) {
		src := fakeBigQuery(t, append(datasetWithTable(),
			fakeRoute{http.MethodGet, "p/datasets", http.StatusOK, `{"datasets":[{"datasetReference":{"projectId":"p","datasetId":"d"}}]}`},
			fakeRoute{http.MethodGet, "p/datasets/d/tables/t/data", http.StatusOK, `{"totalRows":"1","rows":[{"f":[{"v":"1"}]}]}`})...)
		st := checkStatus(t, src, Config{ProjectID: "p"})
		require.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
	})
}

// datasetWithTable serves a successful test query and dataset p.d holding
// one table t.
func datasetWithTable() []fakeRoute {
	return []fakeRoute{
		{http.MethodPost, "p/queries", http.StatusOK, selectOneResponse},
		{http.MethodGet, "p/datasets/d", http.StatusOK, `{"datasetReference":{"projectId":"p","datasetId":"d"},"location":"US"}`},
		{http.MethodGet, "p/datasets/d/tables", http.StatusOK,
			`{"tables":[{"tableReference":{"projectId":"p","datasetId":"d","tableId":"t"},"type":"TABLE"}],"totalItems":1}`},
		{http.MethodGet, "p/datasets/d/tables/t", http.StatusOK,
			`{"tableReference":{"projectId":"p","datasetId":"d","tableId":"t"},"type":"TABLE","schema":{"fields":[{"name":"id","type":"INTEGER","mode":"NULLABLE"}]}}`},
	}
}

func TestQuote(t *testing.T) {
	bt := "`"
	require.Equal(t, bt+"a"+bt, quote("a"))
	require.Equal(t, bt+`a\\`+bt, quote(`a\`))
	require.Equal(t, bt+`a\`+bt+bt, quote("a"+bt))
	require.Equal(t, bt+`a\\\`+bt+bt, quote(`a\`+bt))
}
