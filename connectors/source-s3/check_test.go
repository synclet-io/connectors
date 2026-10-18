package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func TestCheckErrorMapping(t *testing.T) {
	api := func(code string) error { return &smithy.GenericAPIError{Code: code, Message: "m"} }
	for _, code := range []string{"InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidClientTokenId"} {
		require.ErrorContains(t, describeListError(api(code), "b", "p/"), "invalid credentials ("+code+")")
		require.ErrorContains(t, describeReadError(api(code), "k"), "invalid credentials")
	}
	require.ErrorContains(t, describeListError(api("NoSuchBucket"), "b", "p/"), `bucket "b" does not exist`)
	require.ErrorContains(t, describeListError(api("AccessDenied"), "b", "p/"), `no list permission on prefix "p/"`)
	require.ErrorContains(t, describeListError(api("PermanentRedirect"), "b", "p/"), "in another region")
	require.ErrorContains(t, describeReadError(api("AccessDenied"), "p/k"), `no read permission on "p/k"`)
	require.ErrorContains(t, describeListError(errors.New("dial tcp: refused"), "b", "p/"), `list prefix "p/" of bucket "b": dial tcp`)
	require.ErrorContains(t, describeCredentialError(api("AccessDenied")), "AccessDenied")
}

// TestCheckRoleNotAssumable runs check against an STS endpoint that accepts
// the source credentials and denies AssumeRole, as AWS does for a role
// whose trust policy does not name the caller.
func TestCheckRoleNotAssumable(t *testing.T) {
	const role = "arn:aws:iam::123456789012:role/not-trusted"
	var assumeCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil || r.Form.Get("Action") != "AssumeRole" || r.Form.Get("RoleArn") != role {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		assumeCalls.Add(1)
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>AccessDenied</Code>` +
			`<Message>User: arn:aws:iam::123456789012:user/reader is not authorized to perform: sts:AssumeRole on resource: ` + role + `</Message>` +
			`</Error><RequestId>r-1</RequestId></ErrorResponse>`))
	}))
	t.Cleanup(srv.Close)

	cfg := Config{
		Bucket:         "b",
		Endpoint:       srv.URL,
		ForcePathStyle: true,
		Credentials:    Credentials{AuthType: authAssumeRole, RoleARN: role, AccessKeyID: "reader", SecretAccessKey: "reader-secret"},
		Streams:        []StreamConfig{{Name: "s", Glob: "data/*.csv", Format: Format{Filetype: formatCSV}}},
	}
	err := Source{}.Check(t.Context(), cfg)
	require.ErrorContains(t, err, `cannot assume role "`+role+`"`)
	require.ErrorContains(t, err, "AccessDenied")
	require.NotContains(t, err.Error(), "invalid credentials")
	require.Equal(t, int32(1), assumeCalls.Load())
}
