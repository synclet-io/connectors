package main

import (
	"errors"
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
