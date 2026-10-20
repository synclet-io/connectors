package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	googleTokenURL       = "https://oauth2.googleapis.com/token"
	legacyGoogleTokenURL = "https://accounts.google.com/o/oauth2/token"
	googleUniverseDomain = "googleapis.com"

	serviceAccountType = "service_account"
	authorizedUserType = "authorized_user"
)

// keyMembers lists the members a key of each accepted type keeps.
var keyMembers = map[string][]string{
	serviceAccountType: {"type", "project_id", "private_key_id", "private_key", "client_email", "client_id", "token_uri", "universe_domain", "quota_project_id"},
	authorizedUserType: {"type", "client_id", "client_secret", "refresh_token", "token_uri", "universe_domain", "quota_project_id"},
}

// checkGoogleKey checks that raw is a Google credential JSON of one of types
// and returns the key with token_uri set to tokenURL, and its type. The
// Google auth libraries send the key's grant to token_uri, derive request
// hosts from universe_domain and take other flows from members such as
// installed or source_credentials. So a key whose token_uri is anything but
// Google's token endpoint, or whose universe_domain is not googleapis.com, is
// rejected, and the members its type does not use are dropped.
func checkGoogleKey(raw []byte, tokenURL string, types ...string) ([]byte, string, error) {
	var key map[string]json.RawMessage
	if err := json.Unmarshal(raw, &key); err != nil {
		return nil, "", errors.New("the key is not a JSON object")
	}
	field := func(name string) (string, error) {
		var s string
		if v, ok := key[name]; ok && json.Unmarshal(v, &s) != nil {
			return "", fmt.Errorf("%s is not a string", name)
		}
		return s, nil
	}
	typ, err := field("type")
	if err != nil {
		return nil, "", err
	}
	if !slices.Contains(types, typ) {
		return nil, "", fmt.Errorf("type is %q, want %s", typ, strings.Join(types, " or "))
	}
	uri, err := field("token_uri")
	if err != nil {
		return nil, "", err
	}
	switch uri {
	case "", googleTokenURL, legacyGoogleTokenURL:
	default:
		return nil, "", fmt.Errorf("token_uri must be %s", googleTokenURL)
	}
	universe, err := field("universe_domain")
	if err != nil {
		return nil, "", err
	}
	if universe != "" && universe != googleUniverseDomain {
		return nil, "", fmt.Errorf("universe_domain must be %s", googleUniverseDomain)
	}
	kept := make(map[string]json.RawMessage, len(keyMembers[typ]))
	for _, name := range keyMembers[typ] {
		if v, ok := key[name]; ok {
			kept[name] = v
		}
	}
	if kept["token_uri"], err = json.Marshal(tokenURL); err != nil {
		return nil, "", err
	}
	out, err := json.Marshal(kept)
	if err != nil {
		return nil, "", err
	}
	return out, typ, nil
}
