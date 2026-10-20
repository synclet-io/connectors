package main

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	googleTokenURL       = "https://oauth2.googleapis.com/token"
	legacyGoogleTokenURL = "https://accounts.google.com/o/oauth2/token"
	googleUniverseDomain = "googleapis.com"
)

// checkServiceAccountKey checks that raw is a Google service account JSON
// key and returns it with token_uri set to tokenURL. The Google auth
// libraries post the signed assertion to token_uri and derive request hosts
// from universe_domain, so a key whose token_uri is anything but Google's
// token endpoint, or whose universe_domain is not googleapis.com, is
// rejected before any request.
func checkServiceAccountKey(raw []byte, tokenURL string) ([]byte, error) {
	var key map[string]json.RawMessage
	if err := json.Unmarshal(raw, &key); err != nil {
		return nil, errors.New("the key is not a JSON object")
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
		return nil, err
	}
	if typ != "service_account" {
		return nil, fmt.Errorf("type is %q, a service account key has type service_account", typ)
	}
	uri, err := field("token_uri")
	if err != nil {
		return nil, err
	}
	switch uri {
	case "", googleTokenURL, legacyGoogleTokenURL:
	default:
		return nil, fmt.Errorf("token_uri must be %s", googleTokenURL)
	}
	universe, err := field("universe_domain")
	if err != nil {
		return nil, err
	}
	if universe != "" && universe != googleUniverseDomain {
		return nil, fmt.Errorf("universe_domain must be %s", googleUniverseDomain)
	}
	key["token_uri"], err = json.Marshal(tokenURL)
	if err != nil {
		return nil, err
	}
	return json.Marshal(key)
}
