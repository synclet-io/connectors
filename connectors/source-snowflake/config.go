package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/youmark/pkcs8"
)

// Config is the connector configuration.
type Config struct {
	Account     string            `json:"account"`
	Warehouse   string            `json:"warehouse"`
	Database    string            `json:"database"`
	Schemas     []string          `json:"schemas"`
	Role        string            `json:"role"`
	Credentials CredentialsConfig `json:"credentials"`
}

// CredentialsConfig is how the connector logs in to Snowflake.
type CredentialsConfig struct {
	AuthType           string `json:"auth_type"`
	Username           string `json:"username"`
	Password           string `json:"password"`
	PrivateKey         string `json:"private_key"`
	PrivateKeyPassword string `json:"private_key_password"`
}

const (
	authKeyPair  = "Key Pair Authentication"
	authPassword = "username/password"
)

func (c *Config) validate() error {
	var errs []error
	for _, f := range []struct{ name, value string }{
		{"account", c.Account}, {"warehouse", c.Warehouse}, {"database", c.Database},
	} {
		if f.value == "" {
			errs = append(errs, fmt.Errorf("%s is required", f.name))
		}
	}
	for _, s := range c.Schemas {
		if s == "" {
			errs = append(errs, errors.New("schemas may not contain an empty name"))
			break
		}
	}
	cr := c.Credentials
	switch cr.AuthType {
	case authKeyPair:
		if cr.PrivateKey == "" {
			errs = append(errs, errors.New("credentials.private_key is required for auth_type "+authKeyPair))
		}
	case authPassword:
		if cr.Password == "" {
			errs = append(errs, errors.New("credentials.password is required for auth_type "+authPassword))
		}
	default:
		errs = append(errs, fmt.Errorf("credentials.auth_type %q is not one of %q, %q", cr.AuthType, authKeyPair, authPassword))
	}
	if cr.Username == "" {
		errs = append(errs, errors.New("credentials.username is required"))
	}
	return errors.Join(errs...)
}

// parsePrivateKey decodes an RSA private key in PEM: PKCS#8, encrypted
// PKCS#8 with password, or PKCS#1.
func parsePrivateKey(pemText, password string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(strings.TrimSpace(pemText)))
	if block == nil {
		return nil, errors.New("private_key is not a PEM block (-----BEGIN PRIVATE KEY-----)")
	}
	var key any
	var err error
	switch block.Type {
	case "ENCRYPTED PRIVATE KEY":
		if password == "" {
			return nil, errors.New("private_key is encrypted: set private_key_password")
		}
		key, err = pkcs8.ParsePKCS8PrivateKey(block.Bytes, []byte(password))
		if err != nil {
			return nil, fmt.Errorf("decrypt private_key with private_key_password: %w", err)
		}
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	default:
		key, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	}
	if err != nil {
		return nil, fmt.Errorf("parse private_key: %w", err)
	}
	rk, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private_key is a %T; Snowflake key pairs are RSA", key)
	}
	return rk, nil
}
