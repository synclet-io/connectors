package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

// buildTLSConfig creates a *tls.Config based on the SSL mode.
// Returns nil for "preferred" mode (driver handles it natively via DSN tls=preferred).
func buildTLSConfig(ssl SSLConfig) (*tls.Config, error) {
	switch ssl.Mode {
	case "preferred", "":
		return nil, nil

	case "required":
		return &tls.Config{InsecureSkipVerify: true}, nil

	case "verify_ca":
		if ssl.CACert == "" {
			return nil, fmt.Errorf("ca_cert is required for ssl_mode=verify_ca")
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM([]byte(ssl.CACert)) {
			return nil, fmt.Errorf("failed to parse CA certificate PEM")
		}

		return &tls.Config{
			RootCAs: certPool,
		}, nil

	case "verify_identity":
		if ssl.CACert == "" {
			return nil, fmt.Errorf("ca_cert is required for ssl_mode=verify_identity")
		}
		if ssl.ClientCert == "" {
			return nil, fmt.Errorf("client_cert is required for ssl_mode=verify_identity")
		}
		if ssl.ClientKey == "" {
			return nil, fmt.Errorf("client_key is required for ssl_mode=verify_identity")
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM([]byte(ssl.CACert)) {
			return nil, fmt.Errorf("failed to parse CA certificate PEM")
		}

		clientCert, err := tls.X509KeyPair([]byte(ssl.ClientCert), []byte(ssl.ClientKey))
		if err != nil {
			return nil, fmt.Errorf("failed to parse client certificate/key: %w", err)
		}

		return &tls.Config{
			RootCAs:      certPool,
			Certificates: []tls.Certificate{clientCert},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported ssl_mode: %s", ssl.Mode)
	}
}
