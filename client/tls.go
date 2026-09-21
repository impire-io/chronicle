package client

import (
	"crypto/tls"

	"github.com/nats-io/nats.go"
)

// TLSServerName is the option that verifies the server's certificate
// against name rather than the URL's host — for a dialer that reaches a
// TLS server through an address its certificate cannot name, such as a
// managed guest dialing its sandbox gateway to reach the public endpoint.
// Verification stays on; only the expected name changes. An empty name
// keeps the URL's host, and a URL that is not TLS is untouched: the scheme
// decides whether TLS is spoken, this only says which name to expect when
// it is.
func TLSServerName(name string) nats.Option {
	return func(o *nats.Options) error {
		if name == "" {
			return nil
		}
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if o.TLSConfig != nil {
			cfg = o.TLSConfig.Clone()
		}
		cfg.ServerName = name
		o.TLSConfig = cfg
		return nil
	}
}
