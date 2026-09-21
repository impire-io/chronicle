package client

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestTLSServerNameSetsTheName(t *testing.T) {
	opts := nats.GetDefaultOptions()
	if err := TLSServerName("connect.example")(&opts); err != nil {
		t.Fatal(err)
	}
	if opts.TLSConfig == nil || opts.TLSConfig.ServerName != "connect.example" {
		t.Fatalf("TLSConfig = %+v, want ServerName connect.example", opts.TLSConfig)
	}
	if opts.TLSConfig.InsecureSkipVerify {
		t.Fatal("the option must never switch verification off")
	}
	if opts.Secure {
		t.Fatal("the option does not decide whether TLS is spoken; the scheme does")
	}
}

func TestTLSServerNameKeepsTheCallersConfig(t *testing.T) {
	pool := x509.NewCertPool()
	given := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
	opts := nats.GetDefaultOptions()
	if err := nats.Secure(given)(&opts); err != nil {
		t.Fatal(err)
	}
	if err := TLSServerName("connect.example")(&opts); err != nil {
		t.Fatal(err)
	}
	if opts.TLSConfig.RootCAs != pool || opts.TLSConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("the caller's config was not carried: %+v", opts.TLSConfig)
	}
	if opts.TLSConfig.ServerName != "connect.example" {
		t.Fatalf("ServerName = %q, want connect.example", opts.TLSConfig.ServerName)
	}
	if given.ServerName != "" {
		t.Fatal("the caller's config was mutated in place")
	}
}

func TestTLSServerNameEmptyMeansTheURLHost(t *testing.T) {
	opts := nats.GetDefaultOptions()
	if err := TLSServerName("")(&opts); err != nil {
		t.Fatal(err)
	}
	if opts.TLSConfig != nil {
		t.Fatalf("an empty name changed the options: %+v", opts.TLSConfig)
	}
}

// TestTLSServerNameVerifiesAgainstTheGivenName dials a TLS server by an
// address its certificate does not name — the guest's situation, its
// gateway standing in for 127.0.0.1 here — and shows the name is what
// makes the dial verify: without it the dial fails on the address, with a
// wrong name it fails on the name, with the certificate's name it
// connects. Every failure is the verifier's own, never a skipped check.
func TestTLSServerNameVerifiesAgainstTheGivenName(t *testing.T) {
	url, pool := startTLSServer(t, "connect.example")
	trust := nats.Secure(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})

	c, err := ConnectWith(url, "tester", trust, TLSServerName("connect.example"))
	if err != nil {
		t.Fatalf("dial %s with the certificate's name: %v", url, err)
	}
	c.Close()

	for name, opts := range map[string][]nats.Option{
		"the address":  {trust},
		"a wrong name": {trust, TLSServerName("other.example")},
	} {
		c, err := ConnectWith(url, "tester", opts...)
		if err == nil {
			c.Close()
			t.Fatalf("dialing %s verified against %s, which the certificate does not name", url, name)
		}
		var hostname x509.HostnameError
		if !errors.As(err, &hostname) {
			t.Fatalf("dialing %s against %s failed for another reason than the name: %v", url, name, err)
		}
	}
}

// startTLSServer runs an in-process NATS server on 127.0.0.1 whose
// certificate names only the given DNS name, returning the tls:// URL and
// the pool that trusts the certificate.
func startTLSServer(t *testing.T, name string) (url string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		DNSNames:              []string{name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	pool = x509.NewCertPool()
	pool.AddCert(cert)

	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1",
		Port: -1,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
			MinVersion:   tls.VersionTLS12,
		},
	})
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready in time")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL(), pool
}
