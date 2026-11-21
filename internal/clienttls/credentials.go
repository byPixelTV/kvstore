package clienttls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Options struct {
	TLS           bool
	CA, Cert, Key string
}

func (o Options) Enabled() bool { return o.TLS || o.CA != "" || o.Cert != "" || o.Key != "" }

// Credentials verifies the server identity and optionally presents a client certificate.
func (o Options) Credentials() (credentials.TransportCredentials, error) {
	if !o.Enabled() {
		return insecure.NewCredentials(), nil
	}
	cfg, err := o.Config()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(cfg), nil
}

func (o Options) Config() (*tls.Config, error) {
	if (o.Cert == "") != (o.Key == "") {
		return nil, errors.New("TLS client authentication requires both -tls-cert and -tls-key")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if o.CA != "" {
		pem, err := os.ReadFile(o.CA)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("TLS CA file contains no valid certificates")
		}
	}
	if o.Cert != "" {
		pair, err := tls.LoadX509KeyPair(o.Cert, o.Key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}
