package clienttls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Credentials verifies the server identity and optionally presents a client certificate.
func Credentials(ca, cert, key string) (credentials.TransportCredentials, error) {
	if ca == "" && cert == "" && key == "" {
		return insecure.NewCredentials(), nil
	}
	if ca == "" || (cert == "") != (key == "") {
		return nil, errors.New("TLS requires -tls-ca and both or neither of -tls-cert and -tls-key")
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("TLS CA file contains no valid certificates")
	}
	cfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return credentials.NewTLS(cfg), nil
}
