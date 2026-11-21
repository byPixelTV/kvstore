package kvstore

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/byPixelTV/kvstore/internal/clienttls"
)

// DialEnvironment reads KVSTORE_ADDR, KVSTORE_API_KEY, KVSTORE_PLAINTEXT and
// the optional KVSTORE_TLS_CA_FILE, KVSTORE_TLS_CERT_FILE and KVSTORE_TLS_KEY_FILE.
func DialEnvironment() (*Client, error) {
	address := strings.TrimSpace(os.Getenv("KVSTORE_ADDR"))
	if address == "" {
		return nil, errors.New("KVSTORE_ADDR environment variable is not set")
	}
	options := Options{APIKey: os.Getenv("KVSTORE_API_KEY")}
	if raw := os.Getenv("KVSTORE_PLAINTEXT"); raw != "" {
		var err error
		options.Plaintext, err = strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("parse KVSTORE_PLAINTEXT: %w", err)
		}
	}
	files := clienttls.Options{
		CA: os.Getenv("KVSTORE_TLS_CA_FILE"), Cert: os.Getenv("KVSTORE_TLS_CERT_FILE"), Key: os.Getenv("KVSTORE_TLS_KEY_FILE"),
	}
	if files.Enabled() {
		var err error
		options.TLS, err = files.Config()
		if err != nil {
			return nil, fmt.Errorf("configure KVStore TLS: %w", err)
		}
	}
	return Dial(address, options)
}
