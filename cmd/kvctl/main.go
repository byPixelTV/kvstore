package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/byPixelTV/kvstore/internal/console"
	"github.com/byPixelTV/kvstore/internal/tui"
)

var version = "dev"

func main() {
	var endpoint console.Endpoint
	flag.StringVar(&endpoint.Address, "addr", "127.0.0.1:50051", "KVStore gRPC address")
	flag.BoolVar(&endpoint.Transport.TLS, "tls", false, "enable TLS with the system trust store")
	flag.StringVar(&endpoint.Transport.CA, "tls-ca", "", "enable TLS with a custom PEM CA")
	flag.StringVar(&endpoint.Transport.Cert, "tls-cert", "", "optional mTLS client certificate PEM")
	flag.StringVar(&endpoint.Transport.Key, "tls-key", "", "optional mTLS client private key PEM")
	showVersion := flag.Bool("version", false, "print the console version")
	flag.Parse()
	if *showVersion {
		fmt.Println("kvctl", version)
		return
	}
	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	if err := tui.Run(endpoint, console.Open); err != nil {
		fmt.Fprintln(os.Stderr, "kvctl:", err)
		os.Exit(1)
	}
}
