// Command kvctl-demo seeds a local demo endpoint. The access key comes from stdin.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/byPixelTV/kvstore/internal/apikeys"
	"github.com/byPixelTV/kvstore/internal/console"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: kvctl-demo HOST:PORT (read the demo key from stdin)")
	}
	input := bufio.NewScanner(os.Stdin)
	if !input.Scan() {
		log.Fatal("missing demo access key on stdin")
	}
	accessKey := strings.TrimSpace(input.Text())
	if _, ok := apikeys.Digest(accessKey); !ok {
		log.Fatal("invalid demo access key on stdin; use printable ASCII")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, identity, err := console.Open(ctx, console.Endpoint{Address: os.Args[1]}, accessKey)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	if !identity.Read || !identity.Write || !identity.Manage {
		log.Fatal("demo key requires read, write and manage permissions")
	}
	records := []console.Record{
		{Namespace: "players", Key: "alex", Value: []byte(`{"name":"Alex","level":27,"online":true}`)},
		{Namespace: "players", Key: "steve", Value: []byte(`{"name":"Steve","level":16,"online":false}`)},
		{Namespace: "server", Key: "motd", Value: []byte("Welcome to the KVStore demo!")},
		{Namespace: "server", Key: "cooldown", Value: []byte("Expires in 30 minutes"), ExpiresAt: time.Now().Add(30 * time.Minute)},
	}
	for _, record := range records {
		if _, err := client.Set(ctx, record); err != nil {
			log.Fatal(err)
		}
	}
	names, err := client.Namespaces(ctx)
	if err != nil || !slices.Contains(names, "players") || !slices.Contains(names, "server") {
		log.Fatalf("demo namespace check failed: %v (%v)", names, err)
	}
	fmt.Printf("Seeded %d records in players and server.\n", len(records))
}
