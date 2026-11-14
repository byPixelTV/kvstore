package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/byPixelTV/kvstore/internal/sharding"
)

func main() {
	action := flag.String("action", "layout", "layout, migrate, or recover")
	admin := flag.String("admin", "http://127.0.0.1:17110", "router admin HTTP endpoint (bind to loopback/VPN)")
	ns := flag.String("namespace", "", "namespace containing the slot")
	key := flag.String("key", "", "key for automatic slot calculation")
	slot := flag.Int("slot", -1, "slot 0..4095, alternative to -key")
	target := flag.Int("target", -1, "destination Raft group index 0..2")
	flag.Parse()
	token := os.Getenv("KVSTORE_ROUTER_ADMIN_TOKEN")
	if len(token) < 24 {
		fatalf("KVSTORE_ROUTER_ADMIN_TOKEN must be set (24+ characters)")
	}
	endpoint := ""
	method := http.MethodGet
	var payload any
	switch *action {
	case "layout":
		endpoint = "/v1/shards/layout"
	case "migrate", "recover":
		if *ns == "" {
			fatalf("-namespace is required")
		}
		if *slot < 0 && *key != "" {
			*slot = int(sharding.Slot(*ns, *key))
		}
		if *slot < 0 || *slot >= sharding.SlotCount {
			fatalf("-key or -slot 0..4095 is required")
		}
		method = http.MethodPost
		if *action == "migrate" {
			if *target < 0 && *key != "" {
				*target = (sharding.Shard(*ns, *key) + 1) % sharding.ShardCount
			}
			if *target < 0 || *target >= sharding.ShardCount {
				fatalf("-target must be in 0..2, or supply -key to choose a default destination")
			}
			endpoint = "/v1/shards/migrate"
			payload = map[string]any{"namespace": *ns, "slot": *slot, "target": *target}
		} else {
			endpoint = "/v1/shards/recover"
			payload = map[string]any{"namespace": *ns, "slot": *slot}
		}
	default:
		fatalf("unknown action %q", *action)
	}
	if !strings.HasPrefix(*admin, "http://127.0.0.1:") && !strings.HasPrefix(*admin, "http://localhost:") {
		fatalf("admin URL must use local loopback; tunnel or SSH port-forward for remote access")
	}
	var body []byte
	if payload != nil {
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			fatalf("encode payload: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, *admin+endpoint, bytes.NewReader(body))
	if err != nil {
		fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: 30 * time.Minute}).Do(req)
	if err != nil {
		fatalf("admin request failed: %v", err)
	}
	defer resp.Body.Close()
	result, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		fatalf("read response: %v", err)
	}
	if resp.StatusCode >= 300 {
		fatalf("admin returned %s: %s", resp.Status, string(result))
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, result, "", "  ") == nil {
		fmt.Println(pretty.String())
	} else {
		fmt.Println(string(result))
	}
}
func fatalf(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...); os.Exit(1) }
