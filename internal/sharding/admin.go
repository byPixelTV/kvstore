package sharding

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/monitoring"
)

type migrateRequest struct {
	Namespace string `json:"namespace"`
	Slot      uint16 `json:"slot"`
	Target    int    `json:"target"`
}
type recoverRequest struct {
	Namespace string `json:"namespace"`
	Slot      uint16 `json:"slot"`
}

// RecoverSlot is an OPERATOR ACTION, not a background retry. It requires
// external assurance that the previous router is stopped and all its old
// migration RPCs have expired. It rereads Raft-committed metadata and then
// unseals the authoritative owner, sealing every non-owner for that slot.
func (r *Router) RecoverSlot(ctx context.Context, namespace string, slot uint16) error {
	if namespace == "" || slot >= SlotCount {
		return errors.New("invalid namespace/slot")
	}
	if err := invalidKey(namespace, "recovery"); err != nil {
		return err
	}
	r.migrationMu.Lock()
	defer r.migrationMu.Unlock()
	r.unresolved.Store(true)
	r.ready.Store(false)
	client, err := r.backend(0)
	if err != nil {
		return err
	}
	value, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: routeNamespace, Key: routeKey})
	if err != nil {
		return err
	}
	next := &routingState{Version: 1, Epoch: 1, Fingerprint: r.clusterFingerprint, Overrides: map[string]map[uint16]uint8{}}
	if value.GetFound() {
		if err := json.Unmarshal(value.GetValue(), next); err != nil {
			return err
		}
		if err := next.validate(r.clusterFingerprint); err != nil {
			return err
		}
	}
	owner := next.owner(namespace, slot)
	// Apply the unseal on the owner first, then deny future writes to replicas
	// that no longer own the slot. No regular request is allowed while the
	// router is in unresolved state. A healthy router can also be recovered
	// after an explicit administrative maintenance window.
	if err := r.setFence(ctx, owner, namespace, slot, false); err != nil {
		return err
	}
	for i := 0; i < ShardCount; i++ {
		if i == owner {
			continue
		}
		if err := r.setFence(ctx, i, namespace, slot, true); err != nil {
			return err
		}
	}
	if next.Epoch != r.current().Epoch {
		r.install(next)
	}
	r.unresolved.Store(false)
	return r.Refresh(ctx)
}

func (r *Router) startAdmin(metrics http.Handler) (*http.Server, error) {
	addr := os.Getenv("KVSTORE_ROUTER_ADMIN_ADDR")
	if addr == "" {
		return nil, nil
	}
	token := os.Getenv("KVSTORE_ROUTER_ADMIN_TOKEN")
	if len(token) < 24 || r.fenceToken == "" {
		return nil, errors.New("router admin requires KVSTORE_ROUTER_ADMIN_TOKEN (24+ chars) and KVSTORE_JOIN_TOKEN")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("router admin listen %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	if metrics != nil {
		mux.HandleFunc("GET /metrics", monitoring.Protect(os.Getenv("KVSTORE_METRICS_TOKEN"), metrics))
	}
	authorized := func(w http.ResponseWriter, req *http.Request) bool {
		got := req.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(got), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return false
		}
		return true
	}
	mux.HandleFunc("GET /v1/shards/layout", func(w http.ResponseWriter, req *http.Request) {
		if !authorized(w, req) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		b, _ := r.StateJSON()
		_, _ = w.Write(b)
	})
	mux.HandleFunc("POST /v1/shards/migrate", func(w http.ResponseWriter, req *http.Request) {
		if !authorized(w, req) {
			return
		}
		var command migrateRequest
		if err := readJSON(w, req, &command); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 30*time.Minute)
		defer cancel()
		result, err := r.MigrateSlot(ctx, command.Namespace, command.Slot, command.Target)
		if err != nil {
			slog.Error("live slot migration failed", "namespace", command.Namespace, "slot", command.Slot, "error", err)
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("POST /v1/shards/recover", func(w http.ResponseWriter, req *http.Request) {
		if !authorized(w, req) {
			return
		}
		var command recoverRequest
		if err := readJSON(w, req, &command); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 60*time.Second)
		defer cancel()
		if err := r.RecoverSlot(ctx, command.Namespace, command.Slot); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"recovered":true}`)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("router admin listener exited", "error", err)
		}
	}()
	return server, nil
}
func readJSON(w http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(w, request.Body, 4096)
	defer request.Body.Close()
	dec := json.NewDecoder(request.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing JSON")
	}
	return nil
}
