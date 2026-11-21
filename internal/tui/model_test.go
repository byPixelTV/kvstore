package tui

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/byPixelTV/kvstore/internal/console"
	"github.com/charmbracelet/x/ansi"
)

func TestLoginRequiresAndMasksKey(t *testing.T) {
	var received string
	m := newModel(t.Context(), console.Endpoint{Address: "127.0.0.1:50051"}, func(_ context.Context, _ console.Endpoint, key string) (console.Client, console.AccessKey, error) {
		received = key
		return nil, console.AccessKey{}, nil
	})
	if cmd := m.beginLogin(); cmd != nil || !m.status.failure {
		t.Fatal("login was attempted without a key")
	}
	const secret = "kvs_example_key_used_only_in_this_test"
	m.login.key.SetValue(secret)
	view := ansi.Strip(m.View().Content)
	if !strings.Contains(strings.ToLower(view), "access key") || strings.Contains(view, secret) {
		t.Fatal("login does not show a masked access-key prompt")
	}
	cmd := m.beginLogin()
	if cmd == nil || m.login.key.Value() != "" || strings.Contains(m.View().Content, secret) {
		t.Fatal("pending login retained the access key in the UI")
	}
	_ = cmd()
	if received != secret {
		t.Fatal("login did not pass the entered credential to the transport")
	}
}

func TestBinaryEditKeepsValueAndAbsoluteExpiry(t *testing.T) {
	m := newModel(t.Context(), console.Endpoint{}, nil)
	m.identity.Write = true
	m.namespace = "inventory"
	value := []byte{0, 255, 1, 92}
	deadline := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	_ = m.openEdit(console.Record{Namespace: m.namespace, Key: "a", Value: value, ExpiresAt: deadline})
	if m.dialog == nil {
		t.Fatal("edit form was not opened")
	}
	fields := m.dialog.fields
	decoded, err := decodeValue(fields[2].input.Value(), fields[1].area.Value())
	if err != nil || !bytes.Equal(decoded, value) {
		t.Fatalf("binary round trip: %x, %v", decoded, err)
	}
	ttl, err := expiry(time.Now(), fields[3].input.Value())
	if err != nil || !ttl.Equal(deadline) {
		t.Fatalf("edit shifted the expiration: %v, %v", ttl, err)
	}
}

func TestCompletionUsesSessionRightsAndData(t *testing.T) {
	m := newModel(t.Context(), console.Endpoint{}, nil)
	m.identity = console.AccessKey{Read: true}
	m.namespace = "players"
	m.namespaces = []string{"players", "game history"}
	m.pages["players"] = keyPage{items: []keyItem{{key: "player-1"}, {key: "player-2"}, {key: "world"}}}
	for prefix, want := range map[string][]string{
		"ge": {"get"}, "use game": {"use game history"}, "get player": {"get player-1", "get player-2"},
		"create-": nil, "set": nil,
	} {
		if got := m.matches(prefix); !slices.Equal(got, want) {
			t.Errorf("complete %q got %v; want %v", prefix, got, want)
		}
	}
}

type revokeClient struct {
	console.Client
	names []string
}

func (c *revokeClient) RevokeKey(_ context.Context, name string) error {
	c.names = append(c.names, name)
	return nil
}

func TestRevokeRequiresManagerAndExplicitConfirmation(t *testing.T) {
	server := &revokeClient{}
	m := newModel(t.Context(), console.Endpoint{}, nil)
	m.client = server
	m.accessKeys = []console.AccessKey{{Name: "reporting"}}
	if cmd := m.confirmRevoke(); cmd != nil || m.dialog != nil {
		t.Fatal("a non-manager can open the revoke dialog")
	}
	m.identity.Manage = true
	_ = m.confirmRevoke()
	if m.dialog == nil || m.dialog.kind != confirmDialog {
		t.Fatal("manager did not receive a revoke confirmation")
	}
	m.updateDialog(tea.KeyPressMsg{Code: tea.KeyEnter})
	if len(server.names) != 0 || m.dialog == nil {
		t.Fatal("Enter unexpectedly confirmed a revocation")
	}
	m.updateDialog(tea.KeyPressMsg{Code: tea.KeyEscape})
	if m.dialog != nil || len(server.names) != 0 {
		t.Fatal("cancel did not close the dialog without revoking")
	}
	_ = m.confirmRevoke()
	cmd := m.updateDialog(tea.KeyPressMsg{Code: 'y', Text: "y"})
	if cmd == nil {
		t.Fatal("explicit confirmation did not enqueue revocation")
	}
	_ = cmd()
	if !slices.Equal(server.names, []string{"reporting"}) {
		t.Fatalf("revoked keys = %v", server.names)
	}
}

func TestTerminalSafetyAndCompactLayout(t *testing.T) {
	value := "a\x1b]52;c;injection\x07\u202e\r\n" + string([]byte{255})
	if got := safeLine(value); got != `a\x1b]52;c;injection\a\u202e\r\n\xff` {
		t.Fatalf("terminal control conversion = %q", got)
	}
	m := newModel(t.Context(), console.Endpoint{Address: "127.0.0.1:50051"}, nil)
	m.Update(tea.WindowSizeMsg{Width: 32, Height: 12})
	rows := strings.Split(m.View().Content, "\n")
	if len(rows) > 12 {
		t.Fatalf("compact login renders %d rows into 12", len(rows))
	}
	for _, row := range rows {
		if width := ansi.StringWidth(row); width > 32 {
			t.Fatalf("compact login row is %d cells wide", width)
		}
	}
}
