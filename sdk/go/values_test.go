package kvstore_test

import (
	"testing"

	sdk "github.com/byPixelTV/kvstore/sdk/go"
)

func TestReadJSON(t *testing.T) {
	client := testClient(t)
	ctx := t.Context()
	namespace := "sdk-go-readjson"
	if _, err := client.Set(ctx, namespace, "value", []byte(`{"coins":7}`), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Set(ctx, namespace, "empty", nil, 0); err != nil {
		t.Fatal(err)
	}
	type player struct{ Coins int }
	value, found, err := sdk.ReadJSON[player](ctx, client, namespace, "value", sdk.JSONReadOptions{})
	if err != nil || !found || value.Coins != 7 {
		t.Fatalf("read value = %+v, found = %v, err = %v", value, found, err)
	}
	if _, found, err := sdk.ReadJSON[player](ctx, client, namespace, "empty", sdk.JSONReadOptions{}); !found || err == nil {
		t.Fatalf("empty value: found = %v, err = %v", found, err)
	}
	if _, found, err := sdk.ReadJSON[player](ctx, client, namespace, "empty", sdk.JSONReadOptions{EmptyAsMissing: true}); found || err != nil {
		t.Fatalf("empty as missing: found = %v, err = %v", found, err)
	}
}
