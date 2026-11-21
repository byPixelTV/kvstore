package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/byPixelTV/kvstore/internal/apikeys"
)

func runAPIKey(args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: kvstore apikey create|list|revoke")
	}
	command := args[0]
	flags := flag.NewFlagSet("kvstore apikey "+command, flag.ContinueOnError)
	flags.SetOutput(errOut)
	dir := flags.String("dir", os.Getenv("KVSTORE_API_KEYS_DIR"), "API key registry directory")
	var key apikeys.Key
	switch command {
	case "create":
		flags.StringVar(&key.Name, "name", "", "unique name for the key")
		flags.BoolVar(&key.Read, "read", false, "allow Get, Head, Scan and Watch")
		flags.BoolVar(&key.Write, "write", false, "allow Set, Delete and BatchWrite")
		flags.BoolVar(&key.Manage, "manage", false, "allow remote API key listing, creation and revocation")
		flags.Func("namespace", "restrict access to this namespace (repeatable)", func(namespace string) error {
			key.Namespaces = append(key.Namespaces, namespace)
			return nil
		})
	case "revoke":
		flags.StringVar(&key.Name, "name", "", "name of the key to revoke")
	case "list":
	default:
		return fmt.Errorf("unknown API key command %q; use create, list or revoke", command)
	}
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *dir == "" {
		return errors.New("specify --dir or KVSTORE_API_KEYS_DIR and use only named options")
	}
	switch command {
	case "create":
		secret, err := apikeys.Create(*dir, key)
		if secret != "" {
			_, outputErr := fmt.Fprintln(out, secret)
			return errors.Join(err, outputErr)
		}
		return err
	case "revoke":
		return apikeys.Revoke(*dir, key.Name)
	default:
		keys, err := apikeys.List(*dir)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(keys)
	}
}
