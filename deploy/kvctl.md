# KVStore terminal console

## Install and connect

Download `kvctl-<version>-<os>-<arch>` for Linux, macOS (`darwin`), or Windows. Releases provide `amd64` (x86-64) and `arm64` archives. Extract the `.tar.gz` or `.zip` and run `./kvctl --version` (`.\kvctl.exe --version` in PowerShell). Put the binary on your `PATH` to launch it from any directory. [GitHub Releases](https://github.com/byPixelTV/kvstore/releases)

Each interactive launch prompts for a masked API key. Enter a key issued for the standalone server or router you are connecting to. The default address is `127.0.0.1:50051` using plaintext gRPC. Set `--addr HOST:PORT` to prefill the address on the sign-in screen. Use plaintext on a trusted private connection, since the key and data travel in cleartext.

```sh
kvctl
kvctl --addr kvstore.example.com:5443 --tls
kvctl --addr kvstore.example.com:5443 --tls-ca server-ca.crt
kvctl --addr kvstore.example.com:5443 --tls-ca server-ca.crt --tls-cert client.crt --tls-key client.key
```

`--tls` enables TLS with the system trust store. `--tls-ca` enables TLS with the specified CA file. Supply `--tls-cert` and `--tls-key` together when the endpoint requires a client certificate. Use the hostname on the server certificate in `--addr`.

After signing in, press `Ctrl+K` to open the command palette. Use it to switch namespaces, get or change a record, submit a batch, or watch changes. The API key's grants determine which operations and namespaces you can use.

| Key | Action |
| --- | --- |
| `Ctrl+K` or `:` | Open the palette. `Tab` completes commands, namespaces and loaded keys. |
| `1` / `2` / `3` / `4` | Open Data, Watch, Cluster or Access Keys. |
| `N` / `/` / `M` | Choose a namespace, filter loaded keys or load more keys. |
| `Enter` / `G` / `V` | Open the selected value, get an exact key or cycle text, JSON and hex. |
| `S` / `E` / `D` / `B` | Set, edit, delete or submit a batch. |
| `W` | Watch the current namespace. Use `watch <key>` in the palette for one key. |
| `C` / `D` in Access Keys | Create or revoke a named key. |

Enter text or JSON directly in the value editor. Choose `hex` or `base64` for binary bytes. A TTL can be a relative duration such as `15m` or a future RFC3339 timestamp. Editing an existing record prefills its value and expiration. The batch form accepts `value` for text, `hex` for binary bytes or `delete: true` to remove a key.

## Try the Windows demo

From the checkout, with the repository's Go toolchain installed, run this in Windows PowerShell 5.1 or PowerShell 7.

```powershell
.\scripts\start-kvctl-demo.ps1 -CopyKey
```

The launcher starts a standalone server bound to a free loopback port, loads sample records and copies the demo key to your clipboard. Paste it into **Access key** and press **Enter**. The server stops when you close the console. Every run uses a separate folder under `data/`.

## Create or recover a manager key

Create the first manager on a trusted host with filesystem access to the endpoint's API-key registry. Use the same command to recover access if you lose the last manager key. Point `--dir` at the host path for the registry configured through `KVSTORE_API_KEYS_DIR` and choose an unused key name.

```sh
kvstore apikey create --dir "<path>" --name admin --manage --read --write
```

Replace `<path>` with the registry path before running the command. Save the key securely when it is printed, then enter it at the `kvctl` prompt. Bootstrap and recovery require local filesystem access.

After signing in with the management grant, use `keys` in the command palette to list, generate, and revoke API keys. Copy each generated key when it is displayed. The server rereads the same registry to pick up changes. Console generation and revocation require the endpoint process to have write access to the registry directory. For a deployment with several routers, keep all routers on the same authoritative registry so every endpoint applies newly issued keys and revocations consistently.

## Configure the registry on a Linux Compose host

For a standalone endpoint on a trusted private network, create the registry on the host with write access for the container's group and start the API-key overlay. The endpoint listens on `127.0.0.1:50051` by default. The example builds the server from the local checkout and prints the manager key once.

```sh
export KVSTORE_API_KEYS_HOST_DIR="$PWD/data/kvstore-api-keys"
sudo install -d -m 2770 -o "$(id -u)" -g 65532 "$KVSTORE_API_KEYS_HOST_DIR"
kvstore apikey create --dir "$KVSTORE_API_KEYS_HOST_DIR" --name admin --manage --read --write
docker compose -f compose.yml -f compose.solo.apikey.yml up -d --build
```

When using a production TLS overlay, mount the API-key registry separately from the TLS files. Set `KVSTORE_API_KEYS_DIR` to the registry's container path. Use a writable mount for console key management and grant the endpoint process access to the directory. [Production deployment profiles](production.md)
