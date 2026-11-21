# KVStore on Kubernetes

Use `kubectl kustomize` and `kubectl apply -k` with one of these directories.

| Directory | Topology | Client endpoint |
| --- | --- | --- |
| `deploy/kubernetes` | One persistent standalone pod | `kvstore` ClusterIP, gRPC mTLS port `50051` |
| `deploy/kubernetes/production` | Three Raft groups of three voters and two routers | `kvstore-router` LoadBalancer, gRPC mTLS port `5443` |
| `deploy/kubernetes/cluster` | One Raft group of three voters | `kvstore-raft` ClusterIP, private cleartext gRPC port `50051` |

Use a dedicated namespace per installation and a NetworkPolicy-capable CNI. The Raft profiles need three eligible Linux workers with distinct `kubernetes.io/hostname` values on separate physical hosts. For rescheduling all failed voters while a worker remains offline, provide a fourth eligible worker with enough capacity and storage that can attach there. Use a default StorageClass that provisions durable `ReadWriteOnce` volumes, or set `storageClassName` in each claim template in a site overlay before first deployment. The templates request 2 GiB each. Size storage and resources for the target load. Put private Raft traffic on a protected, encrypted worker network.

The manifests assume `cluster.local` and CoreDNS pods labeled `k8s-app: kube-dns` in `kube-system`. Match `CLUSTER_DOMAIN` on all voters and routers to the cluster, and adapt the `kvstore-dns` policy if your CNI or DNS setup uses another resolver such as NodeLocal DNSCache.

## Configure and install

Use a site Kustomization with the selected profile in `resources`, the site namespace in `namespace`, and `images` mapping `kvstore` to an immutable digest that includes explicit Raft bootstrap and native mTLS. The checked-in `kvstore:dev` is a local build tag. Keep the StatefulSet and governing Service names from the profile. Use your existing TCP load balancer with TLS passed through to the routers. Patch the `kvstore-router` Service with the provider's private LB annotations or `loadBalancerClass` and any `loadBalancerSourceRanges` needed at your site. Set a DNS name for the Service and include it in the server certificate's DNS SANs. Use the Service's intended hostname in the solo certificate too.

Create the namespace and provision these Secrets there through your normal secret manager or from protected local files. Token files must contain the token bytes without a trailing newline. Tokens should be randomly generated and independent. Use at least 24 characters for the router admin token.

| Secret | Keys | Profiles |
| --- | --- | --- |
| `kvstore-server-tls` | `server.crt` with its certificate chain, `server.key`, `client-ca.crt` with a dedicated client CA | Solo, production |
| `kvstore-access-policy` (optional) | `policy.json` and `path` with literal value `/etc/kvstore/access/policy.json` | Solo, production |
| `kvstore-raft-auth` | `join-token` | Cluster, production |
| `kvstore-router-auth` | `admin-token` | Production |
| `kvstore-metrics` (optional) | `token` | All profiles |

```sh
ns=kvstore-staging
kubectl create namespace "$ns"
kubectl -n "$ns" create secret generic kvstore-server-tls \
  --from-file=server.crt --from-file=server.key --from-file=client-ca.crt
kubectl -n "$ns" create secret generic kvstore-access-policy \
  --from-file=policy.json --from-literal=path=/etc/kvstore/access/policy.json
kubectl -n "$ns" create secret generic kvstore-raft-auth --from-file=join-token
kubectl -n "$ns" create secret generic kvstore-router-auth --from-file=admin-token
```

With a policy, match each client's URI SAN to its exact `read` and `write` namespace grants. For the staging examples, grant the test client's URI both permissions on `kvstore-staging`.

```json
{"clients":[{"uri":"spiffe://clients/kvstore-staging","read":["kvstore-staging"],"write":["kvstore-staging"],"allow_head":true}]}
```

With only the TLS Secret, every client certified by the client CA can use the data API. Keep the policy Secret provisioned when enforcing namespace grants. Restart the TLS workloads after updating the certificate or policy Secrets. Grant `pods/portforward` only to authorized operators. Raft admin is restricted by NetworkPolicy to same-group voters and the scraper pods described below. Router admin accepts only designated scraper pods over the pod network. Raft and router admin operations require their own tokens. In the single-group profile, label private client pods in this namespace `app.kubernetes.io/component=kvstore-client` to allow port `50051`.

```sh
profile=path/to/site/kustomization-dir
kubectl kustomize "$profile"
kubectl -n "$ns" apply -k "$profile"
kubectl -n "$ns" get pods,svc,pvc,pdb -o wide
```

Solo needs no bootstrap. Wait for `kvstore-0` to become Ready. Keep replicas at one. During staging, write a record with the TLS client, restart the pod with the same claim, and confirm a STRONG Get returns the same bytes and revision. If using `kubectl port-forward svc/kvstore 15051:50051`, connect to `127.0.0.1:15051` and use `grpcurl -authority` with the server's certified DNS name. Use the same technique with `svc/kvstore-router 15443:5443` to check router mTLS before the load balancer is available.

### Initialize a new Raft installation

For each new group (`shard-0-kvstore-raft`, `shard-1-kvstore-raft`, `shard-2-kvstore-raft`, or `kvstore-raft` in the single-group profile), wait for all three voters to be **Running**. Their peer endpoints are published before Ready. In a second terminal, port-forward the group's ordinal zero admin port.

```sh
group=shard-0-kvstore-raft
kubectl -n "$ns" port-forward "pod/$group-0" 17001:7001
```

On the operator workstation, using the same `join-token` file used for the Secret, inspect the new group's bootstrap state before approving it. Check that `node_id` and `bootstrap_node` both name the selected ordinal zero.

```sh
admin=http://127.0.0.1:17001/v1/cluster
printf 'Authorization: Bearer %s\n' "$(cat join-token)" | \
  curl --fail-with-body --silent --show-error --header @- "$admin/bootstrap"
# Once for this brand-new group after the state check above. Success is HTTP 204.
printf 'Authorization: Bearer %s\n' "$(cat join-token)" | \
  curl --fail-with-body --silent --show-error --header @- --request POST \
    --write-out '%{http_code}\n' "$admin/bootstrap"
```

Repeat for every new group, moving the forward each time. The POST checks that all configured peers are reachable with pristine, matching bootstrap plans. Investigate any conflict or uncertain response and check current membership before taking another action. Ordinary restarts, upgrades and empty-volume replacements join existing peers; reserve bootstrap for provisioning a new group on empty storage. After initialization, wait for all voter pods and both routers to become Ready. Verify `GET /v1/cluster/performance` over an authenticated pod forward reports `voters: 3` on each voter. Test the external LoadBalancer with a valid client certificate and verify rejection without a certificate and with a certificate outside the trusted client CA. If using an access policy, verify an ungranted namespace is denied.

### Metrics

To enable scraping, create `kvstore-metrics` with `--from-file=token` before startup. Label the authorized Prometheus scraper pods in the same namespace `app.kubernetes.io/component=kvstore-metrics-scraper`. Give the scraper the metrics token as a bearer credential. Use Kubernetes pod discovery to scrape `/metrics` from each voter pod on `7001` (`kvstore-voters`), each router pod on `7002` (`kvstore-router`), and the solo pod on `7002` (`kvstore-standalone`) for the deployed profile. Load the rules in `deploy/prometheus/kvstore-alerts.yml` and test delivery through the site's Alertmanager. Solo and router start with caps of `1024` in-flight RPCs and `256` concurrent streams. Tune for the target workload and track rejected RPCs.

## Scheduled upgrades

During a maintenance window, start with all voters and both routers healthy. Check release compatibility and available provisioning capacity. Pause any automated reconciliation that could restart pods during this window. Stage the desired image in the site overlay. Update the live template of each voter StatefulSet with `kubectl -n "$ns" set image "statefulset/$group" kvstore="$image"`. `OnDelete` leaves current pods running until explicitly replaced. Delete one pod normally and wait for the same ordinal on the same claim to be Ready on the new image. Confirm all three voters, direct local catch-up and a successful STRONG read through the router before moving on to the next pod. Work through one group at a time and stop on failure. Each group has a two-pod voluntary disruption budget, which governs eviction and node drains. Direct pod deletion must be coordinated by the operator.

After the voters are healthy on the new image, update `deployment/kvstore-router` with `kubectl set image` in the same namespace and wait for `kubectl -n "$ns" rollout status deployment/kvstore-router`. Its rolling strategy requires a spare eligible host for the surge pod and keeps the two existing replicas available during the update. Apply the site configuration with the new image and resume reconciliation. Use the same voter sequence for the single-group profile.

## Staging host loss and fresh-volume recovery

Run the [external recovery test](../production.md#mehrere-hosts-und-kubernetes) against the staging LoadBalancer during this drill. Set `KVSTORE_HOST_TEST_NAMESPACE` to the namespace in the client's access policy. The client needs `allow_head` to read the cluster cursor. The test verifies values and revisions for all acknowledged writes and reports the longest gap between acknowledgements per shard. Record the actual worker shutdown independently.

Run on an isolated staging installation with three healthy groups, two routers, a spare worker, a portable StorageClass, and disposable data. Record the output of `kubectl -n "$ns" get pod,pvc -o wide`, the expected voters on each worker, and existing Pod and PVC UIDs. Run the tests through the real TLS load balancer, using `grpcurl`, `jq` and `openssl` from the repository root.

```sh
set -o pipefail   # Bash
endpoint=kvstore-staging.example.com:5443
app_ns=kvstore-staging
journal="$(mktemp)"
printf 'Verification journal %s\n' "$journal"
rpc() {
  grpcurl -max-time 5 -cacert server-ca.crt -cert client.crt -key client.key \
    -import-path api/proto -proto kvstore/v1/kvstore.proto -d @ \
    "$endpoint" "kvstore.v1.KVStore/$1"
}
record() {
  local key value revision
  for key in "$@"; do
    value="$(openssl rand -base64 24)" || return 1
    revision="$(jq -cn --arg ns "$app_ns" --arg k "$key" --arg v "$value" \
      '{namespace:$ns,key:$k,value:$v}' | rpc Set | jq -er '.revision')" || return 1
    jq -cn --arg k "$key" --arg v "$value" --arg r "$revision" \
      '{key:$k,value:$v,revision:$r}' >> "$journal" || return 1
  done
}
verify() {
  local expected
  [ -s "$journal" ] || return 1
  while IFS= read -r expected; do
    jq -c --arg ns "$app_ns" '{namespace:$ns,key,consistency:"READ_CONSISTENCY_STRONG"}' \
      <<< "$expected" | rpc Get | jq -e --argjson row "$expected" \
      '.found and .value == $row.value and .revision == $row.revision' >/dev/null || return 1
  done < "$journal"
}
record before-4 before-0 before-18 && verify
```

The `before` keys above target shards 0, 1, 2 respectively in an unmigrated layout. The `during` keys below target those same groups. Keep the journal off the workers. Require exit status zero from each `record` and `verify` before proceeding. On a write timeout, determine its outcome with a STRONG Get before attempting another mutation on that key.

1. Pick a worker with a leader (use authenticated per-pod `/v1/cluster/performance`) and one of the routers. If those roles are on different workers, test them in separate outages and restore the full topology between tests. Power off the worker or stop its VM at the host layer. Watch the node report `NotReady` or `Unknown` and record the time to recover reads and acknowledged writes while the worker is still off. Once the remaining voters elect leaders, run `verify`, then `record during-8 during-0 during-4`, then `verify` again. Use a fresh journal and baseline for a separate outage. If rescheduling is blocked on a powered-off worker, confirm it is fenced and use the Kubernetes out-of-service procedure for pod removal and CSI detach. Release a pod identity or storage attachment only after confirming the old instance is stopped. Wait for voters to return on eligible workers with their original claims. Confirm three Ready voters per group, two Ready routers, and another successful `verify`. Restore the worker and clear its out-of-service taint if you added one. [Kubernetes node shutdown procedures](https://kubernetes.io/docs/concepts/cluster-administration/node-shutdown/)
2. Rehearse **loss of the original bootstrap voter's storage** on the disposable staging shard 0. Pause deployment reconciliation. Confirm both other replicas are healthy, then orphan only shard 0's StatefulSet to prevent an immediate pod recreation while preserving its two surviving pods. Delete only `shard-0-kvstore-raft-0` normally and wait for it to terminate and for storage to detach. Delete its old claim `data-shard-0-kvstore-raft-0`. Verify this specific claim is gone and its old volume cannot be rebound. Reapply the same site overlay so Kubernetes adopts the two surviving pods and provisions a new claim for ordinal zero. Verify the replacement joins the surviving peers on its own.

   ```sh
   group=shard-0-kvstore-raft
   kubectl -n "$ns" delete statefulset "$group" --cascade=orphan --wait
   kubectl -n "$ns" delete pod "$group-0" --wait
   # After checking CSI detach and the exact claim name:
   kubectl -n "$ns" delete pvc "data-$group-0" --wait
   # After confirming the old volume cannot be rebound:
   kubectl -n "$ns" apply -k "$profile"
   ```

3. In shard 0, check that only ordinal zero has new Pod and PVC UIDs and its claim binds fresh storage. Confirm the group has three Ready voters. Run `verify` through the router again. To prove ordinal zero itself caught up, forward its port `50051` via Kubernetes RBAC and use `grpcurl -plaintext` against localhost with the same API proto and `READ_CONSISTENCY_EVENTUAL`. Poll the replacement for `before-4` and `during-8` until both values match the journal. The local revision must equal the journal's router revision masked to the low 56 bits. Compare the integers without floating-point rounding. Resume reconciliation only after these checks pass.

If the rehearsal is interrupted after orphaning the StatefulSet, reapply the same site overlay to restore controller ownership and check the live Pod, PVC and attachment state before continuing.

To clean up, first stop the site reconciliation, then use `kubectl -n "$ns" delete -k "$profile"`. PVCs are retained by default. Review the claims and backing volume reclaim policies before deleting any claims for the disposable staging run. Delete the dedicated staging namespace only when you intend to discard its Secrets, claims, and remaining resources. [Kubernetes StatefulSet deletion and volumes](https://kubernetes.io/docs/tasks/run-application/delete-stateful-set/)
