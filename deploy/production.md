# KVStore auf Linux betreiben

Ein KVStore-Image enthält den Server, `kvbench`, `kvshardctl` und `kvimport`. Ohne Modusangabe startet es als persistenter Einzelknoten. Für replizierte Writes den Cluster-Modus verwenden. `compose.sharded.yml` startet drei Raft-Gruppen mit jeweils drei Votern und einem gRPC-Router. Die Performance-Defaults gelten auch, wenn die Container außerhalb von Compose gestartet werden.

| Cluster-Default | Wert |
| --- | --- |
| Write-Batching-Fenster / maximales Fenster | 1,5 ms / 3 ms |
| Write-Batch-Ziel / Obergrenze | 64 / 256 |
| Raft-In-flight-Limit / Commit-Timeout | 8 / 50 ms |
| Raft AppendEntries / Log-Cache | 128 / 512 |
| STRONG-Read-Batching / parallele Quorumprüfungen | 300 µs / 2 |
| Router-Verbindungen pro Backend | 8 |

Der Durchsatz aus dem Benchmark setzt 50 % STRONG-GETs und 50 % logische Writes über `BatchWrite` mit 16 Operationen pro RPC voraus. Für unabhängige Schlüssel nutzt die Anwendung diese RPC direkt. Ein Batch über mehrere Shards liefert den Status je Operation. Ein Timeout kann nach dem Commit eintreten. Bei einem unbekannten Ausgang den Anwendungszustand prüfen, bevor eine Mutation erneut gesendet wird.

## Container starten

Auf dem Linux-Host Docker Compose und persistente SSD-Volumes bereitstellen. Einen konkreten Image-Tag oder Digest per `KVSTORE_IMAGE` wählen. Für einen lokalen Quellstand zuerst `docker compose -f compose.sharded.yml build` ausführen. Für ein veröffentlichtes Image stattdessen `docker compose -f compose.sharded.yml pull` ausführen. Compose benötigt zwei unabhängige Secrets, die über die Umgebung oder eine geschützte lokale `.env` gesetzt werden können.

```sh
export KVSTORE_JOIN_TOKEN="$(openssl rand -hex 32)"
export KVSTORE_ROUTER_ADMIN_TOKEN="$(openssl rand -hex 32)"
docker compose -f compose.sharded.yml up -d --no-build
docker compose -f compose.sharded.yml ps
```

Die Secrets über Neustarts hinweg beibehalten. Der Router veröffentlicht gRPC für lokale Prüfungen auf `127.0.0.1:50081`; `KVSTORE_SHARDED_GRPC_PORT` setzt bei Bedarf einen anderen lokalen Port. Das Join-Token schützt die HTTP-Admin-API. Internes Raft und gRPC verwenden Klartext, daher bei hostübergreifendem Verkehr ein privates verschlüsseltes Netz verwenden.

Jeder Voter braucht seine eigene persistente `/data`-Zuordnung und eine stabile Identität. Bei einem Update innerhalb derselben Raft-WAL-Version pro Gruppe jeweils einen Voter aktualisieren und auf dessen Rückkehr warten, bevor der nächste Voter gestoppt wird. Die Admin-Metriken sind mit dem Join-Token unter `GET /v1/cluster/performance` erreichbar. Ein altes Raft-BoltDB-Volume braucht vor dem Umstieg einen separat getesteten Export-/Importpfad. Die Compose-Topologie verwendet eigene Volumes je Shard. Einen Restore aus externen Backups separat auf Staging proben.

Compose setzt `KVSTORE_CLUSTER_MIN_VOTERS=3`. Der Leader gibt Client-RPCs nach einer Raft-Barriere mit der erwarteten Voter-Konfiguration frei. Membership und Bootstrap laufen unabhängig davon über Raft und die Admin-API. Bei eigener Orchestrierung dieselbe Variable auf allen Votern setzen.

## Production mit Compose

Für einen einzelnen Server `compose.solo.production.yml` ergänzen. Für Shards wird `compose.production.yml` mit `compose.sharded.yml` kombiniert. Für beide Profile `KVSTORE_IMAGE` auf den freigegebenen Image-Digest setzen. Im Verzeichnis `KVSTORE_TLS_DIR` liegen `server.crt` mit Zertifikatskette, `server.key` und `client-ca.crt` für die dedizierte Client-CA. Der Container läuft mit UID 65532 und braucht Lesezugriff auf diese Dateien. Die in `-addr` oder der Anwendung verwendete gRPC-Adresse muss zum Serverzertifikat passen.

```sh
export KVSTORE_TLS_DIR=/pfad/zu/kvstore-tls
export KVSTORE_METRICS_TOKEN="$(openssl rand -hex 32)"
export KVSTORE_IMAGE=registry.example/kvstore@sha256:IMAGE_DIGEST
docker compose -p kvstore-solo -f compose.yml -f compose.solo.production.yml up -d --no-build
```

Für Shards außerdem `KVSTORE_JOIN_TOKEN` und `KVSTORE_ROUTER_ADMIN_TOKEN` aus dem Secret-Store setzen. Der Produktions-Overlay schaltet den automatischen Raft-Bootstrap ab. Nach dem Start mit drei leeren Volumes je Gruppe einmal gezielt den vorgesehenen Voter initialisieren. Alle drei Admin-Endpunkte einer Gruppe müssen erreichbar sein und denselben Bootstrap-Plan melden. Bei einer vorhandenen Gruppe bleiben die Daten und die Mitgliedschaft auf den Volumes erhalten. Nach Ausfall und Ersatz eines Voters wird derselbe Voter mit seiner bisherigen ID und Adresse gestartet. Der Ersatz holt seinen Stand von der bestehenden Mehrheit.

```sh
docker compose -p kvstore-live -f compose.sharded.yml -f compose.production.yml up -d --no-build
# Nur beim erstmaligen Aufbau mit leeren Volumes, sobald alle Voter gestartet sind.
for port in 17101 17105 17109; do
  printf 'Authorization: Bearer %s\n' "$KVSTORE_JOIN_TOKEN" | \
    curl --fail --silent --show-error --retry 12 --retry-connrefused --retry-delay 1 \
      --header @- --request POST "http://127.0.0.1:$port/v1/cluster/bootstrap" || break
done
```

Der Daten-Endpunkt verwendet natives gRPC-mTLS und bindet standardmäßig an `127.0.0.1:5443`. Für externe Clients `KVSTORE_INGRESS_IP` auf die vorgesehene Host-Adresse setzen. Admin und Metriken sind nur über die Host-Loopback-Adresse veröffentlicht. TLS-Dateien und Zugriffsregeln werden beim Start gelesen. Nach einem Wechsel die Solo-Instanz im Wartungsfenster oder die Router nacheinander neu starten.

Bei einem Upgrade vom früheren Envoy-Overlay den bisherigen `ingress`-Container des Projekts im Wartungsfenster stoppen, damit der Router den TLS-Port übernehmen kann. Die Raft-Volumes behalten.

### Rechte pro Namespace

Optional `KVSTORE_ACCESS_POLICY_FILE=/etc/kvstore/tls/policy.json` setzen und die Datei ins TLS-Verzeichnis legen. Jedes Clientzertifikat braucht genau einen URI-SAN. Die Regel gilt für exakt diese Identität und die angegebenen Namespaces. `read` deckt `Get`, `Scan` und `Watch` ab. `write` deckt `Set`, `Delete` und `BatchWrite` ab. Ein gemischter Batch wird vollständig abgewiesen, sobald für einen Eintrag das Schreibrecht fehlt. `allow_head` erlaubt ausdrücklich den globalen Cursor. Für `kvimport` sind `allow_head` sowie Lese- und Schreibrechte auf die Ziel-Namespaces nötig.

```json
{
  "clients": [
    {"uri": "spiffe://example.org/apps/game", "read": ["players"], "write": ["players"], "allow_head": true},
    {"uri": "spiffe://example.org/apps/reporting", "read": ["players"]}
  ]
}
```

Ohne Policy dürfen alle Clients der Client-CA auf die Daten-API zugreifen. Die Produktionsprofile begrenzen pro Solo-Instanz oder Router standardmäßig auf 1024 zeitgleiche Unary-Requests und 256 Streams. `KVSTORE_MAX_INFLIGHT_RPCS` und `KVSTORE_MAX_STREAMS` setzen diese Grenzen. Bei Überschreitung antwortet der Server vor der Ausführung mit `RESOURCE_EXHAUSTED`.

### Benannte API-Keys

Solo und Router unterstützen API-Keys mit und ohne TLS. `KVSTORE_API_KEYS_DIR` aktiviert die Schlüsselprüfung am Daten-Endpunkt. Für das TLS-Compose-Profil kann die Registry im TLS-Bind liegen. Der Container muss die Dateien als UID oder GID 65532 lesen können. Auf Linux erbt das Verzeichnis im Beispiel die Gruppe an neue Einträge.

```sh
keydir="$KVSTORE_TLS_DIR/api-keys"
install -d -m 2750 -g 65532 "$keydir"
kvstore apikey create --dir "$keydir" --name reporting --read --namespace players
kvstore apikey create --dir "$keydir" --name game --read --write --namespace players
kvstore apikey list --dir "$keydir"
kvstore apikey revoke --dir "$keydir" --name reporting
```

`create` gibt den zufälligen Key einmal aus. Das Verzeichnis enthält den SHA-256-Digest und die Rechte. `list` zeigt Name und Rechte. `--read` erlaubt `Get`, `Head`, `Scan` und `Watch`. `--write` erlaubt `Set`, `Delete` und `BatchWrite`. `Head` enthält einen globalen Cursor. Mit wiederholtem `--namespace` lassen sich Datenzugriffe auf bestimmte Namespaces beschränken. Ohne Namespace-Liste umfasst das Recht alle Namespaces.

```sh
export KVSTORE_API_KEYS_DIR=/etc/kvstore/tls/api-keys
# Im reinen API-Key-Betrieb ohne Clientzertifikate setzen.
export KVSTORE_TLS_CLIENT_CA_FILE=''
```

Clients senden `authorization: Bearer <key>` als gRPC-Metadatum. `kvbench` und `kvimport` unterstützen dafür `KVSTORE_API_KEY`. Im TLS-Betrieb benötigt der Server `server.crt` und `server.key`. In den Tools wählt `-tls` den System-Truststore und `-tls-ca` eine eigene CA-Datei. Mit gesetzter Client-CA können bestehende Clients weiter ihr Zertifikat verwenden. Wird bei einer Anfrage ein Bearer-Key gesendet, gelten dessen Rechte. Ein ungültiger Bearer-Key wird auch bei gültigem Clientzertifikat abgewiesen. Im reinen API-Key-Betrieb `KVSTORE_ACCESS_POLICY_FILE` für Zertifikate leer lassen.

Änderungen am Verzeichnis werden auf jedem Endpunkt im Sekundentakt eingelesen. Ein Widerruf beendet dort auch offene Streams des Keys. Bei mehreren Routern die aktuelle Registry an alle Router verteilen und den Widerruf an jedem Endpunkt wirksam werden lassen. Für lokale Key-Verwaltung genügt ein schreibgeschützter Server-Mount. Soll das Terminal-TUI `kvctl` Keys verwalten, braucht der Endpunkt Schreibzugriff auf die Registry. [Terminal-Konsole und Einrichtung](kvctl.md)

#### Solo ohne Zertifikate

Für einen lokalen oder privaten Solo-Endpunkt verwendet `compose.solo.apikey.yml` die Registry ohne TLS. Die Host-Adresse ist standardmäßig `127.0.0.1:50051`. Ohne TLS laufen Key und Nutzdaten im Klartext. Für öffentliche Endpunkte das TLS-Profil verwenden.

```sh
export KVSTORE_API_KEYS_HOST_DIR="$PWD/data/kvstore-api-keys"
install -d -m 2770 -g 65532 "$KVSTORE_API_KEYS_HOST_DIR"
kvstore apikey create --dir "$KVSTORE_API_KEYS_HOST_DIR" --name minecraft --read --write
docker compose -f compose.yml -f compose.solo.apikey.yml up -d --build
```

Beim direkten Start von Solo oder Router genügt `KVSTORE_API_KEYS_DIR`, wenn die TLS-Dateivariablen leer sind. Für diesen Endpunkt verwenden `kvbench` und `kvimport` den `KVSTORE_API_KEY` ohne TLS-Flags.

Für die erste Anmeldung am TUI auf dem Host einen Management-Key anlegen. Verwende dasselbe Verzeichnis, aus dem der Server seine Keys liest. Der Befehl funktioniert auch mit einer leeren Registry.

```sh
kvstore apikey create --dir "$KVSTORE_API_KEYS_HOST_DIR" --name admin --manage --read --write
kvctl --addr 127.0.0.1:50051
```

### Monitoring und Endpunkt-Abnahme

Für Shards `deploy/prometheus/prometheus.yml`, für Solo `deploy/prometheus/prometheus-standalone.yml` auf dem Compose-Host verwenden. Den Wert von `KVSTORE_METRICS_TOKEN` als Datei `token` neben der Konfiguration ablegen, lesbar nur für den Scraper. Das Prometheus-Target muss `UP` melden. Die Regeln prüfen Scrapes, Shard-Routen, Mutationsfehler, p99, Ressourcenlimits, wenig freien Speicher und Pebble-Compaction-Fehler. Bei der Abnahme auf einer isolierten Staging-Instanz das Unary-Limit vorübergehend auf `1` setzen, mit 32 gleichzeitigen Schreibern wiederholt `ResourceExhausted` auslösen und den Eingang von `KVStoreResourceExhausted` im Alertmanager prüfen. Anschließend das Produktionslimit wiederherstellen.

```sh
kvbench -addr kvstore.example.org:5443 -namespace staging-perf \
  -tls-ca server-ca.crt -tls-cert client.crt -tls-key client.key \
  -mode mixed -consistency strong -read-percent 50 -batch-size 16 \
  -concurrency 256 -connections 16 -keys 16384 -target-ops 23000 -duration 5m -json
```

Auf Staging zusätzlich die Verbindung ohne Clientzertifikat und mit einem Zertifikat einer fremden CA prüfen. Bei gesetzter Policy auch einen Client mit gültigem Zertifikat ohne Namespace-Recht prüfen. Der erste `Get` von `kvbench` prüft die Verbindung, bevor der Benchmark Daten schreibt.

## Offline-Backup und Restore

### Solo

Schreibverkehr beenden und den Container stoppen. Während der Sicherung und Wiederherstellung automatische und manuelle Starts des Projekts sperren. Der Helper findet das persistente `/data`-Volume anhand des gestoppten Compose-Dienstes. Das Backup-Verzeichnis muss neu sein. Anschließend das vollständige Verzeichnis samt Prüfsummen verschlüsselt außerhalb des Hosts ablegen.

```sh
docker compose -p kvstore-solo -f compose.yml -f compose.solo.production.yml stop kvstore
bash scripts/backup-standalone-linux.sh backup kvstore-solo /pfad/zu/neu/kvstore-backup
docker compose -p kvstore-solo -f compose.yml -f compose.solo.production.yml start kvstore
```

Nach Verlust der Platte auf einem anderen Host ein neues Compose-Projekt mit einem leeren Volume und demselben Image-Digest anlegen. `create` bereitet den Container vor, damit der Helper das Zielvolume eindeutig prüfen kann.

```sh
docker compose -p kvstore-solo-restore -f compose.yml -f compose.solo.production.yml create --no-build kvstore
bash scripts/backup-standalone-linux.sh restore kvstore-solo-restore /pfad/zu/kvstore-backup
docker compose -p kvstore-solo-restore -f compose.yml -f compose.solo.production.yml start kvstore
```

Vor Freigabe am Restore-Endpunkt Werte, Revisionen und aktive TTL-Fristen mit dem Sicherungsstand vergleichen. `KVSTORE_TEST_IMAGE` auf das lokal verfügbare Image setzen und `bash scripts/test-backup-standalone-linux.sh` für eine isolierte Probe ausführen. Beim Verlust des Solo-Hosts bleibt der Dienst während des Wiederanlaufs offline. Bei Plattenverlust ist der letzte extern gesicherte Stand das Wiederherstellungsziel.

### Shards

Dieses Verfahren sichert die neun Compose-Volumes gemeinsam. Es erhält Raft-Logs, Pebble-Daten, Revisionen und Routing-Metadaten. Die Quelltopologie muss für die gesamte Sicherung gestoppt bleiben. Vor dem Stoppen laufende Slot-Migrationen abschließen und neuen Clientverkehr sperren.

```sh
docker compose -p kvstore-live -f compose.sharded.yml stop
bash scripts/backup-sharded-linux.sh backup kvstore-live /pfad/zu/neu/kvstore-backup
docker compose -p kvstore-live -f compose.sharded.yml start
```

Bei Betrieb mit mTLS für `stop` und `start` beide Compose-Dateien angeben. Das erzeugte Verzeichnis samt `SHA256SUMS` verschlüsselt außerhalb des Hosts ablegen. Der Backup-Ordner muss neu sein. Bei einem Fehler den Vorgang mit einem neuen Ordner wiederholen. Die Archive enthalten die vollständigen Nutzdaten.

Für einen Restore auf einem isolierten Zielhost denselben Image-Digest und dieselben internen Compose-Dienstnamen verwenden. Der Projektname darf sich ändern. Die Zielcontainer anlegen und bis zum abgeschlossenen Restore ausgeschaltet lassen.

```sh
docker compose -p kvstore-restore -f compose.sharded.yml create --no-build
bash scripts/backup-sharded-linux.sh restore kvstore-restore /pfad/zu/kvstore-backup
docker compose -p kvstore-restore -f compose.sharded.yml start
```

Das Skript prüft alle neun Prüfsummen und verlangt leere Zielvolumes. Nach einem abgebrochenen Restore auf einem neuen leeren Projekt erneut beginnen. Nach dem Start auf volle Readiness warten und vor Clientfreigabe bekannte Schlüssel, Bytewerte und Revisionen aus mehreren Shards mit den vor der Sicherung erfassten Werten vergleichen. Der Helper verwendet GNU tar und GNU coreutils aus `debian:bookworm-slim`. Mit `KVSTORE_BACKUP_HELPER_IMAGE` lässt sich dessen Image-Digest vorgeben.

## Freigabe auf der Zielhardware

Auf einem dedizierten Testhost im Repository ausführen. Der Dauertest verwendet eigene Container und Volumes. Er startet mit drei Votern pro Shard und schreibt fünf Minuten durchgehend.

```sh
bash scripts/benchmark-final-slo-linux.sh
```

Der Test verlangt je mindestens 10.000 STRONG GET/s und 10.000 replizierte SET/s, beide p99 unter 100 ms, null Fehler, null fehlende Records und beobachtete Pebble-Compactions ohne Compaction-Fehler. Das Ergebnisverzeichnis enthält die Client-Messung, Raft-WAL- und Pebble-Metriken sowie CPU- und I/O-Pressure. Die Compose-Testcontainer werden gestoppt und ihre Volumes bleiben für die Diagnose erhalten. Mit dem im Ergebnis gespeicherten Projektnamen kann das Testprojekt danach gezielt entfernt werden.

Für einen längeren Durchlauf `DURATION_SECONDS=1800` setzen. Für eine produktionsnahe Abnahme zusätzlich den mTLS-Zugang mit gültigem Clientzertifikat, ohne Clientzertifikat und mit falscher Client-CA prüfen, einen Prometheus-Alarm auslösen und den Restore auf frischen Volumes durchführen.

Den Ausfalltest mit demselben lokal verfügbaren Image und Docker Compose ab Version 2.24.4 ausführen. Er verwendet ein eigenes Projekt mit zufälligen Loopback-Ports und entfernt dessen Testvolumes nach dem Lauf.

```sh
KVSTORE_TEST_IMAGE=kvstore:multiraft-dev go test -tags integration -count=1 -v ./cmd/kvbench -run '^TestDockerClusterRecovery$'
```

Der Ausfalltest sendet fortlaufend BatchWrites durch den Router, beendet den Leader eines Shards abrupt und wartet auf bestätigte Writes vom neuen Leader. Beim anschließenden Quorumverlust prüft er abgelehnte STRONG Reads und ausbleibende Write-Bestätigungen auf dem betroffenen Shard sowie weiterlaufende Writes auf den anderen Shards. Der ursprüngliche Bootstrap-Voter wird mit einem leeren Ersatzvolume gestartet, und ein lokaler Read prüft den replizierten Stand. Nach einem abrupten Neustart der gesamten Topologie vergleicht der Test alle bestätigten Schlüssel, Werte und Revisionen vom Client mit dem persistierten Zustand.

## Mehrere Hosts und Kubernetes

Auf drei physischen Hosts je einen Voter aus jeder der drei Gruppen platzieren. Pro Gruppe denselben Bootstrap-Voter und dieselbe Liste der Admin-Seeds auf allen drei Votern konfigurieren. Pro Voter `KVSTORE_NODE_ID`, `KVSTORE_RAFT_ADVERTISE_ADDR`, `KVSTORE_GRPC_ADVERTISE_ADDR` und `KVSTORE_ADMIN_ADVERTISE_ADDR` stabil halten. Werden mehrere Voter vom selben Host veröffentlicht, brauchen sie unterscheidbare Ports. Beim Router die gRPC-Endpunkte in `KVSTORE_SHARDS` in derselben Gruppenzuordnung eintragen. Der Einzelhost-Compose-Aufbau eignet sich für die lokale Qualifikation. Vor einer Freigabe für mehrere Hosts den Lasttest mit einem Client am produktionsnahen Router und den Ausfall einer ganzen Fehlerdomäne auf Staging prüfen.

Jede der drei Gruppen verwendet `KVSTORE_RAFT_BOOTSTRAP_MODE=explicit` und `KVSTORE_CLUSTER_MIN_VOTERS=3`. Den ersten Bootstrap je Gruppe ausschließlich bei der Erstinstallation ausführen. Nach Verlust eines Hosts auf einem Ersatzhost dieselben Voter-Identitäten und erreichbaren Adressen verwenden. Erst sicherstellen, dass der alte Host diese Identitäten nicht gleichzeitig verwendet. Den öffentlichen gRPC-Endpunkt über mindestens zwei Router auf getrennten Hosts mit mTLS und einem redundanten Load Balancer betreiben.

Die [Kubernetes-Profile](kubernetes/README.md) dokumentieren Solo, eine Raft-Gruppe und die vollständige sharded Topologie. Die Abnahme umfasst einen echten Worker-Ausfall bei Schreiblast, den Catch-up eines Ersatz-Voters auf frischem Speicher und einen kontrollierten Rollout.

Der externe Recovery-Test sendet während eines angekündigten Zeitfensters BatchWrites in einen reservierten Staging-Namespace. Er meldet die längste Unterbrechung zwischen Bestätigungen je Shard und prüft anschließend alle bestätigten Werte und Revisionen über mTLS. Den Host während des angezeigten Fensters abschalten und den tatsächlichen Ausfall separat anhand der Infrastrukturmeldungen belegen. Der Test verändert keine Hosts.

```sh
KVSTORE_HOST_TEST_ADDR=kvstore-staging.example.org:5443 \
KVSTORE_HOST_TEST_NAMESPACE=staging-host-recovery \
KVSTORE_HOST_TEST_TLS_CA=server-ca.crt \
KVSTORE_HOST_TEST_TLS_CERT=client.crt \
KVSTORE_HOST_TEST_TLS_KEY=client.key \
KVSTORE_HOST_TEST_WINDOW=2m \
  go test -tags integration -count=1 -v ./cmd/kvbench -run '^TestExternalHostRecovery$'
```

## KVStore v1 aus MongoDB importieren

Die Zuordnung folgt den v1-Implementierungen in `kvstore-js` und `kvstore-go`. Vor dem Import v1-Transaktionsrecovery abschließen, Zählerstände zwischen Redis und MongoDB abgleichen und Schreibzugriffe auf Quelle und Ziel für den gesamten Import anhalten. Der Importer bricht bei einem nicht leeren v1-Transaktionsjournal ab. Ein MongoDB-Lesezugang reicht aus.

| MongoDB | Ziel-Namespace | Ziel-Key | Zielwert |
| --- | --- | --- | --- |
| `kv_store_<store>` | `<store>` | `key` | UTF-8-Bytes von `value` |
| `single_value` | `legacy-v1-single` | `storeName` | UTF-8-Bytes von `value` |
| `kv_counters` | `legacy-v1-counter/<storeName>` | `key` | Dezimalzahl als UTF-8-Bytes |

`KVSTORE_MONGO_URI` über die Umgebung setzen. `-dry-run` prüft die Quelle. Mit `-addr` liest der Prüflauf zusätzlich das Ziel und zählt neue Schlüssel, bereits identische Werte und Konflikte. Bei Konflikten meldet der Prüflauf einen Fehler. Ohne Modusangabe läuft ebenfalls ein Dry Run. Erst `-apply` schreibt nach der Quellprüfung.

```sh
go run ./cmd/kvimport -database v1_database -dry-run
go run ./cmd/kvimport -database v1_database -addr 127.0.0.1:50081 -dry-run
go run ./cmd/kvimport -database v1_database -addr 127.0.0.1:50081 -apply
```

Mit `-dry-run -addr … -show-conflicts` gibt der Zielvergleich Namespace und Schlüssel jedes Konflikts aus. Die Werte bleiben aus der Ausgabe. Ändern sich die Quellzähler während eines Laufs, meldet der Importer einen Fehler. Bereits bestätigte Writes bleiben erhalten.

Für den mTLS-Endpunkt `-tls-ca`, `-tls-cert` und `-tls-key` angeben und als Adresse den zertifizierten DNS-Namen verwenden. Bei einem Fehler nach bereits bestätigten Writes kann derselbe Import bei weiterhin eingefrorenen Daten neu gestartet werden. Er überspringt identische Zielwerte und bricht bei Konflikten ab.

Der Import übernimmt noch gültige TTLs als absoluten Unix-Zeitpunkt in Millisekunden. Sind `expiresAt` und `expiresAtDate` beide gesetzt, gilt die frühere Frist. Bereits abgelaufene Records werden übersprungen. `-skip-expiring` ist weiterhin für einen Probeimport verfügbar, der TTL-Einträge bewusst auslässt. Zähler werden als gespeicherte MongoDB-Stände importiert, ohne die Zähler-API oder Redis-Koordination von v1 zu übernehmen.

Vor dem ersten TTL-Import alle Router und Raft-Voter auf die TTL-fähige Version aktualisieren und ihre Uhrzeiten synchronisieren. `Get` und `Scan` liefern die Frist und blenden den Wert ab dem gespeicherten Zeitpunkt aus. `Watch` liefert dieselbe Frist für Client-Caches. Diese müssen den Wert zur Frist verwerfen. Die serverseitige Sichtbarkeit hängt unmittelbar an der Frist, und abgelaufene Bytes bleiben bis zur Überschreibung oder expliziten Löschung auf dem Datenträger.

### Wiederholbarer Probeimport

Der separate Compose-Cluster verwendet frische Volumes pro Projekt und veröffentlicht nur den Router auf Loopback. Unter PowerShell 7 lässt er sich mit `./scripts/start-import-rehearsal.ps1` starten. Der Standardport ist `18082`. Für einen weiteren Lauf mit leeren Volumes einen neuen Namen und freien Port wählen, etwa `./scripts/start-import-rehearsal.ps1 -Project kvstore-import-rehearsal-run2 -Port 18084 -SkipBuild`. Ein bereits verwendeter Projektname behält seine Daten.

Für einen konsistenten Capture eine unveränderte MongoDB-Testkopie verwenden oder die v1-Schreiber während des Exports anhalten. Der Export liest über mehrere Collections. Mit aktiven Schreibern ergibt er eine feste Datei für den Probeimport, aber keinen gemeinsamen Zeitpunkt für alle Collections. `KVSTORE_MONGO_URI` dafür wie oben setzen. Die Snapshot-Datei enthält Nutzdaten und gehört in ein privates Verzeichnis. `data/` und Dateien mit der Endung `.kvsnapshot` sind vom Git- und Docker-Build-Kontext ausgeschlossen.

```powershell
New-Item -ItemType Directory -Force ./data/import-rehearsal | Out-Null
$snapshot = "./data/import-rehearsal/betterattack-$(Get-Date -Format yyyyMMdd-HHmmss).kvsnapshot"
$target = '127.0.0.1:18084'
go run ./cmd/kvimport -database 'betterattack' -export-file $snapshot
```

Für einen frischen TTL-Probecluster auf dem Beispielport `./scripts/start-import-rehearsal.ps1 -Project kvstore-import-rehearsal-ttl -Port 18084` verwenden. Der Export verlangt einen neuen Dateinamen und meldet die Anzahl übernommener TTL-Einträge unter `expiring`. Ein älterer Snapshot, der mit `-skip-expiring` erstellt wurde, muss dafür neu exportiert werden. Ab hier arbeiten alle Befehle mit derselben Datei. Die MongoDB-Verbindung wird nur für den Export gebraucht.

```powershell
go run ./cmd/kvimport -input-file $snapshot -addr $target -dry-run -show-conflicts
go run ./cmd/kvimport -input-file $snapshot -addr $target -apply
go run ./cmd/kvimport -input-file $snapshot -addr $target -verify -show-conflicts
```

Jeden Befehl erst nach erfolgreichem Abschluss des vorherigen ausführen. Auf einem frischen Ziel ergeben `would_write` und `expired_since_validation` im Dry Run zusammen die Snapshot-Anzahl. Nach dem Import muss die Verifikation `missing=0 changed=0 extra=0` melden. `matching + expired_since_export` soll die Snapshot-Anzahl ergeben. Der Verifikationslauf vergleicht Wert und Frist in allen im Snapshot erfassten Namespaces. KVStore bietet keinen globalen Namespace-Scan. Frische Zielvolumes sind deshalb Teil der Prüfung auf zusätzliche Daten. Während des Probeimports und der Verifikation das Ziel exklusiv für den Importer verwenden.

Der Snapshot-Import prüft das Ziel mit einem Scan pro Namespace. Er bricht bei Konflikten vor den Writes ab und schreibt fehlende Einträge in begrenzten Raft-Batches. Bei einer Unterbrechung zuerst den laufenden Import beenden und dann denselben Snapshot erneut anwenden. Bereits bestätigte Einträge werden im nächsten Zielscan als identisch erkannt. Bei einem Batch-Fehler können einzelne Shards bereits bestätigt haben. Der Importer meldet diese Bestätigungen und bricht ab, damit ein weiterer Lauf den Zielstand zuerst abgleichen kann.
