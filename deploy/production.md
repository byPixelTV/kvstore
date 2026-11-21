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

## Authentifizierter Clientzugang und Monitoring

Für mTLS den Compose-Overlay ergänzen. Im Verzeichnis `KVSTORE_TLS_DIR` liegen `server.crt` mit Zertifikatskette, `server.key` und `client-ca.crt` für eine dedizierte Client-CA. Der Envoy-Prozess braucht Lesezugriff. Clients brauchen ein von dieser CA signiertes Clientzertifikat und müssen die Identität des Serverzertifikats prüfen. Alle von der Client-CA autorisierten Clients haben Zugriff auf die gRPC-Daten-API.

```sh
export KVSTORE_TLS_DIR=/pfad/zu/kvstore-tls
export KVSTORE_METRICS_TOKEN="$(openssl rand -hex 32)"
docker compose -f compose.sharded.yml -f compose.production.yml up -d --no-build
```

Der Overlay veröffentlicht Envoy standardmäßig auf `127.0.0.1:5443` und entfernt den veröffentlichten Klartext-gRPC-Port des Routers. Für externe Clients `KVSTORE_INGRESS_IP` auf die vorgesehene Host-Adresse setzen. Admin und Metriken bleiben auf Loopback. Nach einem Zertifikatswechsel Envoy neu starten.

Der Overlay verwendet Envoy 1.40.2. `KVSTORE_ENVOY_IMAGE` erlaubt einen eigenen Image-Tag oder Digest. [Envoy-Versionshistorie](https://www.envoyproxy.io/docs/envoy/v1.40.2/version_history/current)

Prometheus auf dem Compose-Host mit `deploy/prometheus/prometheus.yml` konfigurieren. Den Wert von `KVSTORE_METRICS_TOKEN` als Datei `token` neben der Konfiguration ablegen, lesbar nur für den Scraper. Die beiliegenden Regeln erkennen ausgefallene Scrapes, unerreichbare Shards, fehlgeschlagene Mutationen einschließlich Teilfehlern in BatchWrite, p99 ab 100 ms, wenig freien Speicher und Pebble-Compaction-Fehler. Den Benachrichtigungskanal im vorhandenen Alertmanager konfigurieren und bei der Abnahme eine Meldung auslösen.

## Offline-Backup und Restore

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

Der Ausfalltest sendet fortlaufend BatchWrites durch den Router, beendet den Leader eines Shards abrupt und wartet auf bestätigte Writes vom neuen Leader. Beim anschließenden Quorumverlust prüft er abgelehnte STRONG Reads und ausbleibende Write-Bestätigungen auf dem betroffenen Shard sowie weiterlaufende Writes auf den anderen Shards. Nach einem abrupten Neustart der gesamten Topologie vergleicht er alle bestätigten Schlüssel, Werte und Revisionen vom Client mit dem persistierten Zustand.

## Mehrere Hosts

Auf drei physischen Hosts je einen Voter aus jeder der drei Gruppen platzieren. Pro Gruppe denselben Bootstrap-Voter und dieselbe Liste der Admin-Seeds auf allen drei Votern konfigurieren. Pro Voter `KVSTORE_NODE_ID`, `KVSTORE_RAFT_ADVERTISE_ADDR`, `KVSTORE_GRPC_ADVERTISE_ADDR` und `KVSTORE_ADMIN_ADVERTISE_ADDR` stabil halten. Werden mehrere Voter vom selben Host veröffentlicht, brauchen sie unterscheidbare Ports. Beim Router die gRPC-Endpunkte in `KVSTORE_SHARDS` in derselben Gruppenzuordnung eintragen. Der Einzelhost-Compose-Aufbau eignet sich für die lokale Qualifikation. Vor einer Freigabe für mehrere Hosts den Lasttest mit einem Client am produktionsnahen Router und den Ausfall einer ganzen Fehlerdomäne auf Staging prüfen.

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
