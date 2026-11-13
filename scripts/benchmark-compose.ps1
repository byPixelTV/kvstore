# Measures identical gRPC workloads against standalone and 3-node Raft+Envoy.
# Does NOT delete volumes. Results are saved under benchmarks/results/.
param(
    [int]$DurationSeconds = 15,
    [int]$WarmupSeconds = 3,
    [int]$Concurrency = 16,
    [int]$Connections = 4,
    [int]$ValueSize = 256,
    [int]$Keyspace = 1000,
    [int]$Watchers = 5,
    [int]$WatchRate = 100
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path 'bin', 'benchmarks/results' | Out-Null

Write-Host 'Building kvbench...'
go build -o ./bin/kvbench.exe ./cmd/kvbench
if ($LASTEXITCODE -ne 0) { throw 'go build kvbench failed' }

if (-not $env:KVSTORE_JOIN_TOKEN) {
    $env:KVSTORE_JOIN_TOKEN = [guid]::NewGuid().ToString('N')
}

function Assert-Exit([string]$Name) {
    if ($LASTEXITCODE -ne 0) { throw "$Name failed with exit code $LASTEXITCODE" }
}
function Run-Workload([string]$Deployment, [string]$Mode) {
    $path = "benchmarks/results/$Deployment-$Mode.json"
    $args = @('-addr', '127.0.0.1:50051', '-mode', $Mode,
        '-duration', "${DurationSeconds}s", '-warmup', "${WarmupSeconds}s",
        '-concurrency', "$Concurrency", '-connections', "$Connections",
        '-keys', "$Keyspace", '-value-size', "$ValueSize",
        '-watchers', "$Watchers", '-watch-rate', "$WatchRate", '-json')
    for ($attempt=1; $attempt -le 4; $attempt++) {
        # Head connectivity / Raft leader readiness is checked inside kvbench.
        $output = & ./bin/kvbench.exe @args
        if ($LASTEXITCODE -eq 0) {
            $output | Set-Content -Path $path -Encoding utf8
            $r = Get-Content $path -Raw | ConvertFrom-Json
            Write-Host "$Deployment / ${Mode}: GET $([math]::Round($r.get.ops_sec)) ops/s; SET $([math]::Round($r.set.ops_sec)) ops/s; GET p99 $($r.get.latency.p99_ms)ms; SET p99 $($r.set.latency.p99_ms)ms"
            if ($r.watch) { Write-Host "  Watch delivery: $([math]::Round($r.watch.deliveries_per_sec)) events/s; p99 $($r.watch.delivery_latency.p99_ms)ms" }
            return
        }
        if ($attempt -ge 4) { throw "Benchmark $Deployment/$Mode failed" }
        Start-Sleep -Seconds 3
    }
}

Write-Host 'Starting standalone...'
docker compose -f compose.cluster.yml down
Assert-Exit 'stop cluster'
docker compose up -d --build
Assert-Exit 'start standalone'
Start-Sleep -Seconds 5
foreach ($mode in 'get', 'set', 'mixed', 'watch') { Run-Workload 'standalone' $mode }

Write-Host 'Starting Raft cluster (3 nodes + Envoy)...'
docker compose down
Assert-Exit 'stop standalone'
docker compose -f compose.cluster.yml up -d --build
Assert-Exit 'start cluster'
Start-Sleep -Seconds 10
foreach ($mode in 'get', 'set', 'mixed', 'watch') { Run-Workload 'cluster' $mode }
Write-Host 'Done. JSON reports: benchmarks/results/'
Write-Host 'Cluster remains running. No Docker volumes were deleted.'
