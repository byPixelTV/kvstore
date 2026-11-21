param(
    [ValidatePattern('^kvstore-import-rehearsal(?:-[a-z0-9][a-z0-9-]*)?$')]
    [string]$Project = 'kvstore-import-rehearsal',
    [ValidateRange(1, 65535)]
    [int]$Port = 18082,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root ".env.$Project"
$image = 'kvstore:import-rehearsal'
$previousImage = $env:KVSTORE_IMAGE
$previousPort = $env:KVSTORE_SHARDED_GRPC_PORT
$previousJoinToken = $env:KVSTORE_JOIN_TOKEN
$previousRouterToken = $env:KVSTORE_ROUTER_ADMIN_TOKEN
Push-Location $root
try {
    if (-not (Test-Path -LiteralPath $envFile)) {
        $joinToken = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
        $routerToken = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
        @("KVSTORE_JOIN_TOKEN=$joinToken", "KVSTORE_ROUTER_ADMIN_TOKEN=$routerToken") |
            Set-Content -LiteralPath $envFile -Encoding ascii
    }
    if (-not $SkipBuild) {
        docker build -t $image .
        if ($LASTEXITCODE -ne 0) { throw 'KVStore image build failed' }
    }
    $projectTokens = Get-Content -LiteralPath $envFile -Raw | ConvertFrom-StringData
    $env:KVSTORE_JOIN_TOKEN = $projectTokens.KVSTORE_JOIN_TOKEN
    $env:KVSTORE_ROUTER_ADMIN_TOKEN = $projectTokens.KVSTORE_ROUTER_ADMIN_TOKEN
    $env:KVSTORE_IMAGE = $image
    $env:KVSTORE_SHARDED_GRPC_PORT = "$Port"
    docker compose --env-file $envFile -p $Project -f compose.sharded.yml -f compose.import-rehearsal.yml up -d --no-build --pull never --wait --wait-timeout 180
    if ($LASTEXITCODE -ne 0) { throw 'Rehearsal cluster failed to become ready' }
    Write-Output "Rehearsal router ready at 127.0.0.1:$Port (project $Project)"
}
finally {
    $env:KVSTORE_IMAGE = $previousImage
    $env:KVSTORE_SHARDED_GRPC_PORT = $previousPort
    $env:KVSTORE_JOIN_TOKEN = $previousJoinToken
    $env:KVSTORE_ROUTER_ADMIN_TOKEN = $previousRouterToken
    Pop-Location
}
