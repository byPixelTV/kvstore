param(
    [switch]$CopyKey,
    [switch]$CheckOnly
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$demo = Join-Path (Join-Path $repo 'data') ('kvctl-demo-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $demo -Force | Out-Null
$server = Join-Path $demo 'kvstore.exe'
$console = Join-Path $demo 'kvctl.exe'
$seeder = Join-Path $demo 'seed.exe'
$registry = Join-Path $demo 'keys'
$serverLog = Join-Path $demo 'server.log'
$process = $null

Push-Location -LiteralPath $repo
try {
    go build -buildvcs=false -o $server ./cmd/kvstore
    if ($LASTEXITCODE -ne 0) { throw 'Could not build the KVStore server.' }
    go build -buildvcs=false -o $console ./cmd/kvctl
    if ($LASTEXITCODE -ne 0) { throw 'Could not build the terminal console.' }
    go build -buildvcs=false -o $seeder ./scripts/fixtures/kvctl-demo
    if ($LASTEXITCODE -ne 0) { throw 'Could not build the demo seeder.' }

    $keyOutput = @(& $server apikey create --dir $registry --name demo-admin --read --write --manage)
    if ($LASTEXITCODE -ne 0 -or $keyOutput.Count -ne 1 -or $keyOutput[0] -cnotmatch '^kvs_[A-Za-z0-9_-]{43}$') {
        throw 'Could not create the demo access key.'
    }
    $accessKey = [string]$keyOutput[0]

    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try {
        $address = '127.0.0.1:' + $listener.LocalEndpoint.Port
    } finally {
        $listener.Stop()
    }

    $settings = @{
        KVSTORE_MODE = 'standalone'
        KVSTORE_NODE_ID = 'kvctl-demo'
        KVSTORE_EXPECTED_NODE_ID = $null
        KVSTORE_LISTEN_ADDR = $address
        KVSTORE_HEALTH_ADDR = $address
        KVSTORE_HEALTH_LISTEN_ADDR = $null
        KVSTORE_DATA_DIR = (Join-Path $demo 'db')
        KVSTORE_API_KEYS_DIR = $registry
        KVSTORE_TLS_CERT_FILE = $null
        KVSTORE_TLS_KEY_FILE = $null
        KVSTORE_TLS_CLIENT_CA_FILE = $null
        KVSTORE_ACCESS_POLICY_FILE = $null
        KVSTORE_METRICS_TOKEN = $null
    }
    $previousSettings = @{}
    try {
        # Inherit process-local settings in Windows PowerShell 5.1 as well as PowerShell 7.
        foreach ($name in $settings.Keys) {
            $previousSettings[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
            [Environment]::SetEnvironmentVariable($name, $settings[$name], 'Process')
        }
        $process = Start-Process -FilePath $server -WorkingDirectory $demo `
            -RedirectStandardOutput (Join-Path $demo 'stdout.log') `
            -RedirectStandardError $serverLog -WindowStyle Hidden -PassThru
        $ready = $false
        for ($attempt = 0; $attempt -lt 12; $attempt++) {
            if ($process.HasExited) { break }
            # A failed readiness probe can write stderr before the listener starts.
            $ErrorActionPreference = 'Continue'
            try {
                & $server healthcheck readiness 2>$null
            } finally {
                $ErrorActionPreference = 'Stop'
            }
            if ($LASTEXITCODE -eq 0) { $ready = $true; break }
            Start-Sleep -Milliseconds 150
        }
        if (-not $ready) { throw "Demo server startup failed. See $serverLog." }
    } finally {
        foreach ($name in $previousSettings.Keys) {
            [Environment]::SetEnvironmentVariable($name, $previousSettings[$name], 'Process')
        }
    }

    # Give the key only to the seeder process. This avoids native pipeline transcoding.
    $seedInfo = [Diagnostics.ProcessStartInfo]::new()
    $seedInfo.FileName = $seeder
    $seedInfo.Arguments = $address
    $seedInfo.UseShellExecute = $false
    $seedInfo.CreateNoWindow = $true
    $seedInfo.RedirectStandardOutput = $true
    $seedInfo.RedirectStandardError = $true
    $seedInfo.EnvironmentVariables['KVSTORE_DEMO_ACCESS_KEY'] = $accessKey
    $seedProcess = [Diagnostics.Process]::Start($seedInfo)
    try {
        $seedOutput = $seedProcess.StandardOutput.ReadToEndAsync()
        $seedError = $seedProcess.StandardError.ReadToEndAsync()
        $seedProcess.WaitForExit()
        if ($seedProcess.ExitCode -ne 0) { throw "Could not seed the demo data. $($seedError.Result.Trim())" }
        Write-Output $seedOutput.Result.Trim()
    } finally {
        $seedProcess.Dispose()
    }
    if ($CheckOnly) {
        & $console --version
        if ($LASTEXITCODE -ne 0) { throw 'Could not run kvctl.' }
        Write-Output 'Demo startup and access-key login verified.'
        return
    }
    if ($CopyKey) {
        $accessKey | Set-Clipboard
        Write-Host 'Demo key copied. Paste it into Access key, then press Enter.'
    } else {
        Write-Host $accessKey
        [void](Read-Host 'Copy the demo access key above, then press Enter to launch kvctl')
    }
    & $console --addr $address
    if ($LASTEXITCODE -ne 0) { throw "kvctl exited with code $LASTEXITCODE." }
} finally {
    if ($null -ne $process -and -not $process.HasExited) { Stop-Process -Id $process.Id }
    Pop-Location
    Write-Host "Demo files: $demo"
}
