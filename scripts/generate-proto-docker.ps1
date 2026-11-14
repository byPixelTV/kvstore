# Alternative to generate-proto.ps1 when protoc is not installed on Windows.
# Uses the pinned Go/protoc toolchain from the project's Dockerfile.
$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path (Join-Path $PSScriptRoot ".."))
docker build --target builder -t kvstore:proto-builder .
if ($LASTEXITCODE -ne 0) { throw "Docker builder failed; fix compilation errors before generating" }
$id = (docker create kvstore:proto-builder).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($id)) { throw "Cannot create temporary Docker container" }
try {
    New-Item -ItemType Directory -Force -Path "gen/go/kvstore/v1" | Out-Null
    docker cp "${id}:/src/gen/go/kvstore/v1/." "gen/go/kvstore/v1/"
    if ($LASTEXITCODE -ne 0) { throw "Copy generated protobuf failed" }
    Write-Host "Generated Go protobuf bindings under gen/go/kvstore/v1"
} finally {
    docker rm $id | Out-Null
}
