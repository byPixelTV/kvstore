# Run from the repository root. protoc, protoc-gen-go and protoc-gen-go-grpc
# must already be installed and available in PATH.
protoc -I=api/proto `
  --go_out=. `
  --go_opt=module=github.com/byPixelTV/kvstore `
  --go-grpc_out=. `
  --go-grpc_opt=module=github.com/byPixelTV/kvstore `
  api/proto/kvstore/v1/kvstore.proto
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Generated gen/go/kvstore/v1/kvstore*.pb.go"
