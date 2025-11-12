param(
    [string]$Workspace   = "D:\desktop\tinykv-course",
    [string]$AdminAddr   = "0.0.0.0:8080",
    [string]$GrpcAddr    = "127.0.0.1:20160",
    [string]$TierRoot    = "D:\data\aegisdb-0",
    [string]$IngestAddr  = "127.0.0.1:8088"
)

$ErrorActionPreference = "Stop"

function Start-Background {
    param(
        [string]$Name,
        [string]$Command
    )
    Write-Host "==> launch $Name ..."
    Start-Process -WindowStyle Minimized `
        -FilePath "powershell.exe" `
        -ArgumentList @("-NoExit","-Command",$Command)
}

$repo = Join-Path $Workspace "aegisdb"
if (!(Test-Path $repo)) {
    throw "directory not found: $repo"
}

# 1) spawn 10 tinykv nodes
Start-Background -Name "start-10 stores" -Command @"
& {
    Set-Location '$repo'
    powershell -ExecutionPolicy Bypass -File '.\start-10.exe.ps1'
}
"@

Start-Sleep -Seconds 3

# 2) main aegisdb server
Start-Background -Name "aegisdb run" -Command @"
& {
    Set-Location '$repo'
    cargo run --release --bin aegisdb -- run --addr '$GrpcAddr' --db-path '$TierRoot' --admin-addr '$AdminAddr'
}
"@

Start-Sleep -Seconds 3

# 3) ingest_http helper
Start-Background -Name "ingest_http" -Command @"
& {
    Set-Location '$repo'
    cargo run --release --bin ingest_http -- --listen-addr '$IngestAddr' --kv-endpoint 'http://$GrpcAddr' --db-path '$TierRoot'
}
"@

Write-Host "All background processes started."
Write-Host "Dashboard dev: cd ..\aegisdb-panel ; pnpm dev"
Write-Host "Trigger ingest: curl -X POST http://$IngestAddr/api/ingest/start"