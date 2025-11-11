# D:\desktop\tinykv-course\aegisdb\start-10.exe.ps1

$exe = "D:\desktop\tinykv-course\aegisdb\target\debug\aegisdb.exe"
if (!(Test-Path $exe)) {
  Write-Error "未找到可执行文件：$exe，请先运行 cargo build --bin aegisdb"
  exit 1
}

0..9 | ForEach-Object {
  $idx = $_
  $grpc = 20160 + $idx
  $admin = 8080 + $idx
  $dir = "D:\data\aegisdb-$idx"
  New-Item -ItemType Directory -Force -Path $dir | Out-Null

  $args = @(
    "run",
    "--addr","127.0.0.1:$grpc",
    "--db-path",$dir,
    "--admin-addr","0.0.0.0:$admin",
    "--cluster-id","1"
  )

  $logOut = "D:\data\logs\aegisdb-$idx.out.log"
  $logErr = "D:\data\logs\aegisdb-$idx.err.log"
  New-Item -ItemType Directory -Force -Path (Split-Path $logOut) | Out-Null

  Start-Process -FilePath $exe `
    -ArgumentList $args `
    -WorkingDirectory "D:\desktop\tinykv-course\aegisdb" `
    -WindowStyle Hidden `
    -RedirectStandardOutput $logOut `
    -RedirectStandardError $logErr
}

Write-Host "已后台启动 10 个实例。日志在 D:\data\logs\aegisdb-*.out.log / *.err.log"