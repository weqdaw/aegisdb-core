# load-hot-warm.ps1
param(
    [string]$HttpEndpoint = "http://127.0.0.1:8088",
    [string]$GrpcEndpoint = "127.0.0.1:20160",
    [int]$RegionId = 1,
    [int]$TxnCount = 120,
    [string]$Prefix = "desk:pb:",
    [string]$GrpcurlPath = "D:\grpcurl_1.9.2_windows_x86_64\grpcurl.exe",
    [switch]$EnableDemoHotWarm,
    [switch]$EnableColdWarm = $true,
    [int]$ColdWarmSampleCount = 5000,
    [int]$HotInitialTouches = 15,
    [int]$WarmInitialTouches = 8,
    [int]$HotLoopTouches = 12,
    [int]$WarmLoopTouches = 6,
    [int]$ColdTouchTimes = 15
)

function Invoke-TierSnapshot {
    $uri = "$HttpEndpoint/api/storage/tiers"
    try {
        Invoke-RestMethod -Method Get -Uri $uri -ErrorAction Stop
    } catch {
        throw "Failed to fetch tier snapshot: $($_.Exception.Message)"
    }
}

function Get-ColdTierSample {
    param(
        [int]$SampleCount,
        [int]$TouchTimes
    )

    Write-Host "[Info] Fetching cold tier snapshot..."
    $snapshot = Invoke-TierSnapshot
    $coldKeys = @($snapshot.cold | ForEach-Object { $_.key })

    if ($coldKeys.Count -eq 0) {
        throw "Cold tier is empty; nothing to sample."
    }

    Write-Host ("[Info] Cold tier has {0} entries; sampling {1} entries with replacement..." -f $coldKeys.Count, $SampleCount)

    $rng = [System.Random]::new()
    $result = New-Object System.Collections.Generic.List[string]
    for ($i = 0; $i -lt $SampleCount; $i++) {
        $idx = $rng.Next(0, $coldKeys.Count)
        $result.Add($coldKeys[$idx])
    }

    # 记录出现次数便于观察
    $topCounts = $result | Group-Object | Sort-Object Count -Descending | Select-Object -First 10
    Write-Host "[Info] Top 10 sampled key counts:"
    foreach ($entry in $topCounts) {
        Write-Host ("  {0} x {1}" -f $entry.Name, $entry.Count)
    }

    Touch-ColdSample -SampledKeys $result.ToArray() -TouchTimes $TouchTimes
}

function Touch-ColdSample {
    param(
        [string[]]$SampledKeys,
        [int]$TouchTimes
    )

    Write-Host ("[Info] Warming cold samples: {0} keys, {1} touches each..." -f $SampledKeys.Count, $TouchTimes)
    $total = $SampledKeys.Count
    $batchSize = 100
    for ($offset = 0; $offset -lt $total; $offset += $batchSize) {
        $limit = [Math]::Min($batchSize, $total - $offset)
        $batch = $SampledKeys[$offset..($offset + $limit - 1)]
        foreach ($key in $batch) {
            Touch-Key -Key $key -Times $TouchTimes
        }
        Write-Host ("  -> processed {0}/{1}" -f [Math]::Min($offset + $limit, $total), $total)
        Start-Sleep -Milliseconds 200
    }
    Write-Host "[Info] Cold tier warming complete."
}

function Start-IngestTxn {
    param([int]$Count, [string]$Prefix)

    $body = @{
        count     = $Count
        prefix    = $Prefix
        kind      = "txn"
        get_ratio = 0.0
        del_ratio = 0.0
    } | ConvertTo-Json -Compress

    Write-Host ("[Info] Ingesting {0} txn records with prefix {1}" -f $Count, $Prefix)
    Invoke-RestMethod -Method Post -Uri "$HttpEndpoint/api/ingest/start" `
        -ContentType "application/json" -Body $body | Out-Null

    Start-Sleep -Seconds 8
}

function Touch-Key {
    param([string]$Key, [int]$Times)

    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Key))
    $payload = "{""context"":{""region_id"":$RegionId},""key"":""$encoded"",""cf"":""default""}"
    $grpcurl = $GrpcurlPath

    for ($i = 0; $i -lt $Times; $i++) {
        & $grpcurl -plaintext -d $payload $GrpcEndpoint tinykvpb.TinyKv/RawGet 1>$null 2>$null
    }
}

Write-Host "=== TinyKV cold-to-warm warming script ==="
Write-Host "[Config] HTTP endpoint: $HttpEndpoint"
Write-Host "[Config] gRPC endpoint: $GrpcEndpoint"
Write-Host ""

if ($EnableDemoHotWarm) {
    Start-IngestTxn -Count $TxnCount -Prefix $Prefix

    $allTxnKeys = 0..($TxnCount - 1) | ForEach-Object {
        "{0}txn:T{1:D8}" -f $Prefix, $_
    }

    $hotKeys  = $allTxnKeys[0..19]
    $warmKeys = $allTxnKeys[20..59]

    Write-Host "▶ 初次升温 (热 $HotInitialTouches 次, 温 $WarmInitialTouches 次)"
    foreach ($k in $hotKeys)  { Touch-Key -Key $k -Times $HotInitialTouches }
    foreach ($k in $warmKeys) { Touch-Key -Key $k -Times $WarmInitialTouches }

    Write-Host ""
    Write-Host "开始保持热/温层 (Ctrl+C 退出)"
    while ($true) { # 默认关闭循环，按需改成 $true
        foreach ($k in $hotKeys)  { Touch-Key -Key $k -Times $HotLoopTouches }
        foreach ($k in $warmKeys) { Touch-Key -Key $k -Times $WarmLoopTouches }
        Start-Sleep -Seconds 8
    }
}

if ($EnableColdWarm) {
    Get-ColdTierSample -SampleCount $ColdWarmSampleCount -TouchTimes $ColdTouchTimes
} else {
    Write-Host "[Info] Skipping cold tier warming (-EnableColdWarm:$false)."
}