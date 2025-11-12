param(
    [int]$InstanceId       = 0,
    [string]$DataRoot      = "D:\data",
    [string]$LogRoot       = "D:\data\logs",
    [switch]$SkipStopProc
)

$ErrorActionPreference = "Stop"

$TierRoot = Join-Path $DataRoot ("aegisdb-{0}" -f $InstanceId)
$LogDir   = Join-Path $LogRoot ("aegisdb-{0}" -f $InstanceId)

function Stop-ProcessesUsingPath {
    param([string[]]$Paths)

    foreach ($path in $Paths) {
        $pattern = [regex]::Escape($path)
        $matches = Get-CimInstance Win32_Process |
            Where-Object {
                ($_.CommandLine -and $_.CommandLine -match $pattern) -or
                ($_.ExecutablePath -and $_.ExecutablePath -match $pattern)
            }
        foreach ($proc in $matches) {
            try {
                Write-Host "==> stop PID $($proc.ProcessId) ($($proc.Name))" -ForegroundColor DarkYellow
                Stop-Process -Id $proc.ProcessId -Force
            } catch {
                Write-Warning "failed to stop PID $($proc.ProcessId): $_"
            }
        }
    }
}

Stop-ProcessesUsingPath -Paths @($TierRoot, $Workspace)
Stop-ServiceProcesses -Names @("aegisdb","ingest_http","start-10","cargo","powershell")



function Stop-ServiceProcesses {
    param([string[]]$Names)

    if ($SkipStopProc) {
        Write-Warning "skip process termination as requested (-SkipStopProc)"
        return
    }

    Write-Host "==> stop running processes..." -ForegroundColor Cyan
    foreach ($name in $Names) {
        $procs = Get-Process -Name $name -ErrorAction SilentlyContinue
        if ($procs) {
            $procs | Stop-Process -Force
            foreach ($proc in $procs) {
                try { $proc.WaitForExit(5000) } catch {}
            }
        }
    }
}

function Remove-LockedFiles {
    param(
        [Parameter(Mandatory)] [string[]]$Targets,
        [string]$DisplayRoot
    )

    foreach ($target in $Targets) {
        if (-not (Test-Path $target)) { continue }

        try {
            Remove-Item $target -Recurse -Force
        }
        catch {
            Write-Warning "locked: $target"
            if ($DisplayRoot) {
                $escaped = $DisplayRoot.Replace('\','\\')
                $holders = Get-CimInstance Win32_Process |
                    Where-Object { $_.CommandLine -match [regex]::Escape($DisplayRoot) }
                if ($holders) {
                    Write-Warning "possible holders:"
                    $holders | Select-Object ProcessId, Name, CommandLine |
                        Format-Table -AutoSize | Out-String | Write-Warning
                } else {
                    Write-Warning "run Sysinternals handle.exe on $DisplayRoot to locate the handle."
                }
            }
        }
    }
}

Stop-ServiceProcesses -Names @("aegisdb", "ingest_http", "start-10")

Write-Host "==> remove data directory: $TierRoot" -ForegroundColor Cyan
Remove-LockedFiles -Targets @($TierRoot) -DisplayRoot $TierRoot

Write-Host "==> clean logs under $LogDir" -ForegroundColor Cyan
if (Test-Path $LogDir) {
    $logFiles = @("*.out.log","*.err.log") | ForEach-Object {
        Join-Path $LogDir $_
    }
    Remove-LockedFiles -Targets $logFiles -DisplayRoot $LogDir
}

Write-Host "==> recreate directories" -ForegroundColor Cyan
New-Item -Path $TierRoot -ItemType Directory -Force | Out-Null
New-Item -Path $LogDir -ItemType Directory -Force | Out-Null

Write-Host "Clean finished for instance $InstanceId." -ForegroundColor Green