# Sprint 5.11/Phase 1.5 - Launch the 5 LOCAL services that talk to VPS Redis
# via SSH tunnel (127.0.0.1:6380 -> VPS_HOST:6379).
#
# Receiver and Enricher already run on the VPS as systemd services
# (newsbot-receiver, newsbot-enricher) - do NOT start them here.
#
# Services started (in order):
#   1. quik_feed  - reads QUIK candles.csv, publishes candles:1m
#   2. predictor  - news:enriched -> ml:predictions (needs XGBoost models)
#   3. decision   - ml:predictions + candles -> trade:signals
#   4. bridge     - trade:signals -> paper executions
#   5. monitor    - watches heartbeats, emits alerts
#
# Usage from project root:
#   .\scripts\launch_local_services.ps1
#
# Pre-flight (auto-checked):
#   - SSH tunnel to VPS Redis must be alive (script will warn if not)
#   - data\models\predictor\v1\feature_order.json must exist
#   - D:\quik_sber\newsbot\prices\ must exist (prices_*.csv files)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$python = "$projectRoot\.venv\Scripts\python.exe"

# --- Pre-flight checks ---

if (-not (Test-Path $python)) {
    Write-Error "Python venv not found at $python -- create venv: py -m venv .venv"
    exit 1
}

$modelsDir = "$projectRoot\data\models\predictor\v1"
if (-not (Test-Path "$modelsDir\feature_order.json")) {
    Write-Error "Predictor models missing in $modelsDir`nRun first: python scripts\train_predictor_fold13.py"
    exit 1
}

$pricesDir = "D:\quik_sber\newsbot\prices"
if (-not (Test-Path "$pricesDir\prices_SBER.csv")) {
    Write-Error "Historical prices missing in $pricesDir`nNeed 19 prices_*.csv files from Phase 2."
    exit 1
}

# Check SSH tunnel to VPS Redis
$tunnelOk = $false
try {
    $tcp = New-Object Net.Sockets.TcpClient
    $iar = $tcp.BeginConnect('127.0.0.1', 6380, $null, $null)
    if ($iar.AsyncWaitHandle.WaitOne(1000, $false)) {
        $tcp.EndConnect($iar)
        $tunnelOk = $tcp.Connected
    }
    $tcp.Close()
} catch { $tunnelOk = $false }

if (-not $tunnelOk) {
    Write-Host "[!] WARNING: SSH tunnel to VPS Redis (127.0.0.1:6380) is not responding." -ForegroundColor Yellow
    Write-Host "    Services will fail to connect to Redis." -ForegroundColor Yellow
    Write-Host "    Open a new PowerShell first (profile auto-starts tunnel)," -ForegroundColor Yellow
    Write-Host "    or run manually:" -ForegroundColor Yellow
    Write-Host '      Start-Process ssh -ArgumentList "-N","-L","127.0.0.1:6380:127.0.0.1:6379","USER@VPS_HOST" -WindowStyle Hidden' -ForegroundColor Cyan
    Write-Host ""
    $proceed = Read-Host "Continue anyway? (y/n)"
    if ($proceed -ne "y") { exit 1 }
}

# --- Service launcher ---

# Check for already-running instances and refuse to start duplicates.
# Two consumers in the same group split work, but duplicate Bridges cause
# 2x paper-positions, and duplicate Predictors do duplicate Groq work.
$existing = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -like "*src.services.*" }
if ($existing) {
    Write-Host "[!] Found already-running newsbot3 service processes:" -ForegroundColor Yellow
    foreach ($p in $existing) {
        if ($p.CommandLine -match "src\.services\.(\w+)") {
            Write-Host ("    [{0}]  PID {1}" -f $matches[1], $p.ProcessId) -ForegroundColor Yellow
        }
    }
    Write-Host ""
    $answer = Read-Host "Stop them first and relaunch? (y/n)"
    if ($answer -ne "y") {
        Write-Host "Aborted. To stop manually:  nb-stop" -ForegroundColor Yellow
        exit 1
    }
    foreach ($p in $existing) { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 2
    Write-Host "Old instances stopped." -ForegroundColor Green
    Write-Host ""
}

# Detect Windows Terminal
$wtAvailable = $null -ne (Get-Command wt.exe -ErrorAction SilentlyContinue)

function Start-ServiceWindow([string]$name, [string]$module) {
    Write-Host "Starting $name..." -ForegroundColor Green

    # Each tab/window: set UTF-8 + REDIS_URL pointing to tunneled VPS Redis,
    # then run the python module. PROXY_ENABLED=false (local services only
    # talk to Redis).
    $pyCmd = "chcp 65001 | Out-Null; `$env:PYTHONIOENCODING='utf-8'; `$env:REDIS_URL='redis://127.0.0.1:6380'; `$env:PROXY_ENABLED='false'; & '$python' -m $module"

    if ($wtAvailable) {
        # Base64-encode (UTF-16LE) so wt.exe semicolon parser doesn't split args
        $bytes = [System.Text.Encoding]::Unicode.GetBytes($pyCmd)
        $encoded = [System.Convert]::ToBase64String($bytes)

        # Use a NAMED window ("newsbot3") so all 5 tabs land in the same WT
        # window deterministically — using "-w 0" (most recent) had a race
        # condition where two windows could be created with duplicate tabs.
        $wtArgs = "-w newsbot3 new-tab --title `"$name`" -d `"$projectRoot`" powershell -NoExit -EncodedCommand $encoded"
        Start-Process -FilePath "wt.exe" -ArgumentList $wtArgs
    } else {
        # Fallback to separate PowerShell windows
        Start-Process powershell -ArgumentList @("-NoExit", "-Command", $pyCmd) `
            -WindowStyle Normal `
            -WorkingDirectory $projectRoot
    }
}

Write-Host "=== Phase 1.5: Local services launcher ===" -ForegroundColor Cyan
Write-Host "Project: $projectRoot"
Write-Host "Redis:   redis://127.0.0.1:6380 (SSH tunnel -> VPS)"
if ($wtAvailable) {
    Write-Host "Mode:    Windows Terminal tabs" -ForegroundColor Yellow
} else {
    Write-Host "Mode:    separate PowerShell windows (wt.exe not found)" -ForegroundColor Yellow
}
Write-Host ""

# Order matters: quik_feed first (publishes candles); then ML pipeline;
# bridge needs signals so wait for decision; monitor last (read-only).
Start-ServiceWindow "quik_feed" "src.services.quik_feed"
Start-Sleep -Seconds 4

Start-ServiceWindow "predictor" "src.services.predictor"
Start-Sleep -Seconds 4

Start-ServiceWindow "decision"  "src.services.decision"
Start-Sleep -Seconds 4

Start-ServiceWindow "bridge"    "src.services.bridge"
Start-Sleep -Seconds 3

Start-ServiceWindow "monitor"   "src.services.monitor"

Write-Host ""
Write-Host "All 5 local services launched." -ForegroundColor Green
Write-Host ""
Write-Host "Observability (any PowerShell with newsbot3 profile loaded):"
Write-Host "  nb-summary           - stream lengths overview"
Write-Host "  nb-watch             - live tail of news:enriched"
Write-Host "  nb-enriched 10       - last 10 enriched with tickers parsed"
Write-Host "  nb-tg                - Telegram MTProto health"
Write-Host "  nb-log-receiver      - VPS receiver journal (live)"
Write-Host "  nb-log-enricher      - VPS enricher journal (live)"
Write-Host ""
Write-Host "Stop services: Ctrl+C in each tab, or close the tab."
