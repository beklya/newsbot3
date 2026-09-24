# Sprint 5.5 + 5.8 -- End-to-end paper trading soak launcher.
#
# Spawns 7 services as tabs in a single Windows Terminal window (if wt.exe is
# available), or as separate PowerShell windows (fallback for legacy systems).
# Each tab can be Ctrl+C'd independently to stop a single service.
#
# Order matters: each service gets ~5s to start before the next, so that
# news:enriched and candles:1m streams are alive before Predictor subscribes.
#
# Usage from project root:
#   .\scripts\launch_paper_soak.ps1
#
# WINDOWS EXECUTIONPOLICY -- if "execution disabled" error:
#   1) Use .bat wrapper:           .\scripts\launch_paper_soak.bat
#   2) Per-session bypass:         PowerShell -ExecutionPolicy Bypass -File .\scripts\launch_paper_soak.ps1
#   3) One-time per-user:          Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
#
# Pre-flight checklist:
#   1. Memurai (localhost:6379) running. Check: redis-cli ping
#   2. .env: GROQ_MODEL=llama-3.3-70b-versatile, RISK_PER_TRADE_PCT=0.005,
#            QUIK_FEED_SOURCE_PATH set
#   3. data\models\predictor\v1\*.joblib (16 files + feature_order.json)
#   4. D:\quik_sber\newsbot\prices\prices_*.csv (19 files) for historical CSV
#   5. QUIK Workstation running with candle_dump.lua loaded (Sprint 5.8)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$python = "$projectRoot\.venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    Write-Error "Python not found at $python -- create venv: py -m venv .venv"
    exit 1
}

# Sanity check models
$modelsDir = "$projectRoot\data\models\predictor\v1"
if (-not (Test-Path "$modelsDir\feature_order.json")) {
    Write-Error "Models missing in $modelsDir -- run: python scripts\train_predictor_fold13.py"
    exit 1
}

# Detect Windows Terminal availability
$wtAvailable = $null -ne (Get-Command wt.exe -ErrorAction SilentlyContinue)
$script:firstTab = $true

function Start-ServiceWindow([string]$name, [string]$module) {
    Write-Host "Starting $name..." -ForegroundColor Green
    # Command run inside each tab/window: switch console to UTF-8 then run module.
    $pyCmd = "chcp 65001 | Out-Null; `$env:PYTHONIOENCODING='utf-8'; & '$python' -m $module"

    if ($wtAvailable) {
        # Use Windows Terminal -- spawn each service as a tab in a shared window.
        # First call creates the window; subsequent calls add tabs via "-w 0".
        #
        # IMPORTANT: wt.exe parses ";" as its OWN subcommand separator. Our PowerShell
        # command has internal semicolons (chcp; env; python). Passing -Command directly
        # would split the line into 3 broken wt subcommands. We base64-encode the entire
        # PowerShell command (UTF-16LE) and use powershell -EncodedCommand instead --
        # base64 alphabet has no wt.exe special chars, so nothing is split.
        $bytes = [System.Text.Encoding]::Unicode.GetBytes($pyCmd)
        $encoded = [System.Convert]::ToBase64String($bytes)

        if ($script:firstTab) {
            $wtArgs = "new-tab --title `"$name`" -d `"$projectRoot`" powershell -NoExit -EncodedCommand $encoded"
            $script:firstTab = $false
        } else {
            $wtArgs = "-w 0 new-tab --title `"$name`" -d `"$projectRoot`" powershell -NoExit -EncodedCommand $encoded"
        }
        Start-Process -FilePath "wt.exe" -ArgumentList $wtArgs
    } else {
        # Fallback -- separate PowerShell window per service.
        Start-Process powershell -ArgumentList @("-NoExit", "-Command", $pyCmd) `
            -WindowStyle Normal `
            -WorkingDirectory $projectRoot
    }
}

Write-Host "=== Sprint 5.5 + 5.8 paper soak launch ===" -ForegroundColor Cyan
Write-Host "Project: $projectRoot"
if ($wtAvailable) {
    Write-Host "Mode: Windows Terminal tabs (single window)" -ForegroundColor Yellow
} else {
    Write-Host "Mode: separate PowerShell windows (wt.exe not found)" -ForegroundColor Yellow
}
Write-Host ""

Start-ServiceWindow "quik_feed" "src.services.quik_feed"
Start-Sleep -Seconds 5

Start-ServiceWindow "receiver"  "src.services.receiver"
Start-Sleep -Seconds 5

Start-ServiceWindow "enricher"  "src.services.enricher"
Start-Sleep -Seconds 5

Start-ServiceWindow "predictor" "src.services.predictor"
Start-Sleep -Seconds 5

Start-ServiceWindow "decision"  "src.services.decision"
Start-Sleep -Seconds 5

Start-ServiceWindow "bridge"    "src.services.bridge"
Start-Sleep -Seconds 3

Start-ServiceWindow "monitor"   "src.services.monitor"

Write-Host ""
if ($wtAvailable) {
    Write-Host "All 7 services launched as tabs in Windows Terminal." -ForegroundColor Green
    Write-Host "To stop: Ctrl+C in each tab, or close the tab."
} else {
    Write-Host "All 7 services launched in separate windows." -ForegroundColor Green
    Write-Host "To stop: Ctrl+C in each window (or close it)."
}
Write-Host ""
Write-Host "Observability commands (run from project root):"
Write-Host "  python scripts\redis_inspect.py summary"
Write-Host "  python scripts\redis_inspect.py len candles:1m"
Write-Host "  python scripts\watch_enriched.py"
Write-Host "  python scripts\analyze_soak.py --hours 1"
