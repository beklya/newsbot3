# Phase 1 sweep — runs 6 backtest variants on same 1766 events
# Outputs into data/replay/sweep_phase1/*

$env:PYTHONIOENCODING='utf-8'
$venv = "D:\quik_sber\newsbot\newsbot3\.venv\Scripts\python.exe"
$out = "D:\quik_sber\newsbot\newsbot3\data\replay\sweep_phase1"
New-Item -ItemType Directory -Force -Path $out | Out-Null

$variants = @(
    @{ name="v1_baseline";       args="" },
    @{ name="v1_70b_rolling";    args="--models-dir data/models/predictor/v1_70b_rolling" },
    @{ name="v3_expanding";      args="--models-dir data/models/predictor/v3" },
    @{ name="v5_no_llm";         args="--models-dir data/models/predictor/v5" },
    @{ name="v1_blacklist_gnv";  args="--blacklist GAZP,NG,VTBR" },
    @{ name="v1_70b_blacklist";  args="--models-dir data/models/predictor/v1_70b_rolling --blacklist GAZP,NG,VTBR" },
    @{ name="v1_minconf_030";    args="--direction-min-conf 0.3" },
    @{ name="v1_70b_minconf_030"; args="--models-dir data/models/predictor/v1_70b_rolling --direction-min-conf 0.3" }
)

foreach ($v in $variants) {
    Write-Host "==== running variant: $($v.name) ===="
    $reportPath = Join-Path $out "$($v.name)_report.json"
    $tradesPath = Join-Path $out "$($v.name)_trades.csv"
    $argsList = @("scripts/replay_vps_window_backtest.py", "--output", $reportPath, "--trades-out", $tradesPath)
    if ($v.args -ne "") {
        $argsList += $v.args -split " "
    }
    & $venv $argsList | Select-Object -Last 25
    Write-Host ""
}
