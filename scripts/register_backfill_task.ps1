# Sprint 6 - Register Windows Scheduled Task for daily MOEX backfill.
#
# Creates a Task Scheduler job that runs backfill_prices.py daily at 23:55 MSK
# (after MOEX close 23:50 MSK).  Idempotent — re-running overwrites the task.
#
# Run as Administrator if you want the task to start when computer wakes.
# Without admin: works only when user is logged in.
#
# Uninstall:
#     Unregister-ScheduledTask -TaskName "newsbot3-backfill-prices" -Confirm:$false

$projectRoot = "D:\quik_sber\newsbot\newsbot3"
$venvPython  = Join-Path $projectRoot ".venv\Scripts\python.exe"
$script      = Join-Path $projectRoot "scripts\backfill_prices.py"
$taskName    = "newsbot3-backfill-prices"

if (-not (Test-Path $venvPython)) {
    Write-Error "venv python not found at: $venvPython"
    Write-Error "Create venv first:  py -m venv .venv  ;  .venv\Scripts\activate  ;  pip install requests"
    exit 1
}
if (-not (Test-Path $script)) {
    Write-Error "script not found at: $script"
    exit 1
}

# Drop existing if present
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "[backfill] removing existing task $taskName"
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

$action = New-ScheduledTaskAction `
    -Execute $venvPython `
    -Argument "`"$script`"" `
    -WorkingDirectory $projectRoot

# Daily at 23:55 local time (assumed Moscow on this PC)
$trigger = New-ScheduledTaskTrigger -Daily -At "23:55"

# Also trigger 5 minutes after Windows boot (catch missed runs)
$startupTrigger = New-ScheduledTaskTrigger -AtStartup
$startupTrigger.Delay = "PT5M"

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopOnIdleEnd `
    -RestartOnFailure `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 10) `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger @($trigger, $startupTrigger) `
    -Settings $settings `
    -User $env:USERNAME `
    -Description "newsbot3: daily MOEX ISS backfill for D:\quik_sber\newsbot\prices CSV files"

Write-Host ""
Write-Host "[backfill] task registered:  $taskName"
Write-Host "  schedule:    daily 23:55 + 5min after boot"
Write-Host "  script:      $script"
Write-Host "  log file:    $projectRoot\logs\backfill_prices.log"
Write-Host ""
Write-Host "To run NOW for testing:"
Write-Host "  Start-ScheduledTask -TaskName $taskName"
Write-Host ""
Write-Host "Inspect last run:"
Write-Host "  Get-ScheduledTaskInfo -TaskName $taskName"
Write-Host ""
Write-Host "Unregister:"
Write-Host "  Unregister-ScheduledTask -TaskName $taskName -Confirm:`$false"
