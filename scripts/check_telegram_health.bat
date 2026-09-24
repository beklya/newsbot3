@echo off
REM Telegram health check (cron / Task Scheduler friendly).
REM
REM Usage from project root:
REM   .\scripts\check_telegram_health.bat
REM   .\scripts\check_telegram_health.bat --json
REM   .\scripts\check_telegram_health.bat --quiet
REM
REM Exit codes:
REM   0 = healthy
REM   1 = warning (recent reconnects)
REM   2 = storm   (active Telegram MTProto outage)
REM   3 = no data (receiver missing or stale)

setlocal
set SCRIPT_DIR=%~dp0
set PYTHON=%SCRIPT_DIR%..\.venv\Scripts\python.exe
"%PYTHON%" "%SCRIPT_DIR%check_telegram_health.py" %*
endlocal
