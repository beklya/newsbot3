@echo off
REM SSH tunnel watchdog launcher.
REM Bypasses ExecutionPolicy and starts hidden — for Task Scheduler at logon.
setlocal
set SCRIPT_DIR=%~dp0
powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -NoProfile -File "%SCRIPT_DIR%ssh_tunnel_watchdog.ps1"
endlocal
