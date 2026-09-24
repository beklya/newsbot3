@echo off
REM Wrapper for launch_local_services.ps1 that bypasses ExecutionPolicy
REM if it's still set Restricted somewhere upstream.
setlocal
set SCRIPT_DIR=%~dp0
powershell.exe -ExecutionPolicy Bypass -NoProfile -File "%SCRIPT_DIR%launch_local_services.ps1"
endlocal
