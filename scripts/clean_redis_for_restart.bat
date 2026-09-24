@echo off
REM Reset transient Redis state before fresh paper-soak restart.
REM
REM Usage:
REM   .\scripts\clean_redis_for_restart.bat            (heartbeats + PEL)
REM   .\scripts\clean_redis_for_restart.bat --full     (also clear paper risk state)
REM   .\scripts\clean_redis_for_restart.bat --dry-run  (preview only)

setlocal
set SCRIPT_DIR=%~dp0
set PYTHON=%SCRIPT_DIR%..\.venv\Scripts\python.exe
"%PYTHON%" "%SCRIPT_DIR%clean_redis_for_restart.py" %*
endlocal
