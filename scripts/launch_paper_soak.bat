@echo off
REM Sprint 5.5 + 5.8 — Paper trading soak launcher (Windows .bat wrapper).
REM
REM Этот wrapper обходит Windows ExecutionPolicy block для .ps1 скриптов.
REM Никаких изменений системных настроек не нужно.
REM
REM Usage (двойной клик ИЛИ из cmd/PowerShell):
REM   .\scripts\launch_paper_soak.bat
REM
REM Эквивалентно: PowerShell -ExecutionPolicy Bypass -File .\scripts\launch_paper_soak.ps1

setlocal
set SCRIPT_DIR=%~dp0
PowerShell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%launch_paper_soak.ps1" %*
endlocal
