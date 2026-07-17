@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"
git pull
echo.
pause
