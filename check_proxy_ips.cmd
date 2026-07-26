@echo off
setlocal EnableDelayedExpansion

REM Fetch public IP for each running proxy-pool slot (one check per Xray process).
REM Usage:
REM   check_proxy_ips.cmd
REM   check_proxy_ips.cmd 10801 10999

set "IP_URL=https://api.ipify.org"
set "CONNECT_TIMEOUT=3"
set "MAX_TIME=12"
set "PORT_MIN=10801"
set "PORT_MAX=10999"
set "TMP=%TEMP%\proxy_ports_%RANDOM%.txt"
set "PIDS_FILE=%TEMP%\proxy_pids_%RANDOM%.txt"
set "OUT=%TEMP%\proxy_ip_%RANDOM%.txt"

if not "%~1"=="" set "PORT_MIN=%~1"
if not "%~2"=="" set "PORT_MAX=%~2"

where curl.exe >nul 2>&1
if errorlevel 1 (
  echo curl.exe not found on PATH.
  exit /b 1
)

echo Host public IP:
curl.exe -s --connect-timeout %CONNECT_TIMEOUT% --max-time %MAX_TIME% "%IP_URL%"
echo.
echo.
echo Scanning proxy slots on 127.0.0.1 ports %PORT_MIN%-%PORT_MAX% ...
echo.

type nul > "%TMP%"
type nul > "%PIDS_FILE%"

for /f "tokens=2,5" %%A in ('netstat -ano ^| findstr /C:"127.0.0.1:" ^| findstr "LISTENING"') do (
  set "ADDR=%%A"
  set "PID=%%B"
  for /f "tokens=2 delims=:" %%P in ("!ADDR!") do (
    set "PORT=%%P"
    set /a "N=!PORT!" 2>nul
    if defined N (
      if !N! GEQ %PORT_MIN% if !N! LEQ %PORT_MAX% (
        >>"%TMP%" echo !PID! !N!
        findstr /X /C:"!PID!" "%PIDS_FILE%" >nul 2>&1
        if errorlevel 1 >>"%PIDS_FILE%" echo !PID!
      )
    )
  )
)

set "FOUND=0"
for /f "usebackq delims=" %%P in ("%PIDS_FILE%") do (
  call :check_pid %%P
)

del "%TMP%" >nul 2>&1
del "%PIDS_FILE%" >nul 2>&1
del "%OUT%" >nul 2>&1

if "!FOUND!"=="0" (
  echo No responding proxies found.
  echo Make sure the Proxy pool is running.
  exit /b 2
)

echo.
echo Done. !FOUND! proxy slot^(s^) checked.
exit /b 0

:check_pid
set "PID=%~1"
set "P1="
set "P2="
for /f "tokens=1,2" %%A in ('findstr /B /C:"%PID% " "%TMP%"') do (
  if not defined P1 (
    set "P1=%%B"
  ) else if not defined P2 (
    if not "%%B"=="!P1!" set "P2=%%B"
  )
)

if not defined P1 goto :eof

set "SOCKS="
set "HTTP="
if defined P2 (
  if !P1! LSS !P2! (
    set "SOCKS=!P1!"
    set "HTTP=!P2!"
  ) else (
    set "SOCKS=!P2!"
    set "HTTP=!P1!"
  )
) else (
  set "HTTP=!P1!"
)

set "IP="
set "OK=0"

if defined HTTP (
  curl.exe -s --connect-timeout %CONNECT_TIMEOUT% --max-time %MAX_TIME% -x "http://127.0.0.1:!HTTP!" "%IP_URL%" > "%OUT%" 2>nul
  set "IP="
  for /f "usebackq delims=" %%I in ("%OUT%") do set "IP=%%I"
  call :is_ipv4 "!IP!"
  if "!IS_IP!"=="1" set "OK=1"
)

if "!OK!"=="0" if defined SOCKS (
  curl.exe -s --connect-timeout %CONNECT_TIMEOUT% --max-time %MAX_TIME% --socks5-hostname "127.0.0.1:!SOCKS!" "%IP_URL%" > "%OUT%" 2>nul
  set "IP="
  for /f "usebackq delims=" %%I in ("%OUT%") do set "IP=%%I"
  call :is_ipv4 "!IP!"
  if "!IS_IP!"=="1" set "OK=1"
)

if "!OK!"=="0" (
  echo [FAIL] pid !PID! ports !P1! !P2!
  goto :eof
)

set /a FOUND+=1
if defined SOCKS if defined HTTP (
  echo [OK] SOCKS5 :!SOCKS! / HTTP :!HTTP!  -^>  !IP!
) else (
  echo [OK] HTTP :!HTTP!  -^>  !IP!
)
goto :eof

:is_ipv4
set "IS_IP=0"
set "CAND=%~1"
if "!CAND!"=="" goto :eof
echo !CAND!| findstr /R "^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*$" >nul
if not errorlevel 1 set "IS_IP=1"
goto :eof
