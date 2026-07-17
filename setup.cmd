@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

title fetch-mtproto setup
echo ========================================
echo  fetch-mtproto - Windows setup
echo ========================================
echo.
echo Project: %CD%
echo.

set "ERR=0"
set "PYTHON="
set "VENV_PY=%CD%\.venv\Scripts\python.exe"
set "REQUIREMENTS=%CD%\requirements.txt"
set "XRAY_DIR=%CD%\xray"
set "XRAY_EXE=%CD%\xray\xray.exe"
set "TMP_DIR=%CD%\.setup_tmp"

goto :main

:fail
echo.
echo ERROR: %~1
set "ERR=1"
goto :eof

:ok
echo   OK: %~1
goto :eof

:refresh_python_path
if exist "%LocalAppData%\Programs\Python\Launcher\py.exe" (
  set "PATH=%LocalAppData%\Programs\Python\Launcher;%PATH%"
)
for /d %%D in ("%LocalAppData%\Programs\Python\Python3*") do (
  if exist "%%~D\python.exe" set "PATH=%%~D;%%~D\Scripts;%PATH%"
)
for /d %%D in ("%ProgramFiles%\Python3*") do (
  if exist "%%~D\python.exe" set "PATH=%%~D;%%~D\Scripts;%PATH%"
)
for /d %%D in ("%ProgramFiles%\Python*") do (
  if exist "%%~D\python.exe" set "PATH=%%~D;%%~D\Scripts;%PATH%"
)
goto :eof

:probe_python
:: %1 = command to run that prints sys.executable on success
set "PYTHON="
set "PY_PROBE=%TEMP%\fetch_mtproto_py_probe.txt"
del "%PY_PROBE%" >nul 2>&1
%* >"%PY_PROBE%" 2>nul
if not exist "%PY_PROBE%" goto :eof
set /p PYTHON=<"%PY_PROBE%"
del "%PY_PROBE%" >nul 2>&1
if not defined PYTHON goto :eof
if not exist "%PYTHON%" (
  set "PYTHON="
  goto :eof
)
"%PYTHON%" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>&1
if errorlevel 1 (
  echo   Found Python but need 3.10+: %PYTHON%
  set "PYTHON="
)
goto :eof

:find_python
set "PYTHON="
call :refresh_python_path

:: Prefer Python already on PATH (3.10+)
where python >nul 2>&1
if not errorlevel 1 call :probe_python python -c "import sys; print(sys.executable)"

if not defined PYTHON (
  where python3 >nul 2>&1
  if not errorlevel 1 call :probe_python python3 -c "import sys; print(sys.executable)"
)

if not defined PYTHON (
  where py >nul 2>&1
  if not errorlevel 1 (
    call :probe_python py -3 -c "import sys; print(sys.executable)"
    if not defined PYTHON call :probe_python py -3.10 -c "import sys; print(sys.executable)"
  )
)

:: Fallback install locations (e.g. after winget, before PATH refresh)
if not defined PYTHON (
  for %%P in (
    "%LocalAppData%\Programs\Python\Python310\python.exe"
    "%LocalAppData%\Programs\Python\Python312\python.exe"
    "%LocalAppData%\Programs\Python\Python311\python.exe"
    "%LocalAppData%\Programs\Python\Python313\python.exe"
    "%ProgramFiles%\Python310\python.exe"
    "%ProgramFiles%\Python312\python.exe"
    "%ProgramFiles%\Python311\python.exe"
  ) do (
    if not defined PYTHON if exist %%~P call :probe_python "%%~P" -c "import sys; print(sys.executable)"
  )
)
goto :eof

:deps_satisfied
"%VENV_PY%" -c "import telethon, python_socks, TelethonFakeTLS, cryptography" >nul 2>&1
goto :eof

:find_xray_on_path
set "XRAY_ON_PATH="
where xray.exe >nul 2>&1
if not errorlevel 1 for /f "delims=" %%X in ('where xray.exe 2^>nul') do (
  set "XRAY_ON_PATH=%%X"
  goto :eof
)
where xray >nul 2>&1
if not errorlevel 1 for /f "delims=" %%X in ('where xray 2^>nul') do (
  set "XRAY_ON_PATH=%%X"
  goto :eof
)
goto :eof

:main
:: ---------- 1) Python ----------
echo [1/6] Checking for installed Python 3.10+ ...
call :find_python
if defined PYTHON (
  call :ok "Using installed Python: %PYTHON%"
) else (
  echo   No suitable Python found. Installing Python 3.10.0 via winget ...
  where winget >nul 2>&1
  if errorlevel 1 (
    call :fail "winget not found. Install Python 3.10.0 from https://www.python.org/downloads/release/python-3100/ then re-run setup.cmd"
    goto :done
  )

  winget install -e --id Python.Python.3.10 --version 3.10.0 --scope user --accept-package-agreements --accept-source-agreements
  if errorlevel 1 (
    echo   User-scope install failed; trying machine scope ...
    winget install -e --id Python.Python.3.10 --version 3.10.0 --accept-package-agreements --accept-source-agreements
  )
  if errorlevel 1 (
    call :fail "Could not install Python 3.10.0 via winget. Install it manually from https://www.python.org/downloads/release/python-3100/ then re-run setup.cmd"
    goto :done
  )

  call :find_python
  if not defined PYTHON (
    call :fail "Python 3.10.0 installed but not on PATH yet. Close this window, open a new one, and re-run setup.cmd"
    goto :done
  )
  call :ok "Installed Python 3.10.0: %PYTHON%"
)
echo.

:: ---------- 2) Virtual environment ----------
echo [2/6] Creating virtual environment (.venv) ...
if exist "%VENV_PY%" (
  call :ok "Existing .venv found"
) else (
  "%PYTHON%" -m venv "%CD%\.venv"
  if errorlevel 1 (
    call :fail "Failed to create .venv"
    goto :done
  )
  call :ok "Created .venv"
)
if not exist "%VENV_PY%" (
  call :fail "venv python missing: %VENV_PY%"
  goto :done
)
echo.

:: ---------- 3) Python packages ----------
echo [3/6] Installing Python packages ...
if not exist "%REQUIREMENTS%" (
  call :fail "requirements.txt not found"
  goto :done
)
call :deps_satisfied
if not errorlevel 1 (
  call :ok "Dependencies already installed in .venv"
) else (
  "%VENV_PY%" -m pip install --upgrade pip setuptools wheel
  if errorlevel 1 (
    call :fail "pip upgrade failed"
    goto :done
  )
  "%VENV_PY%" -m pip install -r "%REQUIREMENTS%"
  if errorlevel 1 (
    call :fail "pip install -r requirements.txt failed"
    goto :done
  )
  call :ok "Dependencies installed"
)
echo.

:: ---------- 4) Xray-core ----------
echo [4/6] Checking Xray-core ...
call :find_xray_on_path
if defined XRAY_ON_PATH (
  call :ok "Using xray from PATH: !XRAY_ON_PATH!"
) else if exist "%XRAY_EXE%" (
  call :ok "xray.exe already present in xray folder"
) else if exist "%CD%\bin\xray.exe" (
  if not exist "%XRAY_DIR%" mkdir "%XRAY_DIR%" >nul 2>&1
  copy /y "%CD%\bin\xray.exe" "%XRAY_EXE%" >nul
  call :ok "Copied bin\xray.exe to xray folder"
) else (
  echo   Downloading Xray-core for Windows ...
  where curl >nul 2>&1
  if errorlevel 1 (
    call :fail "curl.exe not found (needed to download Xray)"
    goto :done
  )
  where tar >nul 2>&1
  if errorlevel 1 (
    call :fail "tar.exe not found (needed to extract Xray)"
    goto :done
  )

  if exist "%TMP_DIR%" rd /s /q "%TMP_DIR%"
  mkdir "%TMP_DIR%\xray" >nul 2>&1

  set "XRAY_ASSET=Xray-windows-64.zip"
  if /i "%PROCESSOR_ARCHITECTURE%"=="ARM64" set "XRAY_ASSET=Xray-windows-arm64-v8a.zip"
  if /i "%PROCESSOR_ARCHITEW6432%"=="ARM64" set "XRAY_ASSET=Xray-windows-arm64-v8a.zip"

  set "XRAY_URL=https://github.com/XTLS/Xray-core/releases/latest/download/!XRAY_ASSET!"
  echo   URL: !XRAY_URL!
  curl.exe -L --retry 3 --fail -o "%TMP_DIR%\!XRAY_ASSET!" "!XRAY_URL!"
  if errorlevel 1 (
    call :fail "Failed to download !XRAY_ASSET!"
    if exist "%TMP_DIR%" rd /s /q "%TMP_DIR%"
    goto :done
  )

  tar -xf "%TMP_DIR%\!XRAY_ASSET!" -C "%TMP_DIR%\xray"
  if errorlevel 1 (
    call :fail "Failed to extract !XRAY_ASSET!"
    if exist "%TMP_DIR%" rd /s /q "%TMP_DIR%"
    goto :done
  )

  if not exist "%TMP_DIR%\xray\xray.exe" (
    call :fail "xray.exe missing from archive"
    if exist "%TMP_DIR%" rd /s /q "%TMP_DIR%"
    goto :done
  )

  if not exist "%XRAY_DIR%" mkdir "%XRAY_DIR%" >nul 2>&1

  copy /y "%TMP_DIR%\xray\xray.exe" "%XRAY_EXE%" >nul
  if not exist "%XRAY_DIR%\geoip.dat" if exist "%TMP_DIR%\xray\geoip.dat" copy /y "%TMP_DIR%\xray\geoip.dat" "%XRAY_DIR%\geoip.dat" >nul
  if not exist "%XRAY_DIR%\geosite.dat" if exist "%TMP_DIR%\xray\geosite.dat" copy /y "%TMP_DIR%\xray\geosite.dat" "%XRAY_DIR%\geosite.dat" >nul

  if exist "%TMP_DIR%" rd /s /q "%TMP_DIR%"
  if not exist "%XRAY_EXE%" (
    call :fail "xray.exe was not installed"
    goto :done
  )
  call :ok "Installed xray.exe in xray folder"
)
echo.

:: ---------- 5) Config ----------
echo [5/6] Checking config.yaml ...
if exist "%CD%\config.yaml" (
  call :ok "config.yaml already exists (left unchanged)"
) else if exist "%CD%\config.example.yaml" (
  copy /y "%CD%\config.example.yaml" "%CD%\config.yaml" >nul
  call :ok "Created config.yaml from config.example.yaml"
  echo.
  echo   IMPORTANT: Edit config.yaml and set telegram.api_id / telegram.api_hash from
  echo   https://my.telegram.org/apps
) else (
  call :fail "Neither config.yaml nor config.example.yaml found"
  goto :done
)
echo.

:: ---------- 6) Data directories ----------
echo [6/6] Ensuring data folders ...
mkdir "%CD%\data" >nul 2>&1
mkdir "%CD%\data\mtproto" >nul 2>&1
mkdir "%CD%\data\v2ray" >nul 2>&1
mkdir "%CD%\sessions" >nul 2>&1
mkdir "%CD%\logs" >nul 2>&1
call :ok "data, sessions, and logs folders ready (catalog.db is created on first run)"
echo.

:done
echo ========================================
if "%ERR%"=="0" (
  echo  Setup finished successfully.
  echo ========================================
  echo.
  echo Next steps:
  echo   1. Edit config.yaml  ^(telegram.api_id, telegram.api_hash, telegram.sources^)
  echo   2. Optional: seed data\mtproto\proxies.txt with tg://proxy
  echo      links for one-time import into data\catalog.db
  echo      ^(bot falls back to direct if none work^)
  echo   3. Launch the control panel:
  echo        .venv\Scripts\pythonw.exe app.py
  echo.
  echo All features ^(scraper, pings, subscription server,
  echo open top proxies in Telegram^) run from the GUI.
) else (
  echo  Setup failed. See errors above.
  echo ========================================
)
echo.
pause
exit /b %ERR%
