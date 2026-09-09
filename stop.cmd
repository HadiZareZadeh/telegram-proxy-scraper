@echo off
setlocal EnableExtensions
cd /d "%~dp0"
title fetch-mtproto stop
echo Stopping leftover fetch-mtproto processes in:
echo   %CD%
echo.
set "FETCH_MTPROTO_ROOT=%CD%"
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-Content -LiteralPath '%~f0' | Select-Object -Skip 12 | Out-String | Invoke-Expression"
echo.
goto :eof

# --- powershell ---
$ErrorActionPreference = 'SilentlyContinue'
$root = [string]$env:FETCH_MTPROTO_ROOT
if (-not $root) { $root = (Get-Location).Path }
$root = $root.TrimEnd('\')
$self = [int]$PID
$skip = New-Object 'System.Collections.Generic.HashSet[int]'
[void]$skip.Add($self)
$parent = (Get-CimInstance Win32_Process -Filter "ProcessId=$self").ParentProcessId
if ($parent) { [void]$skip.Add([int]$parent) }

function Test-ProjectProcess($proc) {
    $name = [string]$proc.Name
    $cl = [string]$proc.CommandLine
    $exe = [string]$proc.ExecutablePath
    if ($cl -match '(?i)stop\.cmd') { return $false }
    if ($exe -and $exe.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) { return $true }
    if ($cl -and $cl.IndexOf($root, [StringComparison]::OrdinalIgnoreCase) -ge 0) { return $true }
    if ($name -match '(?i)^python(w)?\.exe$' -and (
            $cl -match 'fetch_mtproto' -or
            $cl -match '(?i)app\.pyw' -or
            $cl -match '(?i)(^|[\\/ ])app\.py(\s|$)'
        )) { return $true }
    if ($name -match '(?i)^xray\.exe$' -and (
            $cl -match 'fetch-mtproto' -or
            $cl -match 'xray-ping-'
        )) { return $true }
    if ($name -match '(?i)^cmd\.exe$' -and $cl -match 'fetch_mtproto') { return $true }
    return $false
}

$killed = New-Object 'System.Collections.Generic.HashSet[int]'
for ($pass = 1; $pass -le 3; $pass++) {
    $targets = @(Get-CimInstance Win32_Process | Where-Object {
        $_.ProcessId -and -not $skip.Contains([int]$_.ProcessId) -and (Test-ProjectProcess $_)
    })
    if (-not $targets.Count) { break }
    foreach ($proc in $targets) {
        $id = [int]$proc.ProcessId
        if ($killed.Contains($id)) { continue }
        Write-Host ("  kill {0,-6} {1}" -f $id, $proc.Name)
        Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
        [void]$killed.Add($id)
    }
    Start-Sleep -Milliseconds 400
}

if ($killed.Count -eq 0) {
    Write-Host 'No leftover processes found.'
} else {
    Write-Host ("Stopped {0} process(es)." -f $killed.Count)
}
