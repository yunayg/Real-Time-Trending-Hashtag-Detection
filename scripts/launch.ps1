# One-click start: runs the supervisor (which starts and keeps everything running)
# and opens the dashboard once it is up. Safe to run again at any time.
#   powershell -ExecutionPolicy Bypass -File scripts\launch.ps1 [-Skip 1220000]
# -Skip N makes a (re)started producer begin N tweets into the replay; 1220000 is 15 June 2009.
# Without it, the producer resumes where it last stopped.

param([int]$Skip = -1)

$Root = Split-Path -Parent $PSScriptRoot

$producer = Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -match 'tweet_producer\.py' }
if ($Skip -ge 0) {
    if ($producer) {
        Write-Host 'The producer is already running, so -Skip is ignored (stop it first to jump).'
    } else {
        New-Item -ItemType Directory -Force "$Root\state" | Out-Null
        "{`"status`": `"stopped`", `"rate`": 500, `"position`": $Skip, `"updated_ms`": 0}" | Set-Content "$Root\state\producer.json" -Encoding ascii
        Remove-Item "$Root\state\producer.paused" -ErrorAction SilentlyContinue
        Write-Host "The producer will start $Skip tweets into the replay."
    }
}

$running = Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -match 'supervisor\.py' }
if (-not $running) {
    Write-Host 'Starting the supervisor...'
    # window style 7 = minimised without taking focus
    $shell = New-Object -ComObject WScript.Shell
    $shell.CurrentDirectory = $Root
    $shell.Run('cmd /k "title Supervisor && python -u scripts\supervisor.py"', 7, $false) | Out-Null
} else {
    Write-Host 'Supervisor already running.'
}

Write-Host 'Waiting for the dashboard (first start takes 1-2 minutes)...'
for ($i = 0; $i -lt 180; $i++) {
    if (Get-NetTCPConnection -State Listen -LocalPort 5000 -ErrorAction SilentlyContinue) { break }
    Start-Sleep -Seconds 1
}
Start-Process 'http://localhost:5000'
