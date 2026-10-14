# One-click start: runs the supervisor (which starts and keeps everything running)
# and opens the dashboard once it is up. Safe to run again at any time.
#   powershell -ExecutionPolicy Bypass -File scripts\launch.ps1

$Root = Split-Path -Parent $PSScriptRoot

$running = Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -match 'supervisor\.py' }
if (-not $running) {
    Write-Host 'Starting the supervisor...'
    Start-Process cmd -WorkingDirectory $Root -WindowStyle Minimized -ArgumentList '/k', 'title Supervisor && python -u scripts\supervisor.py'
} else {
    Write-Host 'Supervisor already running.'
}

Write-Host 'Waiting for the dashboard (first start takes 1-2 minutes)...'
for ($i = 0; $i -lt 180; $i++) {
    if (Get-NetTCPConnection -State Listen -LocalPort 5000 -ErrorAction SilentlyContinue) { break }
    Start-Sleep -Seconds 1
}
Start-Process 'http://localhost:5000'
