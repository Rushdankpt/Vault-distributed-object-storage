$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$pidFile = Join-Path $projectRoot 'data\cluster-pids.json'

if (!(Test-Path -LiteralPath $pidFile)) {
  Write-Host 'No Vault process list was found; the cluster is probably already stopped.'
  exit 0
}

$processes = Get-Content -Raw -LiteralPath $pidFile | ConvertFrom-Json
foreach ($process in $processes) {
  $running = Get-Process -Id $process.pid -ErrorAction SilentlyContinue
  if ($running) {
    Stop-Process -Id $process.pid -Force
    Write-Host "Stopped $($process.name) (PID $($process.pid))."
  }
}
Remove-Item -LiteralPath $pidFile
Write-Host 'Vault cluster stopped.' -ForegroundColor Yellow
