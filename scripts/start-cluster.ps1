$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$dataRoot = Join-Path $projectRoot 'data'
$pidFile = Join-Path $dataRoot 'cluster-pids.json'
$logs = Join-Path $dataRoot 'logs'

if (Test-Path -LiteralPath $pidFile) {
  $oldPids = Get-Content -Raw -LiteralPath $pidFile | ConvertFrom-Json
  $active = @($oldPids | Where-Object { Get-Process -Id $_.pid -ErrorAction SilentlyContinue })
  if ($active.Count -gt 0) {
    throw "Vault is already running (process IDs: $($active.Id -join ', ')). Run npm run stop first."
  }
}

New-Item -ItemType Directory -Force -Path $dataRoot, $logs | Out-Null
$nodeExecutable = (Get-Command node -ErrorAction Stop).Source
$processes = @()

function Start-VaultProcess([string]$name, [string[]]$arguments) {
  $stdout = Join-Path $logs "$name.out.log"
  $stderr = Join-Path $logs "$name.error.log"
  $process = Start-Process -FilePath $nodeExecutable -ArgumentList $arguments -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
  $script:processes += [PSCustomObject]@{ name = $name; pid = $process.Id }
}

Start-VaultProcess 'node-a' @('src/storage-node.js', 'node-a', '3001', 'data/node-a')
Start-VaultProcess 'node-b' @('src/storage-node.js', 'node-b', '3002', 'data/node-b')
Start-VaultProcess 'node-c' @('src/storage-node.js', 'node-c', '3003', 'data/node-c')
Start-Sleep -Milliseconds 500
Start-VaultProcess 'coordinator' @('src/coordinator.js', 'data/coordinator', '3100')

$processes | ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath $pidFile
Write-Host 'Vault cluster is running.' -ForegroundColor Green
Write-Host 'Open http://127.0.0.1:3100 in your browser.'
Write-Host 'Three storage nodes are active (ports 3001-3003); node-d is the replacement spare used by the failure demo.'
