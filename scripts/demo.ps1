$projectRoot = Split-Path -Parent $PSScriptRoot
$pidFile = Join-Path $projectRoot 'data\cluster-pids.json'

if (!(Test-Path -LiteralPath $pidFile)) { throw 'Start Vault first: npm run start' }
$processes = Get-Content -Raw -LiteralPath $pidFile | ConvertFrom-Json
$target = $processes | Where-Object { $_.name -eq 'node-a' }
if (!$target) { throw 'node-a is not listed as running.' }

Write-Host 'Stopping node-a to simulate an independent storage-node failure...' -ForegroundColor Yellow
Stop-Process -Id $target.pid -Force -ErrorAction Stop
$nodeExecutable = (Get-Command node -ErrorAction Stop).Source
$logs = Join-Path $projectRoot 'data\logs'
$spare = Start-Process -FilePath $nodeExecutable -ArgumentList @('src/storage-node.js', 'node-d', '3004', 'data/node-d') -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logs 'node-d.out.log') -RedirectStandardError (Join-Path $logs 'node-d.error.log') -PassThru
$updated = @($processes) + [PSCustomObject]@{ name = 'node-d'; pid = $spare.Id }
$updated | ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath $pidFile
Write-Host 'Started node-d as the replacement storage node.' -ForegroundColor Green
Write-Host 'Now open http://127.0.0.1:3100, download your uploaded object, wait about five seconds, and press Run integrity scan.'
Write-Host 'Expected result: download succeeds from nodes b/c; the coordinator copies the missing third replica to node-d.'
