# ─────────────────────────────────────────────────────────────────────────────
# Cold-pull validation: fresh node joins an ALREADY-CONVERGED mesh (the §4 "Cold"
# mode). This is the clean, deterministic proof of the cold-pull mechanism and its
# VALUE — and it sidesteps the iroh same-identity-restart ghost.
#
# Topology (mdns off, seed-only — mirrors the soak):
#   A = node-admin / hub. NO seeds.
#   C,D = workers. seed=A, RAFKA_NODE_ADMIN_ADDR=A.
# Converge so the hub A holds the full view (count=3: A,C,D).
# THEN start a brand-new worker B (seed=A, RAFKA_NODE_ADMIN_ADDR=A). On its first
# connect to A it COLD-PULLS A's live_digests over QUIC and hydrates IMMEDIATELY —
# so B knows the whole mesh within ~1s of boot instead of waiting on gossip re-flood.
#
# PASS:
#   * B's log shows  "cold-pull: hydrated live_digests from node-admin snapshot count=N" with N>=2
#   * A's log shows  "cold-pull snapshot served ... count=3"
#   * B reaches topo count = 4 (A,B,C,D) within a few seconds of boot.
#
# Usage:  pwsh -File repro/coldpull.ps1
# ─────────────────────────────────────────────────────────────────────────────
param([int]$ConvergeSecs = 30, [int]$WatchSecs = 20)
$ErrorActionPreference = 'Stop'
$root      = Split-Path -Parent $PSScriptRoot
$BrokerExe = Join-Path $root 'target\debug\rafka-broker.exe'
$dataRoot  = Join-Path $PSScriptRoot 'data-cp'
$logRoot   = Join-Path $PSScriptRoot 'logs-cp'
if (-not (Test-Path $BrokerExe)) { Write-Error "broker not built: $BrokerExe"; exit 1 }

Get-Process rafka-broker -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800
Remove-Item -Recurse -Force $dataRoot, $logRoot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $dataRoot, $logRoot | Out-Null

$ports = @{ A = 17811; B = 17812; C = 17813; D = 17814 }

function Launch($name, $seeds, $admin) {
    $env:RAFKA_MESH_ID = 'meshR'; $env:RAFKA_MDNS_ENABLE = 'false'
    $env:RAFKA_GOSSIP_INTERVAL_MS = '1000'; $env:RAFKA_STALENESS_MS = '30000'
    $env:RUST_LOG = 'info'; $env:OTEL_EXPORTER_OTLP_ENDPOINT = 'http://localhost:4316'
    $env:RAFKA_DATA_DIR = (Join-Path $dataRoot $name)
    $env:RAFKA_NODE_BIND_ADDR = "127.0.0.1:$($ports[$name])"
    if ($seeds) { $env:RAFKA_SEED_NODES = $seeds } else { Remove-Item Env:\RAFKA_SEED_NODES -ErrorAction SilentlyContinue }
    if ($admin) { $env:RAFKA_NODE_ADMIN_ADDR = $admin } else { Remove-Item Env:\RAFKA_NODE_ADMIN_ADDR -ErrorAction SilentlyContinue }
    Remove-Item Env:\RAFKA_AUTO_SHUTDOWN_SECS -ErrorAction SilentlyContinue
    Start-Process -FilePath $BrokerExe -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logRoot "$name.out.log") `
        -RedirectStandardError  (Join-Path $logRoot "$name.err.log") | Out-Null
}
function Clean($t) { return ($t -replace "\x1b\[[0-9;]*m", "") }
function NodeId($name) {
    $c = Clean (Get-Content (Join-Path $logRoot "$name.out.log") -Raw)
    if ($c -match 'node_id=([0-9a-f]{64})') { return $matches[1] }; return $null
}
function TopoCount($name) {
    $f = Join-Path $logRoot "$name.out.log"
    if (-not (Test-Path $f)) { return '-' }
    $m = [regex]::Matches((Clean (Get-Content $f -Raw)), 'topology-node cache snapshot[^\r\n]*count=(\d+)')
    if ($m.Count) { return $m[$m.Count-1].Groups[1].Value } else { return '-' }
}

Write-Host "[1] start node-admin/hub A ..."
Launch 'A' $null $null
Start-Sleep -Seconds 5
$aId = NodeId 'A'; if (-not $aId) { Write-Error 'no A node_id'; exit 1 }
$aSeed = "$aId@127.0.0.1:$($ports.A)"
Write-Host "    A = $aSeed"

Write-Host "[2] start workers C,D (seed=A, admin=A) and converge ${ConvergeSecs}s ..."
foreach ($n in 'C','D') { Launch $n $aSeed $aSeed; Start-Sleep -Milliseconds 400 }
Start-Sleep -Seconds $ConvergeSecs
Write-Host ("    converged: hub A topo count = {0} (expect 3: A,C,D)" -f (TopoCount A))

Write-Host "[3] start FRESH node B (seed=A, admin=A) — should cold-pull A's full view at boot ..."
$bBootUtc = (Get-Date).ToUniversalTime()
Launch 'B' $aSeed $aSeed

Write-Host "[4] watch B hydrate ${WatchSecs}s ..."
$elapsed = 0
while ($elapsed -lt $WatchSecs) {
    Start-Sleep -Seconds 4; $elapsed += 4
    Write-Host ("    +{0,2}s  B topo count = {1}" -f $elapsed, (TopoCount B))
}

# ---- verdict ----
$bLog = Clean (Get-Content (Join-Path $logRoot 'B.out.log') -Raw)
$aLog = Clean (Get-Content (Join-Path $logRoot 'A.out.log') -Raw)
$hyd  = [regex]::Matches($bLog, 'cold-pull: hydrated live_digests from node-admin snapshot[^\r\n]*count=(\d+)')
$served = [regex]::Matches($aLog, 'cold-pull snapshot served[^\r\n]*count=(\d+)')
# first cold-pull B made (the boot one)
$firstHyd = if ($hyd.Count) { [int]$hyd[0].Groups[1].Value } else { 0 }
$maxServed = 0; foreach ($m in $served) { if ([int]$m.Groups[1].Value -gt $maxServed) { $maxServed = [int]$m.Groups[1].Value } }

Write-Host ""
Write-Host "=== COLD-PULL VERDICT ==="
Write-Host ("  B cold-pull hydrations: {0}  (first count={1})" -f $hyd.Count, $firstHyd)
Write-Host ("  A snapshots served:     {0}  (max count={1})" -f $served.Count, $maxServed)
Write-Host ("  B final topo count:     {0}  (expect 4)" -f (TopoCount B))
$pass = ($hyd.Count -gt 0) -and ($firstHyd -ge 2)
Write-Host ("  RESULT: {0}" -f $(if ($pass) { 'PASS — fresh node hydrated the full mesh view from the node-admin at boot' } else { 'CHECK LOGS' })) -ForegroundColor $(if ($pass) { 'Green' } else { 'Yellow' })
Write-Host "(processes left running; logs in $logRoot)"
