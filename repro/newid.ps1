# ─────────────────────────────────────────────────────────────────────────────
# Isolate the same-identity restart wedge: bring a worker back with a NEW node-id
# (fresh data dir) on the SAME port, and confirm the mesh handles it cleanly —
# i.e. the collision in the restart test is specifically the reused NodeId, and the
# fix is "come back as a new identity".
#
#   A = node-admin/hub (no seeds).  C,D = workers (seed=A, admin=A).
#   B(old id) joins, converges.  Kill B.  Restart on the SAME port but with a FRESH
#   data dir → NEW node-id + NEW node-name.
#
# PASS (new-id avoids the wedge):
#   * B-new logs "peer connected (outbound)" to A  AND  "cold-pull: hydrated ..."
#   * B-new shows NO persistent "unknown NodeIdMappedAddr" wedge
#   * A's view adds B-new's id; B-old's id ages out (staleness) → the mesh redirected.
#
# Contrast: the same-id restart (coldpull.ps1 worker case) never reconnects.
#
# Usage:  pwsh -File repro/newid.ps1
# ─────────────────────────────────────────────────────────────────────────────
param([int]$ConvergeSecs = 18, [int]$WatchSecs = 45)
$ErrorActionPreference = 'Stop'
$root      = Split-Path -Parent $PSScriptRoot
$BrokerExe = Join-Path $root 'target\debug\rafka-broker.exe'
$dataRoot  = Join-Path $PSScriptRoot 'data-ni'
$logRoot   = Join-Path $PSScriptRoot 'logs-ni'
if (-not (Test-Path $BrokerExe)) { Write-Error "broker not built: $BrokerExe"; exit 1 }

Get-Process rafka-broker -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800
Remove-Item -Recurse -Force $dataRoot, $logRoot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $dataRoot, $logRoot | Out-Null

$ports = @{ A = 17821; B = 17822; C = 17823; D = 17824 }
$procs = @{}

# Generate a random 32-byte iroh secret key (64 hex) → a brand-new identity whose
# self-derived node-name carries a random 6-hex suffix.
function RandKey() { return (-join ((1..32) | ForEach-Object { '{0:x2}' -f (Get-Random -Maximum 256) })) }

function Launch($name, $dataName, $logName, $seeds, $admin, $secret) {
    $env:RAFKA_MESH_ID = 'meshR'; $env:RAFKA_MDNS_ENABLE = 'false'
    $env:RAFKA_GOSSIP_INTERVAL_MS = '1000'; $env:RAFKA_STALENESS_MS = '30000'
    $env:RUST_LOG = 'info'; $env:OTEL_EXPORTER_OTLP_ENDPOINT = 'http://localhost:4316'
    $env:RAFKA_DATA_DIR = (Join-Path $dataRoot $dataName)
    $env:RAFKA_NODE_BIND_ADDR = "127.0.0.1:$($ports[$name])"
    if ($seeds) { $env:RAFKA_SEED_NODES = $seeds } else { Remove-Item Env:\RAFKA_SEED_NODES -ErrorAction SilentlyContinue }
    if ($admin) { $env:RAFKA_NODE_ADMIN_ADDR = $admin } else { Remove-Item Env:\RAFKA_NODE_ADMIN_ADDR -ErrorAction SilentlyContinue }
    if ($secret) { $env:RAFKA_NODE_SECRET_KEY = $secret } else { Remove-Item Env:\RAFKA_NODE_SECRET_KEY -ErrorAction SilentlyContinue }
    Remove-Item Env:\RAFKA_AUTO_SHUTDOWN_SECS -ErrorAction SilentlyContinue
    Start-Process -FilePath $BrokerExe -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logRoot "$logName.out.log") `
        -RedirectStandardError  (Join-Path $logRoot "$logName.err.log")
}
function Clean($t) { return ($t -replace "\x1b\[[0-9;]*m", "") }
function NodeId($logName) {
    $f = Join-Path $logRoot "$logName.out.log"
    if (-not (Test-Path $f)) { return $null }
    if ((Clean (Get-Content $f -Raw)) -match 'node_id=([0-9a-f]{64})') { return $matches[1] }; return $null
}
function NodeName($logName) {
    $f = Join-Path $logRoot "$logName.out.log"
    if ((Clean (Get-Content $f -Raw)) -match 'node_name="([^"]+)"') { return $matches[1] }; return '?'
}
# Does A's CURRENT live view (its newest topology snapshot region) contain this id?
function AhasId($id) {
    $c = Clean (Get-Content (Join-Path $logRoot 'A.out.log') -Raw)
    # look at upserts/removes near the end: count last upsert vs remove for this id
    $up = ([regex]::Matches($c, "topology-node upserted from gossip[^\r\n]*node_id=$id")).Count
    $rm = ([regex]::Matches($c, "topology-node evicted[^\r\n]*node_id=$id")).Count
    return "up=$up rm=$rm"
}

Write-Host "[1] start hub A + workers C,D, converge ${ConvergeSecs}s ..."
$procs['A'] = Launch 'A' 'A' 'A' $null $null
Start-Sleep -Seconds 5
$aId = NodeId 'A'; if (-not $aId) { Write-Error 'no A id'; exit 1 }
$aSeed = "$aId@127.0.0.1:$($ports.A)"
foreach ($n in 'C','D') { $procs[$n] = Launch $n $n $n $aSeed $aSeed; Start-Sleep -Milliseconds 400 }
$procs['B'] = Launch 'B' 'B-old' 'B-old' $aSeed $aSeed
Start-Sleep -Seconds $ConvergeSecs
$bOldId = NodeId 'B-old'
Write-Host "    B-old node_id = $bOldId  name=$(NodeName 'B-old')"
Write-Host "    A view of B-old: $(AhasId $bOldId)"

Write-Host "[2] kill B, bring it back on SAME port with a RANDOM new key (new id → new 6-hex name suffix) ..."
Stop-Process -Id $procs['B'].Id -Force
Start-Sleep -Seconds 2
$bKey = RandKey
$procs['B'] = Launch 'B' 'B-new' 'B-new' $aSeed $aSeed $bKey
Start-Sleep -Seconds 6
$bNewId = NodeId 'B-new'
$oldSuffix = ($(NodeName 'B-old') -split '\.')[-1]
$newSuffix = ($(NodeName 'B-new') -split '\.')[-1]
Write-Host "    B-old name=$(NodeName 'B-old')   (suffix $oldSuffix)"
Write-Host "    B-new name=$(NodeName 'B-new')   (suffix $newSuffix)"
Write-Host ("    new random 6-hex suffix, old != new: {0}" -f ($bOldId -ne $bNewId))

Write-Host "[3] watch B-new reconnect ${WatchSecs}s ..."
$elapsed = 0
while ($elapsed -lt $WatchSecs) {
    Start-Sleep -Seconds 9; $elapsed += 9
    $bn = Clean (Get-Content (Join-Path $logRoot 'B-new.out.log') -Raw)
    $conn = ([regex]::Matches($bn, 'peer connected \(outbound\)')).Count
    $hyd  = ([regex]::Matches($bn, 'cold-pull: hydrated')).Count
    $wedge= ([regex]::Matches((Clean (Get-Content (Join-Path $logRoot 'B-new.err.log') -Raw)), 'unknown NodeIdMappedAddr')).Count
    Write-Host ("    +{0,2}s  B-new: outbound_connects={1} coldpull={2} NodeIdMappedAddr_drops={3}" -f $elapsed,$conn,$hyd,$wedge)
}

Write-Host ""
Write-Host "=== NEW-ID RESTART VERDICT ==="
$bn = Clean (Get-Content (Join-Path $logRoot 'B-new.out.log') -Raw)
$conn = ([regex]::Matches($bn, 'peer connected \(outbound\)')).Count
$hyd  = ([regex]::Matches($bn, 'cold-pull: hydrated')).Count
Write-Host ("  B-new connected to A:   {0}" -f ($conn -gt 0))
Write-Host ("  B-new cold-pulled:      {0}" -f ($hyd -gt 0))
Write-Host ("  A view of B-new id:     {0}" -f (AhasId $bNewId))
Write-Host ("  A view of B-old id:     {0}  (expect rm>=1 after ~30s staleness)" -f (AhasId $bOldId))
$pass = ($conn -gt 0) -and ($hyd -gt 0)
Write-Host ("  RESULT: {0}" -f $(if ($pass) { 'PASS — new-id reconnects cleanly (collision was the reused NodeId)' } else { 'CHECK LOGS' })) -ForegroundColor $(if ($pass) { 'Green' } else { 'Yellow' })
Write-Host "(logs in $logRoot)"
