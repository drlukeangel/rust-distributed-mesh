# ─────────────────────────────────────────────────────────────────────────────
# Node-drop repro: hub-restart, seed-only, mdns-off (mirrors the real soak).
#
# Topology (matches admin-ui spawn config: RAFKA_MDNS_ENABLE=false, children seed
# to the admin-ui hub):
#   A  = hub. NO seeds. Pinned port. Stable identity.
#   B,C,D = workers. Each seeds ONLY to A. NO mdns. Stable identities/ports.
#
# Converge → A.peer_count=3, B/C/D.peer_count=1 (each connected to hub A).
#
# Then HARD-kill + restart A (same node_id + port). The survivors B/C/D lose their
# connection to A (detected ~30s later via iroh max_idle_timeout) and must RE-DIAL
# their seed. Current dial_seeds is one-shot (breaks after first success) → B/C/D
# never re-dial → stuck at peer_count=0 → the node-drop bug.
#
# A hard kill (Stop-Process -Force) is staleness_dead, NOT a graceful Leaving, so it
# is NOT held in recently_evicted's 10s resurrection guard — a fast restart is not
# silently dropped.
#
# VERDICT:
#   BUG   : after the restart window, B/C/D.peer_count stays 0 (+ A stays 0).
#   FIXED : B/C/D re-dial A → A.peer_count returns to 3, B/C/D return to 1.
#
# Re-runnable. Usage:  pwsh -File repro/kill-one.ps1
# ─────────────────────────────────────────────────────────────────────────────
param(
    [int]$ConvergeSecs = 18,
    [int]$PostKillSecs = 95,
    [int]$SnapEverySecs = 15
)
$ErrorActionPreference = 'Stop'
$root      = Split-Path -Parent $PSScriptRoot
$BrokerExe = Join-Path $root 'target\debug\rafka-broker.exe'
$dataRoot  = Join-Path $PSScriptRoot 'data'
$logRoot   = Join-Path $PSScriptRoot 'logs'

if (-not (Test-Path $BrokerExe)) { Write-Error "broker not built: $BrokerExe"; exit 1 }

# ---- 0. clean slate ----
Get-Process rafka-broker -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800
Remove-Item -Recurse -Force $dataRoot, $logRoot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $dataRoot, $logRoot | Out-Null

$nodes = @{
    A = @{ port = 17801; seeds = $null }
    B = @{ port = 17802; seeds = $null }
    C = @{ port = 17803; seeds = $null }
    D = @{ port = 17804; seeds = $null }
}

function Launch($name, $port, $seeds, $tag) {
    $env:RAFKA_MESH_ID          = 'meshR'
    $env:RAFKA_MDNS_ENABLE      = 'false'
    $env:RAFKA_GOSSIP_INTERVAL_MS = '1000'
    $env:RAFKA_STALENESS_MS     = '30000'
    $env:RUST_LOG               = 'info'
    $env:OTEL_EXPORTER_OTLP_ENDPOINT = 'http://localhost:4316'
    $env:RAFKA_DATA_DIR         = (Join-Path $dataRoot $name)
    $env:RAFKA_NODE_BIND_ADDR   = "127.0.0.1:$port"
    if ($seeds) { $env:RAFKA_SEED_NODES = $seeds } else { Remove-Item Env:\RAFKA_SEED_NODES -ErrorAction SilentlyContinue }
    Remove-Item Env:\RAFKA_AUTO_SHUTDOWN_SECS -ErrorAction SilentlyContinue
    $out = Join-Path $logRoot "$name$tag.out.log"
    $err = Join-Path $logRoot "$name$tag.err.log"
    Start-Process -FilePath $BrokerExe -PassThru -WindowStyle Hidden -RedirectStandardOutput $out -RedirectStandardError $err
}

# Strip ANSI color escapes — the fmt layer always colorizes, so `node_id=` etc.
# are not literally adjacent in the raw bytes.
function Clean($text) { return ($text -replace "\x1b\[[0-9;]*m", "") }

function NodeId($name) {
    $out = Join-Path $logRoot "$name.out.log"
    if (-not (Test-Path $out)) { return $null }
    $c = Clean (Get-Content $out -Raw)
    if ($c -match 'node_id=([0-9a-f]{64})') { return $matches[1] }
    return $null
}

# peer_count from the LAST heartbeat line in the node's NEWEST log (a restarted
# node writes a fresh "-restart" log; the killed process's frozen log must be ignored).
function PeerCount($name) {
    $logs = Get-ChildItem -Path $logRoot -Filter "$name*.out.log" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime
    $last = $null
    foreach ($l in $logs) {
        $c = Clean (Get-Content $l.FullName -Raw)
        $m = [regex]::Matches($c, 'rafka\.mesh\.heartbeat\{[^}]*peer_count=(\d+)')
        if ($m.Count) { $last = $m[$m.Count-1].Groups[1].Value }
    }
    if ($null -eq $last) { return '-' } else { return $last }
}

function Snap($label) {
    $a = PeerCount A; $b = PeerCount B; $c = PeerCount C; $d = PeerCount D
    Write-Host ("  [{0,-14}] peer_count  A={1}  B={2}  C={3}  D={4}" -f $label, $a, $b, $c, $d)
}

# ---- 1. start hub A, learn its node_id ----
Write-Host "[1] starting hub A (no seeds) ..."
$procs = @{}
$procs['A'] = Launch 'A' $nodes.A.port $null ''
Start-Sleep -Seconds 5
$aId = NodeId 'A'
if (-not $aId) { Write-Error "could not parse A node_id from log"; exit 1 }
Write-Host "    A node_id = $aId"
$aSeed = "$aId@127.0.0.1:$($nodes.A.port)"

# ---- 2. start workers B,C,D seeding to A ----
Write-Host "[2] starting workers B,C,D (seed=A, mdns off) ..."
foreach ($n in 'B','C','D') {
    $procs[$n] = Launch $n $nodes[$n].port $aSeed ''
    Start-Sleep -Milliseconds 400
}

Write-Host "[3] converging ${ConvergeSecs}s ..."
Start-Sleep -Seconds $ConvergeSecs
Snap 'baseline'

# ---- 3. HARD kill + restart hub A (same identity/port, no seeds) ----
Write-Host "[4] HARD-killing hub A (pid $($procs['A'].Id)) ..."
Stop-Process -Id $procs['A'].Id -Force
Start-Sleep -Seconds 2
Write-Host "    restarting hub A (same data dir + port) ..."
$procs['A'] = Launch 'A' $nodes.A.port $null '-restart'
$killTime = Get-Date

# ---- 4. watch recovery ----
Write-Host "[5] watching recovery for ${PostKillSecs}s (idle-timeout ~30s) ..."
$elapsed = 0
while ($elapsed -lt $PostKillSecs) {
    Start-Sleep -Seconds $SnapEverySecs
    $elapsed += $SnapEverySecs
    Snap "+${elapsed}s"
}

# ---- 5. re-dial detection: did B/C/D dial A's node_id AFTER the kill? ----
Write-Host "[6] re-dial check (B/C/D 'peer connected (outbound)' to A after kill):"
foreach ($n in 'B','C','D') {
    $logs = Get-ChildItem -Path $logRoot -Filter "$n*.out.log"
    $reconnects = 0
    foreach ($l in $logs) {
        # count peer.connected spans naming A's id (a fresh dial of the hub)
        $c = Clean (Get-Content $l.FullName -Raw)
        $reconnects += ([regex]::Matches($c, "rafka\.mesh\.peer\.connected\{[^}]*peer_id=$aId")).Count
    }
    Write-Host ("    {0}: peer.connected(A) total spans = {1}" -f $n, $reconnects)
}

# ---- 6. verdict ----
$bF = PeerCount B; $cF = PeerCount C; $dF = PeerCount D; $aF = PeerCount A
Write-Host ""
Write-Host ("VERDICT  final peer_count: A=$aF B=$bF C=$cF D=$dF")
if ($bF -eq '1' -and $cF -eq '1' -and $dF -eq '1' -and $aF -eq '3') {
    Write-Host "RESULT: RECOVERED (workers re-acquired the hub)." -ForegroundColor Green
} else {
    Write-Host "RESULT: NOT RECOVERED (node-drop reproduced)." -ForegroundColor Red
}

Write-Host ""
Write-Host "(processes left running; logs in $logRoot)"
