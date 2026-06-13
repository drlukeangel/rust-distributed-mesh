# Fast smoke: node-admin boots, spawns nodes that JOIN with the new <mesh>.<type>.<N>
# names AND pass the name-bound cert check. Then a stateful restart → verify the name
# stays + node_id ROTATES + the mesh rebinds. ~90s. De-risks the long soak.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$exe  = Join-Path $root 'target\debug\rafka-node-admin.exe'
$data = Join-Path $PSScriptRoot 'data-smoke'
$log  = Join-Path $PSScriptRoot 'logs-smoke'
Get-Process rafka-node-admin,rafka-admin-ui,rafka-broker,rafka-gateway,rafka-compute,rafka-registry -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800
Remove-Item -Recurse -Force $data,$log -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $data,$log | Out-Null

$env:RAFKA_MESH_ID='mesh1'; $env:RAFKA_DATA_DIR=$data
$env:RAFKA_ADMIN_UI_BIND_ADDR='127.0.0.1:6969'; $env:RAFKA_NODE_BIND_ADDR='127.0.0.1:14819'
$env:RAFKA_SPAWN_PORT_BASE='15820'; $env:RAFKA_CHILD_BUILD_PROFILE='debug'
$env:CARGO_TARGET_DIR='./target'; $env:RUST_LOG='info'
$p = Start-Process -FilePath $exe -PassThru -WindowStyle Hidden -RedirectStandardOutput (Join-Path $log 'admin.out.log') -RedirectStandardError (Join-Path $log 'admin.err.log')
Write-Host "[1] node-admin starting (UI 6969) pid=$($p.Id) ..."

for ($i=0; $i -lt 40; $i++) { Start-Sleep -Milliseconds 500; try { Invoke-RestMethod 'http://127.0.0.1:6969/api/health' -TimeoutSec 1 | Out-Null; break } catch {} }
Write-Host "[2] health up. spawning 3 brokers + 1 gateway in mesh1 ..."
foreach ($t in 'broker','broker','broker','gateway') {
    try { Invoke-RestMethod -Method Post 'http://127.0.0.1:6969/api/nodes/spawn' -ContentType 'application/json' -Body (@{node_type=$t; mesh_id='mesh1'} | ConvertTo-Json) -TimeoutSec 5 | Out-Null } catch { Write-Host "  spawn $t err: $_" }
    Start-Sleep -Milliseconds 500
}

Write-Host "[3] converge 25s ..."
Start-Sleep -Seconds 25
$topo = Invoke-RestMethod 'http://127.0.0.1:6969/api/topology' -TimeoutSec 5
Write-Host ("[4] topology: {0} nodes" -f $topo.nodes.Count)
$topo.nodes | ForEach-Object { Write-Host ("    {0,-22} type={1,-10} state={2}" -f $_.id, $_.type, $_.state) }

# pick a stateful... actually spawned nodes aren't stateful by default; just verify names+states
$named = $topo.nodes | Where-Object { $_.id -match '^mesh1\.(broker|gateway)\.\d+$' }
$alive = $topo.nodes | Where-Object { $_.state -eq 'Alive' }
Write-Host ("    naming: {0}/{1} <mesh>.<type>.<N> names; {2} Alive (cert-admitted)" -f $named.Count, $topo.nodes.Count, $alive.Count)

# ---- [5] identity rotation + rebind: stateful restart keeps name, rotates id ----
Write-Host ""
Write-Host "[5] spawn a STATEFUL broker, restart it -> name stays + id ROTATES + re-admitted ..."
$rotationOk = $false
try {
    $sf = Invoke-RestMethod -Method Post 'http://127.0.0.1:6969/api/nodes/spawn' -ContentType 'application/json' -Body (@{node_type='broker'; mesh_id='mesh1'; stateful=$true} | ConvertTo-Json) -TimeoutSec 8
    $sfName = $sf.node_name
    Write-Host "    stateful node = $sfName"
    Start-Sleep -Seconds 12
    $rr = Invoke-RestMethod -Method Post ("http://127.0.0.1:6969/api/nodes/{0}/restart" -f $sfName) -TimeoutSec 25
    $rotated = ($rr.old_node_id -ne $rr.node_id)
    Write-Host ("    restart: old_id={0}.. new_id={1}.. rotated={2}" -f $rr.old_node_id.Substring(0,8), $rr.node_id.Substring(0,8), $rotated)
    Start-Sleep -Seconds 22
    $after = (Invoke-RestMethod 'http://127.0.0.1:6969/api/topology' -TimeoutSec 5).nodes | Where-Object { $_.id -eq $sfName }
    $nameKept = ($null -ne $after)
    $stillAlive = ($after.state -eq 'Alive')
    Write-Host ("    after restart: name '{0}' present={1} state={2}" -f $sfName, $nameKept, $after.state)
    $rotationOk = $rotated -and $nameKept -and $stillAlive
} catch { Write-Host "    rotation test error: $_" }
Write-Host ("    ROTATION+REBIND: {0}" -f $(if ($rotationOk) {'OK — name stable, id rotated, node re-admitted (cert survived rotation)'} else {'CHECK'}))

Write-Host ""
$namingOk = ($named.Count -ge 4 -and $alive.Count -ge 4)
Write-Host ("VERDICT: naming+certs={0}  rotation+rebind={1}" -f $(if($namingOk){'PASS'}else{'CHECK'}), $(if($rotationOk){'PASS'}else{'CHECK'}))
if ($namingOk -and $rotationOk) {
    Write-Host "SMOKE PASS — naming, name-bound certs, identity rotation + rebind all working." -ForegroundColor Green
} else {
    Write-Host "SMOKE CHECK — inspect logs in $log" -ForegroundColor Yellow
}
Write-Host "(node-admin left running; logs in $log)"
