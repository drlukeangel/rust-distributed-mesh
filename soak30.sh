#!/bin/bash
# 30-minute chaos soak. Every 5 min: per-node CPU/RAM (mesh-reported, cores/GB) for
# each live node on both consoles, plus OS proc RSS + health counters. Chaos stays on;
# a refill batch is spawned each mark so the fleet doesn't drain to zero.
LOG=/tmp/soak30.log
: > "$LOG"
C1=35a39cda4d988ee577076b71a8390543824faa83373f142ef17ce86ec0a04d8b

snap() {
  {
    echo ""
    echo "================= T+${1} min  $(date -u +%H:%M:%S) ================="
    for port in 19100 19101; do
      con=$([ "$port" = "19100" ] && echo "c1/mesh1" || echo "c2/mesh2")
      curl -s "http://127.0.0.1:$port/api/topology" 2>/dev/null | python -c "
import sys,json
try: d=json.load(sys.stdin)
except: print('  [$con] (no response)'); sys.exit()
ns=sorted(d['nodes'],key=lambda x:x['id'])
print('  [$con] %d nodes'%len(ns))
for n in ns:
    print('    %-26s %-9s cpu=%5.2f/%-4.1f cores  ram=%5.2f/%-4.1f GB' % (
        n['id'], str(n.get('state')), n.get('cpu_used') or 0, n.get('cpu_budget') or 0,
        n.get('ram_used') or 0, n.get('ram_budget') or 0))
"
    done
    powershell.exe -NoProfile -Command "\$p=Get-Process | ? {\$_.ProcessName -like 'rafka*'}; '  OS: {0} procs, total RSS {1:N0} MB, {2:N1} MB/proc' -f \$p.Count, ((\$p|Measure-Object WorkingSet64 -Sum).Sum/1MB), ((\$p|Measure-Object WorkingSet64 -Average).Average/1MB)" 2>/dev/null
    echo "  health: chaos_kills=$(grep -aci 'chaos killed' /tmp/f1.log) node.evicted=$(grep -aci 'node evicted (terminal state)' /tmp/f1.log) panics=$(grep -aciE 'thread .* panicked|FATAL' /tmp/f1.log /tmp/f2.log) tombstone_refs=$(grep -aci tombstone /tmp/f1.log /tmp/f2.log)"
  } >> "$LOG"
}

refill() {
  # spawn a small batch into each mesh to offset chaos kills (ghost-fix makes this safe)
  for t in broker gateway compute; do
    curl -s -X POST http://127.0.0.1:19100/api/nodes/spawn -H 'Content-Type: application/json' -d "{\"node_type\":\"$t\",\"mesh_id\":\"mesh1\"}" >/dev/null 2>&1
  done
  for t in broker registry; do
    curl -s -X POST http://127.0.0.1:19101/api/nodes/spawn -H 'Content-Type: application/json' -d "{\"node_type\":\"$t\",\"mesh_id\":\"mesh2\"}" >/dev/null 2>&1
  done
}

# ensure chaos is on
curl -s -X POST http://127.0.0.1:19100/api/chaos/start >/dev/null 2>&1
refill; sleep 18   # let the first batch converge before T+0
for m in 0 5 10 15 20 25 30; do
  snap "$m"
  if [ "$m" -lt 30 ]; then refill; sleep 282; fi   # 18s refill-converge + 282s = 300s
done
echo "" >> "$LOG"
echo "===== SOAK COMPLETE $(date -u +%H:%M:%S) =====" >> "$LOG"
