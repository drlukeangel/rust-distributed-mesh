import urllib.request, json, sys
from datetime import datetime, timezone
port = sys.argv[1] if len(sys.argv) > 1 else "19090"
base = f"http://127.0.0.1:{port}"
try:
    topo = json.loads(urllib.request.urlopen(f"{base}/api/topology", timeout=5).read())
    chaos = json.loads(urllib.request.urlopen(f"{base}/api/chaos/state", timeout=5).read())
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    nodes = topo.get("nodes", [])
    print(f"{ts}  nodes={len(nodes)} chaos_events={chaos.get('total_events',0)} running={chaos.get('running',False)}")
    groups = {}
    for n in nodes:
        groups.setdefault(n.get("type","?"), []).append(n)
    for t in sorted(groups):
        g = groups[t]
        r = [float(n.get("ram_used",0)) for n in g]
        c = [float(n.get("cpu_used",0)) for n in g]
        if len(g) == 1:
            print(f"  - {t:10s} (x1) | RAM {r[0]:.3f} GB | CPU {c[0]:.3f} cores")
        else:
            print(f"  - {t:10s} (x{len(g)}) | RAM {min(r):.3f}/{sum(r)/len(r):.3f}/{max(r):.3f} GB | CPU {min(c):.3f}/{sum(c)/len(c):.3f}/{max(c):.3f} cores")
except Exception as e:
    print(f"status error: {e}")
