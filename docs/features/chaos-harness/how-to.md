# chaos-harness — how-to

## Run a single primitive

```bash
# random target
rfa mesh chaos kill
# primitive: kill_node
# target:    broker-XXX
# detection: Passed { waited_ms: 0 }

# explicit target
rfa mesh chaos kill --target broker-a1b2c3d4

# restart
rfa mesh chaos restart
# old:       broker-XXX
# new:       broker-YYY
# detection: Passed { waited_ms: 0 }
```

## Pre-populate targets before chaos

```bash
# spawn one of each so primitives have targets
for t in gateway broker compute registry; do rfa mesh node add --type $t; done
sleep 5
# confirm
curl -s http://localhost:19090/api/nodes/spawned
```

## Smoke soak (CI per-PR check)

```bash
rfa mesh chaos soak --duration 5m --interval 30s --seed 42
# soak start: duration=5m interval=30s seed=42
# soak end: events=10 passed=10 failed_timeout=0 failed_assertion=0
# report: E:/tmp/rafka-chaos-soak-42.json
```

Exit code 0 = pass; non-zero = fail.

## 1-hour soak (extended PR / pre-merge)

```bash
rfa mesh chaos soak --duration 1h --interval 30s --seed 1
# ~120 events
```

## 24-hour soak (nightly CI gate)

```bash
rfa mesh chaos soak --duration 24h --interval 30s --seed $(date +%s)
# ~2880 events
# report at E:/tmp/rafka-chaos-soak-<epoch>.json
```

## Inspect soak report

```bash
cat E:/tmp/rafka-chaos-soak-42.json | python -m json.tool | head -50

# aggregate stats:
python -c "import json; r=json.load(open('E:/tmp/rafka-chaos-soak-42.json')); print(f'{r[\"event_count\"]} events / {r[\"passed\"]} pass / {r[\"failed_timeout\"]} timeout / {r[\"failed_assertion\"]} assertion')"
```

## View chaos events in Jaeger

```
http://localhost:16686/search?service=rfa&operation=rafka.chaos.primitive.executed&lookback=1h
http://localhost:16686/search?service=rfa&operation=rafka.chaos.primitive.detected&lookback=1h
```

## Replay a failing soak

(Future — not yet implemented.) `rfa mesh chaos soak --replay <report-path>` will rerun the recorded primitive sequence exactly for debugging.
