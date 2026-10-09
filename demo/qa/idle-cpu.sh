#!/usr/bin/env bash
# idle-cpu.sh <estate.json>: per node of the estate, `ps -o pcpu` (lifetime) and the CPU used over a 10 s window.
set -u
ROOT=$(jq -r .estate_root "$1")
declare -A N0
pids=$(grep -laP "RDM_DATA_DIR=$ROOT/" /proc/[0-9]*/environ 2>/dev/null | sed 's#/proc/##;s#/environ##')
for p in $pids; do N0[$p]=$(awk '{print $14+$15}' /proc/$p/stat); done
sleep 10
HZ=$(getconf CLK_TCK)
printf "%-18s %7s %8s %9s\n" node pid ps_pcpu cpu_10s_pct
for p in $pids; do
  [ -r /proc/$p/stat ] || continue
  name=$(tr '\0' '\n' < /proc/$p/environ | sed -n 's#^RDM_DATA_DIR=.*/##p' | sed 's/-[a-z0-9]*$//' | head -1)
  now=$(awk '{print $14+$15}' /proc/$p/stat)
  printf "%-18s %7s %8s %9.1f\n" "$name" "$p" "$(ps -o pcpu= -p $p | tr -d ' ')" "$(echo "($now-${N0[$p]})*100/($HZ*10)" | bc -l)"
done | sort
