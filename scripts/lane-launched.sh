#!/usr/bin/env bash
# lane-launched.sh <lane> — record that the lane's seat starts now (the lead runs it as it launches the
# seat's agent), so lane-land.sh's time report tells waiting for a seat from the seat's own time.
set -uo pipefail
. "$(dirname "$0")/lane-lib.sh"
L="${1:?usage: lane-launched.sh <lane>}"
date +%s > "$(state_dir "$L")/launched" && echo "LAUNCHED lane $L at $(date +%T)"
