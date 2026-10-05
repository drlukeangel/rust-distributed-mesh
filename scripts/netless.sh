#!/bin/sh
# Run a command in a fresh network namespace with loopback only: no route
# off the host, so nothing it starts can reach n0 DNS/Pkarr or any other
# external service (i143.e4.s13 acceptance). Needs root (or `sudo`).
#   scripts/netless.sh cargo test --offline -p rafka-test-scenario --test <t>
set -e
if [ "$(id -u)" != 0 ]; then exec sudo -E env "PATH=$PATH" "$0" "$@"; fi
exec unshare --net -- sh -c '
  if command -v ip >/dev/null 2>&1; then ip link set lo up
  else python3 -c "
import socket, fcntl, struct
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
f = struct.unpack(\"16sH\", fcntl.ioctl(s, 0x8913, struct.pack(\"16sH\", b\"lo\", 0)))[1]
fcntl.ioctl(s, 0x8914, struct.pack(\"16sH\", b\"lo\", f | 1))"
  fi
  if getent hosts iroh.link >/dev/null 2>&1 && curl -s -m 3 -o /dev/null https://iroh.link 2>/dev/null; then
    echo "netless: iroh.link is reachable; the namespace is not isolated" >&2; exit 3
  fi
  exec "$@"' netless "$@"
