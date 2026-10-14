# Chaos kit

`crates/rafka-chaos` holds the fault backends a scenario, a test or the demo admin UI applies to
a running estate. Each answers with a typed outcome: applied (with the observation that
acknowledged it) or refused (with the reason; nothing was done).

- `process_faults`: kill, stop and continue of ONE exact process runtime, read from the runtime
  record its birth published (control domain, pid, kernel start token). A pid alone is never a
  target; a recycled pid or a foreign host is refused by name.
- `netfault`: UDP between two sets of loopback ports dropped with `iptables` (root or `sudo -n`)
  until the `Partition` is dropped. Where the host cannot, `Partition::start` names why.
- `container_faults`: Docker primitives against the exact containers of a container estate
  (`ContainerEstate` names the Fabric and its containers).

The kit depends on no product crate. `rafka-test-scenario` re-exports it, and `demo/admin-ui`'s
Chaos tab drives it. The fabric-primary is never aimed at: the UI refuses a fault or a cut that
touches it by name (`fabric-primary-is-never-a-target`).
