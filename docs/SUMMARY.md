# Summary

[Introduction](introduction.md)
- [Getting started](getting-started.md)
- [Glossary](glossary.md)

# Building an application

- [Building an application on RDM](eng/consumer-guide.md)
- [Your own ops on Node RPC](eng/consumer-guide-ops.md)

# Architecture

- [Fabric, mesh and node lifecycles](architecture/fabric-mesh-node-lifecycles.md)
  - [States](architecture/lifecycles/states.md)
  - [Gossip](architecture/gossip.md)
  - [Node RPC envelope](architecture/node-rpc-envelope.md)
  - [Node RPC runtime](architecture/node-rpc.md)
  - [Query](architecture/query.md)
  - [Fabric and node hooks](architecture/fabric-node-hooks.md)
  - [Fabric certs](architecture/fabric-certs.md)
  - [Elections](architecture/fabric-node-lifecycle-elections.md)
  - [Node-admin lifecycle](architecture/node-admin-lifecycle.md)
  - [Build](architecture/build.md)
  - [Build deployment](architecture/build-deployment.md)
  - [Deployment provider](architecture/deployment-provider.md)
  - [Fabric primary](architecture/fabric-primary.md)
  - [Node identity and state](architecture/node-identity-and-state.md)
  - [Connections](architecture/connections.md)
  - [Storage](architecture/storage.md)
  - [Telemetry](architecture/telemetry.md)
  - [Chaos kit](architecture/chaos-kit.md)

# Workflows

- [Fabric create](architecture/lifecycles/fabric-create.md)
- [Fabric delete](architecture/lifecycles/fabric-delete.md)
- [Fabric recovery](architecture/lifecycles/fabric-recovery.md)
- [fabric.ca.renew and fabric.root.update](architecture/lifecycles/fabric-ca-renew.md)
- [Mesh create](architecture/lifecycles/mesh-create.md)
- [Mesh delete](architecture/lifecycles/mesh-delete.md)
- [Mesh investigation](architecture/lifecycles/mesh-investigation.md)
- [mesh.issuer.renew](architecture/lifecycles/mesh-issuer-renew.md)
- [Node create](architecture/lifecycles/node-create.md)
- [Node start](architecture/lifecycles/node-start.md)
- [Node stop](architecture/lifecycles/node-stop.md)
- [Node delete](architecture/lifecycles/node-delete.md)
- [Node replace](architecture/lifecycles/node-replace.md)
- [Node recovery](architecture/lifecycles/node-recovery.md)
- [node investigation](architecture/lifecycles/node-investigation.md)
- [node.tickle](architecture/lifecycles/node-tickle.md)
- [node.runtime.inspect](architecture/lifecycles/node-runtime-inspect.md)
- [node.mesh.join](architecture/lifecycles/node-mesh-join.md)
- [node.mesh.leave](architecture/lifecycles/node-mesh-leave.md)
- [Node status](architecture/lifecycles/node-status.md)
- [Node topology get](architecture/lifecycles/node-topology-get.md)
- [Node name update](architecture/lifecycles/node-name-update.md)
- [Node config update](architecture/lifecycles/node-config-update.md)
- [Node primary update](architecture/lifecycles/node-primary-update.md)
- [Node connections delete](architecture/lifecycles/node-connections-delete.md)
- [node.connections.admit](architecture/lifecycles/node-connections-admit.md)
- [node.cert.renew](architecture/lifecycles/node-cert-renew.md)
- [node.issuer.update](architecture/lifecycles/node-issuer-update.md)

# Running a fabric

- [Troubleshooting](eng/troubleshooting.md)
- [Fabric recovery runbook](eng/fabric-recovery-runbook.md)
- [Node storage runbook](eng/node-storage-runbook.md)
- [Provider runbook](eng/provider-runbook.md)
- [Topology storage provider](features/topology-storage-provider/overview.md)
  - [How-to](features/topology-storage-provider/how-to.md)
  - [Runbook](features/topology-storage-provider/runbook.md)
- [Telemetry patterns](eng/telemetry-patterns.md)

# Testing

- [Testing strategy](eng/testing-strategy.md)
- [Seeds](eng/testing-strategy-seed.md)
- [Test harnesses](eng/testing-harnesses.md)
- [Testing with node-admin](eng/testing-with-node-admin.md)
- [A test's fabric](eng/testing-mesh-lifecycle.md)
- [Naming conventions](eng/testing-naming-conventions.md)
- [Trace capture](eng/test-otlp-capture.md)

# Reference

- [API documentation (rustdoc)](reference/api.md)
- [Configuration](reference/rdm/config.md)
- [Spans](reference/rdm/spans.md)

# For maintainers

- [The lane scripts](eng/testing-lane-scripts.md)
- [PRD seed template](eng/templates/prd-seed-template.md)
