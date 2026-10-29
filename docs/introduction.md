# RDM: rust-distributed-mesh

RDM is the generic Mesh product: a fabric of meshes of nodes on one iroh network. Node-admins run
the control plane (topology on iroh-gossip, Node RPC, Builds, deployment providers); an application
such as rafka-v2 pins this repository and puts its own node kinds on it.

- **Getting started.** [Getting started](getting-started.md) walks from a clean checkout to a
  running two-mesh estate, one application op, and one Build.
- **Glossary.** [Glossary](glossary.md) defines every RDM term and links each to the architecture
  page that owns it.
- **Architecture.** The pages under Architecture and Workflows are the contract: code conforms to
  the page. Start at [Fabric, mesh and node lifecycles](architecture/fabric-mesh-node-lifecycles.md),
  the index of every page and workflow.
- **Building an application.** [Building an application on RDM](eng/consumer-guide.md) and
  [Your own ops on Node RPC](eng/consumer-guide-ops.md).
- **API.** The [API documentation](reference/api.md) is rustdoc, built beside this guide.

This site is built locally from the repository with `scripts/docs-site.sh`.
