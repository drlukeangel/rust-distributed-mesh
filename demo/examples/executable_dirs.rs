//! `RDM_EXECUTABLE_DIRS`: the host directories that hold the executables a Build names
//! (build-deployment.md).
//!
//! Every node-admin reads `RDM_EXECUTABLE_DIRS` at its start, a comma-separated list of absolute
//! directories, and refuses to start by name on a relative entry or one that is not a directory; the
//! launching admin writes its own value into every node-admin it launches, so the fabric runs on one
//! list. A Build names each created path's executable by its absolute path and sha256 (a `Binding`),
//! and the launching admin hashes it again at every launch. Under the container provider each directory
//! is mounted read-only at its host path, and a Build naming an executable outside them is refused at
//! acceptance, `executable-refused`, naming `RDM_EXECUTABLE_DIRS`.
//!
//! Here a caller replaces one node's executable: it checks the executable is inside the directories the
//! fabric runs with (its own `RDM_EXECUTABLE_DIRS`, the value the estate was started with) before it
//! sends anything, hashes it, and submits the fabric's current topology with that one path's binding
//! changed. RDM realizes the replacement as `node.delete` then `node.create` at the path. A refusal is
//! printed by name.
//!
//! Build: `cargo build --example executable_dirs` in `demo/`. Run against an estate started with the
//! directories set (`RDM_EXECUTABLE_DIRS=<dir>[,<dir>...] scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_EXECUTABLE_DIRS=<dir> RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" \
//!     target/debug/examples/executable_dirs <path.name> <dir>/<executable>
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{BuildFrame, BuildId, CallEnd, Change, Fabrics, Filter, MeshNode, Nodes, PathName, Peers};
use rdm_core::binding::{sha256_file, Binding};
use std::path::{Path, PathBuf};

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

/// The directories `RDM_EXECUTABLE_DIRS` lists, each absolute, as every node-admin requires.
fn executable_dirs() -> Result<Vec<PathBuf>> {
    let value = std::env::var("RDM_EXECUTABLE_DIRS").context("set RDM_EXECUTABLE_DIRS to the directories the fabric runs with")?;
    value
        .split(',')
        .filter(|d| !d.trim().is_empty())
        .map(|d| {
            let d = PathBuf::from(d.trim());
            if d.is_absolute() && d.is_dir() {
                Ok(d)
            } else {
                Err(anyhow!("RDM_EXECUTABLE_DIRS entry {} is not an absolute directory", d.display()))
            }
        })
        .collect()
}

/// Whether `executable` lies inside one of `dirs`, compared after both are made canonical.
fn inside(executable: &Path, dirs: &[PathBuf]) -> Result<bool> {
    let exe = executable.canonicalize().with_context(|| format!("{}", executable.display()))?;
    Ok(dirs.iter().filter_map(|d| d.canonicalize().ok()).any(|d| exe.starts_with(d)))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-executable-dirs");
    let mut args = std::env::args().skip(1);
    let (Some(node), Some(executable)) = (args.next(), args.next()) else { return Err(anyhow!("usage: executable_dirs <path.name> <executable>")) };
    let node: PathName = node.parse().map_err(|e| anyhow!("{node}: {e}"))?;
    let executable = PathBuf::from(executable);
    let dirs = executable_dirs()?;
    if !inside(&executable, &dirs)? {
        return Err(anyhow!("{} is outside RDM_EXECUTABLE_DIRS {dirs:?}: the fabric's admins would refuse it (executable-refused)", executable.display()));
    }
    let sha256 = sha256_file(&executable).with_context(|| format!("hashing {}", executable.display()))?;

    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let fabric = Fabrics::new(carrier.clone()).get(&Filter::new()).await?.into_iter().next().ok_or_else(|| anyhow!("fabric.get answered no fabric"))?;
    let based_on = BuildId::parse(fabric.build_id.as_deref().ok_or_else(|| anyhow!("fabric.get names no current Build"))?).map_err(|e| anyhow!("the current Build id: {e}"))?;

    // The current topology, each path with the tags and config its row holds (a Build applies every
    // property of a spec that differs from the row), and this one path on the new executable.
    let rows = Nodes::new(carrier.clone()).get(&Filter::new()).await?;
    let mut nodes = carrier.build(&based_on).await?.nodes;
    for spec in &mut nodes {
        if let Some(row) = rows.iter().find(|r| r.name == spec.path && r.incarnation_id.is_some()) {
            spec.tags = row.tags.clone();
            spec.config = row.config.clone();
        }
    }
    let spec = nodes.iter_mut().find(|s| s.path == node).ok_or_else(|| anyhow!("the current Build names no {node}"))?;
    let image = spec.executable.as_ref().and_then(|b| b.image.clone());
    spec.executable = Some(Binding { launch_id: node.kind.name().into(), executable: executable.clone(), sha256, image });

    let stream = match carrier.create(Change::Reconcile { based_on, nodes, topology_storage_provider: None }).await {
        Ok(s) => s,
        Err(CallEnd::Rejected { reason, detail }) => return Err(anyhow!("refused {reason}: {detail}")),
        Err(e) => return Err(e.into()),
    };
    println!("replacing {node} onto {}: Build {}", executable.display(), stream.build_id().log());
    let followed = carrier.follow(stream).await?;
    match followed.frames.last() {
        Some(BuildFrame::Complete { .. }) => println!("{node} runs {}", executable.display()),
        other => return Err(anyhow!("the Build ended {other:?}")),
    }
    Ok(())
}
