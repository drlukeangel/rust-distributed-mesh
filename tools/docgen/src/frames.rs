//! frames.md: the gossip `Frame` enum (rafka-mesh-transport membership.rs), variants and fields in
//! declaration order (their postcard position), then every workspace type its fields carry.

use crate::render::{self, Columns};
use crate::source::Workspace;
use std::collections::BTreeSet;

/// The page title.
pub const TITLE: &str = "Gossip frames";

const CRATE: &str = "rdm-transport";
const FILE: &str = "crates/rdm-transport/src/membership.rs";
const NAME: &str = "Frame";

/// Render the page. The `Frame` enum missing from its file is refused by name.
pub fn page(ws: &Workspace) -> anyhow::Result<String> {
    let krate = ws.package(CRATE)?;
    let frame = ws.declared_in(krate, NAME).into_iter().find(|d| d.file.path == FILE && matches!(d.item, crate::source::DeclItem::Enum(_))).ok_or_else(|| anyhow::anyhow!("no enum {NAME} in {FILE}"))?;
    let (roots, carried) = render::closure(ws, vec![frame]);
    let linked: BTreeSet<String> = roots.iter().chain(&carried).map(|d| d.anchor()).collect();
    let cols = Columns { serde_name: false };
    let mut out = format!("{}\n# {TITLE}\n\n", crate::MARKER);
    out.push_str(&format!("The gossip `{NAME}` enum (`{FILE}`): what travels on a mesh channel or the backbone, one postcard frame. The complete gossip message is postcard `(Vec<(String, String)>, Vec<u8>)`: standard CloudEvents metadata followed by opaque data selected by its literal type. The metadata names are `specversion`, `id`, `source`, `type`, `subject`, optional `sequence`, `time`, and `datacontenttype`; source names the original author birth. A frame below describes domain data, in declaration order, without an outer kind discriminant on the wire. The numeric column is an internal schema index, never a wire kind. In-process carriers marked serde skip are omitted. Members publication identity in data is `(fabric_id, node_id, incarnation, counter)`; the Rust frame retains the counter alongside its authored context. A field with a serde `with` module travels in that module's wire shape, so its Rust type is not followed. The workspace types the other fields carry follow, sorted by name.\n\n"));
    for d in &roots {
        render::type_section(&mut out, ws, d, cols, &linked);
    }
    out.push_str("## Types the frames carry\n\nEvery workspace struct and enum a frame field names, followed through their own fields, sorted by name.\n\n");
    for d in &carried {
        render::type_section(&mut out, ws, d, cols, &linked);
    }
    Ok(out)
}
