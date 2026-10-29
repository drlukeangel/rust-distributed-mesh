//! What the wire, frame and status pages once carried, now on the types themselves (Q262): the check
//! that every variant's doc comment states it, read from the source with `syn`.
//!
//! ```text
//! a Node RPC body    each `impl NodeProtocol`'s request and reply enums, and every workspace enum
//!                    their fields carry: each serialized variant's last doc paragraph is
//!                    "Position <n> on the wire." (postcard encodes a variant by its declaration index)
//! the gossip Frame   `Frame` (rdm-transport topology.rs): "Schema index <n>, never a wire kind." (its
//!                    data is selected by the CloudEvents type, with no kind discriminant); the enums its
//!                    fields carry take positions as a body's do
//! a status           every `pub enum *Status` / `*State`: "Serialized as `<serde name>`." when it
//!                    derives `Serialize`, and "`<method>()` is `<label>`." for each label its own
//!                    `&'static str` methods give the variant
//! the op index       every op is a row of its crate's op table (`| op | name | protocol | request |
//!                    reply |`): `rdm_core::protocol` for RDM's, `rdm_test`'s front page for the testkit's
//! ```
//!
//! One enum in more than one set gets one paragraph, its parts joined with `; `. [`findings`] names
//! every variant whose paragraph differs from the one its source implies, with the paragraph it owes.

use crate::render;
use crate::source::{self, Decl, DeclItem, Workspace};
use std::collections::BTreeMap;

/// The header of an op table in a crate's docs.
pub const OP_TABLE: &str = "| op | name | protocol | request | reply |";

const FRAME_CRATE: &str = "rdm-transport";
const FRAME_FILE: &str = "crates/rdm-transport/src/topology.rs";
const FRAME: &str = "Frame";

/// One Node RPC op as its `impl NodeProtocol` declares it.
struct Op<'w> {
    code: String,
    name: String,
    family: String,
    site: String,
    krate: &'w source::Crate,
    request: Decl<'w>,
    reply: Decl<'w>,
}

/// Every `impl NodeProtocol` in the workspace. A body type that does not resolve to one workspace
/// struct or enum is refused by name.
fn ops(ws: &Workspace) -> anyhow::Result<Vec<Op<'_>>> {
    let mut ops = Vec::new();
    for krate in &ws.crates {
        for file in &krate.files {
            for (module, item) in source::items_in(&file.syntax.items, &file.module) {
                let syn::Item::Impl(imp) = item else { continue };
                let Some((_, tr, _)) = &imp.trait_ else { continue };
                if tr.segments.last().is_none_or(|s| s.ident != "NodeProtocol") {
                    continue;
                }
                let family = source::text(&*imp.self_ty);
                let site = format!("{} impl NodeProtocol for {family}", file.path);
                let (mut consts, mut types) = (BTreeMap::new(), BTreeMap::new());
                for ii in &imp.items {
                    match ii {
                        syn::ImplItem::Const(c) => {
                            consts.insert(c.ident.to_string(), source::text(&c.expr));
                        }
                        syn::ImplItem::Type(t) => {
                            types.insert(t.ident.to_string(), &t.ty);
                        }
                        _ => {}
                    }
                }
                let code = consts.get("OP").cloned().ok_or_else(|| anyhow::anyhow!("{site} sets no OP"))?;
                let raw = consts.get("NAME").cloned().ok_or_else(|| anyhow::anyhow!("{site} sets no NAME"))?;
                let name = syn::parse_str::<syn::LitStr>(&raw).map(|l| l.value()).unwrap_or(raw);
                let body = |which: &str| -> anyhow::Result<Decl<'_>> {
                    let ty = types.get(which).ok_or_else(|| anyhow::anyhow!("{site} sets no type {which}"))?;
                    let found = match ty {
                        syn::Type::Path(p) => ws.resolve(krate, file, &module, &p.path),
                        _ => None,
                    };
                    found.ok_or_else(|| anyhow::anyhow!("{site}: type {which} = {} names no single workspace struct or enum", source::text(*ty)))
                };
                let (request, reply) = (body("Request")?, body("Reply")?);
                ops.push(Op { code, name, family, site, krate, request, reply });
            }
        }
    }
    if ops.is_empty() {
        anyhow::bail!("no impl NodeProtocol in the workspace");
    }
    Ok(ops)
}

/// One part of a variant's paragraph.
enum Part {
    Position(usize),
    SchemaIndex(usize),
    Serde(String),
    Label(String, String),
}

/// The paragraph `parts` make: joined with `; `, the first letter upper-cased, a full stop.
fn paragraph(parts: &[Part]) -> String {
    let text = parts
        .iter()
        .map(|p| match p {
            Part::Position(n) => format!("position {n} on the wire"),
            Part::SchemaIndex(n) => format!("schema index {n}, never a wire kind"),
            Part::Serde(s) => format!("serialized as `{s}`"),
            Part::Label(f, v) => format!("`{f}()` is `{v}`"),
        })
        .collect::<Vec<_>>()
        .join("; ");
    let mut chars = text.chars();
    match chars.next() {
        Some(c) => format!("{}{}.", c.to_uppercase(), chars.as_str()),
        None => String::new(),
    }
}

/// The last paragraph of an item's doc comment, its lines trimmed and joined with a space.
fn last_paragraph(attrs: &[syn::Attribute]) -> String {
    let mut text = String::new();
    for a in attrs.iter().filter(|a| a.path().is_ident("doc")) {
        if let syn::Meta::NameValue(syn::MetaNameValue { value: syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }), .. }) = &a.meta {
            text.push_str(&s.value());
            text.push('\n');
        }
    }
    let lines: Vec<&str> = text.lines().map(str::trim).collect();
    let end = lines.iter().rposition(|l| !l.is_empty()).map_or(0, |i| i + 1);
    let start = lines[..end].iter().rposition(|l| l.is_empty()).map_or(0, |i| i + 1);
    lines[start..end].join(" ")
}

fn skipped(v: &syn::Variant) -> bool {
    source::serde_attrs(&v.attrs).iter().any(|a| a == "skip")
}

type Owed<'w> = BTreeMap<(String, String), (Decl<'w>, BTreeMap<String, Vec<Part>>)>;

fn add<'w>(owed: &mut Owed<'w>, d: &Decl<'w>, variant: String, part: Part) {
    owed.entry((d.file.path.clone(), d.name())).or_insert_with(|| (d.clone(), BTreeMap::new())).1.entry(variant).or_default().push(part);
}

/// Every serialized variant of `d` (an enum) at its declaration index: a position, or the Frame's
/// schema index.
fn positions<'w>(owed: &mut Owed<'w>, d: &Decl<'w>, frame: bool) {
    let DeclItem::Enum(e) = &d.item else { return };
    for (i, v) in e.variants.iter().enumerate().filter(|(_, v)| !skipped(v)) {
        add(owed, d, v.ident.to_string(), if frame { Part::SchemaIndex(i) } else { Part::Position(i) });
    }
}

/// Every variant whose last doc paragraph is not the one its source implies, named with its file,
/// its enum and the paragraph it owes; and every op missing from its crate's op table.
pub fn findings(ws: &Workspace) -> anyhow::Result<Vec<String>> {
    // (file, enum) -> (declaration, variant -> parts), in the order the sets are read.
    let mut owed: Owed<'_> = BTreeMap::new();
    let ops = ops(ws)?;
    let (bodies, carried) = render::closure(ws, ops.iter().flat_map(|o| [o.request.clone(), o.reply.clone()]).collect());
    let krate = ws.package(FRAME_CRATE)?;
    let frame = ws.declared_in(krate, FRAME).into_iter().find(|d| d.file.path == FRAME_FILE && matches!(d.item, DeclItem::Enum(_))).ok_or_else(|| anyhow::anyhow!("no enum {FRAME} in {FRAME_FILE}"))?;
    let (_, frame_carried) = render::closure(ws, vec![frame.clone()]);
    let mut seen = std::collections::BTreeSet::new();
    for d in bodies.iter().chain(&carried) {
        if seen.insert(d.key()) {
            positions(&mut owed, d, false);
        }
    }
    positions(&mut owed, &frame, true);
    for d in &frame_carried {
        if seen.insert(d.key()) && d.key() != frame.key() {
            positions(&mut owed, d, false);
        }
    }
    for krate in &ws.crates {
        for file in &krate.files {
            for (module, item) in source::items_in(&file.syntax.items, &file.module) {
                let syn::Item::Enum(e) = item else { continue };
                let name = e.ident.to_string();
                if !matches!(e.vis, syn::Visibility::Public(_)) || !(name.ends_with("Status") || name.ends_with("State")) {
                    continue;
                }
                let d = Decl { krate, file, module, item: DeclItem::Enum(e) };
                if source::derives(&e.attrs, "Serialize") {
                    for v in e.variants.iter().filter(|v| !skipped(v)) {
                        if let Some(s) = render::serde_name(&e.attrs, v) {
                            add(&mut owed, &d, v.ident.to_string(), Part::Serde(s));
                        }
                    }
                }
                for (variant, labels) in render::labels(&d) {
                    for l in labels {
                        let Some((f, v)) = l.split_once("() = ") else { continue };
                        let v = syn::parse_str::<syn::LitStr>(v).map(|l| l.value()).unwrap_or_default();
                        add(&mut owed, &d, variant.clone(), Part::Label(f.to_string(), v));
                    }
                }
            }
        }
    }
    let mut out = Vec::new();
    for ((file, name), (d, variants)) in &owed {
        let DeclItem::Enum(e) = &d.item else { continue };
        for v in &e.variants {
            let Some(parts) = variants.get(&v.ident.to_string()) else { continue };
            let want = paragraph(parts);
            let got = last_paragraph(&v.attrs);
            if got != want {
                out.push(format!("{file}: {name}::{} owes the last doc paragraph \"{want}\" (it ends \"{got}\")", v.ident));
            }
        }
    }
    for o in &ops {
        let row = format!("| `{}` | `{}` | [`{}`]", o.code, o.name, o.family);
        let tables: Vec<String> = o.krate.files.iter().map(|f| crate_doc(&f.syntax.attrs)).filter(|t| t.contains(OP_TABLE)).collect();
        if !tables.iter().any(|t| t.lines().any(|l| l.trim_start().starts_with(&row))) {
            out.push(format!("{}: the op is no row of {}'s op table ({OP_TABLE}); it owes a row starting {row}", o.site, o.krate.package));
        }
    }
    Ok(out)
}

/// A file's inner doc comment (`//!`), its lines joined.
fn crate_doc(attrs: &[syn::Attribute]) -> String {
    let mut text = String::new();
    for a in attrs.iter().filter(|a| a.path().is_ident("doc") && matches!(a.style, syn::AttrStyle::Inner(_))) {
        if let syn::Meta::NameValue(syn::MetaNameValue { value: syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }), .. }) = &a.meta {
            text.push_str(&s.value());
            text.push('\n');
        }
    }
    text
}
