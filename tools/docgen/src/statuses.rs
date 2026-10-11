//! statuses.md: every `pub enum` whose name ends in `Status` or `State` in RDM's non-test source,
//! each variant with its serde name and the labels the enum's methods give it, then every
//! `pub const` string whose value is one of those names (the mesh / fabric status strings).

use crate::render::{self, Columns};
use crate::source::{self, Decl, DeclItem, Workspace};
use std::collections::BTreeSet;

/// The page title.
pub const TITLE: &str = "Statuses";

/// Every public `*Status` / `*State` enum, sorted by name and package.
fn status_enums(ws: &Workspace) -> Vec<Decl<'_>> {
    let mut out = Vec::new();
    for krate in &ws.crates {
        for file in &krate.files {
            for (module, item) in source::items_in(&file.syntax.items, &file.module) {
                if let syn::Item::Enum(e) = item {
                    let name = e.ident.to_string();
                    if matches!(e.vis, syn::Visibility::Public(_)) && (name.ends_with("Status") || name.ends_with("State")) {
                        out.push(Decl { krate, file, module, item: DeclItem::Enum(e) });
                    }
                }
            }
        }
    }
    out.sort_by_key(|d| d.key());
    out
}

/// Render the page.
pub fn page(ws: &Workspace) -> anyhow::Result<String> {
    let enums = status_enums(ws);
    if enums.is_empty() {
        anyhow::bail!("no pub enum named *Status or *State in the workspace");
    }
    let mut names = BTreeSet::new();
    for d in &enums {
        let DeclItem::Enum(e) = &d.item else { continue };
        if source::derives(&e.attrs, "Serialize") {
            names.extend(e.variants.iter().filter_map(|v| render::serde_name(&e.attrs, v)));
        }
        for labels in render::labels(d).into_values() {
            names.extend(labels.iter().filter_map(|l| l.split_once(" = ").and_then(|(_, v)| syn::parse_str::<syn::LitStr>(v).ok()).map(|l| l.value())));
        }
    }
    let mut out = format!("{}\n# {TITLE}\n\n", crate::MARKER);
    out.push_str("Every `pub enum` whose name ends in `Status` or `State` in RDM's non-test source, sorted by name. Each variant has its serde name (how a JSON view spells it; an enum that does not derive `Serialize` says so) and any label the enum's own methods give it (`fn name(&self) -> &'static str`). The status strings follow.\n\n");
    out.push_str("| enum | crate | site |\n|---|---|---|\n");
    for d in &enums {
        out.push_str(&format!("| [`{}`](#{}) | {} | `{}` |\n", d.name(), d.anchor(), d.krate.package, d.site()));
    }
    out.push('\n');
    let cols = Columns { serde_name: true };
    for d in &enums {
        render::type_section(&mut out, ws, d, cols, &BTreeSet::new());
    }
    let mut consts = Vec::new();
    for krate in &ws.crates {
        for file in &krate.files {
            for item in source::items(&file.syntax.items) {
                let syn::Item::Const(c) = item else { continue };
                let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) = &*c.expr else { continue };
                if matches!(c.vis, syn::Visibility::Public(_)) && names.contains(&s.value()) {
                    consts.push((c.ident.to_string(), s.value(), format!("{}:{}", file.path, c.ident.span().start().line), source::doc_line(&c.attrs)));
                }
            }
        }
    }
    consts.sort();
    out.push_str("## Status strings\n\nEvery `pub const` string in RDM's non-test source whose value is a serde name or label above.\n\n| const | value | where | doc |\n|---|---|---|---|\n");
    for (name, value, site, doc) in &consts {
        out.push_str(&format!("| `{name}` | {} | `{site}` | {} |\n", render::code(value), render::doc(doc)));
    }
    Ok(out)
}
