//! wire.md: every Node RPC op — each `impl NodeProtocol for T` in RDM's non-test source — sorted by
//! op code, its constants as written, its request and reply bodies with variants and fields in
//! declaration order (their postcard position), then every workspace type those bodies carry. The
//! page names files, never source lines, so it changes only when a wire body changes.

use crate::render::{self, Columns};
use crate::source::{self, Decl, Workspace};
use std::collections::{BTreeMap, BTreeSet};

/// The page title.
pub const TITLE: &str = "Node RPC wire bodies";

/// The `NodeProtocol` constants each op section lists, in this order.
const CONSTS: [&str; 8] = ["OP", "NAME", "FORWARDABLE", "SERVED_WHILE_CONNECTIONS_DELETING", "REQUEST_VARIANTS", "REPLY_VARIANTS", "MAX_REQUEST_FRAME_BYTES", "MAX_REPLY_FRAME_BYTES"];

struct Op<'w> {
    code: u64,
    family: String,
    file: String,
    package: String,
    consts: BTreeMap<String, String>,
    request: Decl<'w>,
    reply: Decl<'w>,
}

impl Op<'_> {
    fn name(&self) -> String {
        let raw = self.consts.get("NAME").cloned().unwrap_or_default();
        syn::parse_str::<syn::LitStr>(&raw).map(|l| l.value()).unwrap_or(raw)
    }
}

/// Every `impl NodeProtocol` in the workspace, sorted by op code. A body type that does not
/// resolve to one workspace struct or enum is refused by name: never a partial page.
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
                let site = format!("{}:{}", file.path, imp.impl_token.span.start().line);
                let (mut consts, mut types) = (BTreeMap::new(), BTreeMap::new());
                for ii in &imp.items {
                    match ii {
                        syn::ImplItem::Const(c) => {
                            consts.insert(c.ident.to_string(), source::text(&c.expr));
                        }
                        syn::ImplItem::Type(t) => {
                            types.insert(t.ident.to_string(), &t.ty);
                        }
                        syn::ImplItem::Macro(m) if m.mac.path.segments.last().is_some_and(|s| s.ident == "protocol_variants") => {
                            let (requests, replies) = helper_counts(m.mac.tokens.clone()).ok_or_else(|| anyhow::anyhow!("{site}: impl NodeProtocol for {family}: its protocol_variants! lists no request and reply"))?;
                            consts.entry("REQUEST_VARIANTS".to_string()).or_insert(requests.to_string());
                            consts.entry("REPLY_VARIANTS".to_string()).or_insert(replies.to_string());
                        }
                        _ => {}
                    }
                }
                let op_text = consts.get("OP").ok_or_else(|| anyhow::anyhow!("{site}: impl NodeProtocol for {family} sets no OP"))?;
                let code = parse_int(op_text).ok_or_else(|| anyhow::anyhow!("{site}: impl NodeProtocol for {family}: OP = {op_text} is not an integer literal"))?;
                let body = |which: &str| -> anyhow::Result<Decl<'_>> {
                    let ty = types.get(which).ok_or_else(|| anyhow::anyhow!("{site}: impl NodeProtocol for {family} sets no type {which}"))?;
                    let found = match ty {
                        syn::Type::Path(p) => ws.resolve(krate, file, &module, &p.path),
                        _ => None,
                    };
                    found.ok_or_else(|| anyhow::anyhow!("{site}: impl NodeProtocol for {family}: type {which} = {} names no single workspace struct or enum", source::text(*ty)))
                };
                let (request, reply) = (body("Request")?, body("Reply")?);
                ops.push(Op { code, family, file: file.path.clone(), package: krate.package.clone(), consts, request, reply });
            }
        }
    }
    if ops.is_empty() {
        anyhow::bail!("no impl NodeProtocol in the workspace");
    }
    ops.sort_by(|a, b| (a.code, &a.family).cmp(&(b.code, &b.family)));
    Ok(ops)
}

/// An integer literal as written (`0x1B`, `27`, `0x1B_u8`).
fn parse_int(text: &str) -> Option<u64> {
    syn::parse_str::<syn::LitInt>(text).ok()?.base10_parse().ok()
}

/// The variant counts a `rdm_core::protocol_variants!` invocation writes: its `request` list's
/// length and its `reply` list's, `@reserved` counting the six reserved variants.
fn helper_counts(tokens: proc_macro2::TokenStream) -> Option<(usize, usize)> {
    use proc_macro2::{Delimiter, TokenTree};
    let count = |list: proc_macro2::TokenStream| {
        let mut entries: Vec<Vec<TokenTree>> = vec![Vec::new()];
        for t in list {
            match &t {
                TokenTree::Punct(p) if p.as_char() == ',' => entries.push(Vec::new()),
                _ => entries.last_mut().expect("one entry is open").push(t),
            }
        }
        entries.iter().filter_map(|e| e.first()).map(|t| if matches!(t, TokenTree::Punct(p) if p.as_char() == '@') { 6 } else { 1 }).sum::<usize>()
    };
    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
    let list = |word: &str| {
        tokens.windows(3).find_map(|w| match w {
            [TokenTree::Ident(i), TokenTree::Ident(_), TokenTree::Group(g)] if i == word && g.delimiter() == Delimiter::Brace => Some(count(g.stream())),
            _ => None,
        })
    };
    Some((list("request")?, list("reply")?))
}

/// The `NodeProtocol` trait's constant defaults, as written.
fn defaults(ws: &Workspace) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for krate in &ws.crates {
        for file in &krate.files {
            for item in source::items(&file.syntax.items) {
                let syn::Item::Trait(t) = item else { continue };
                if t.ident != "NodeProtocol" {
                    continue;
                }
                for ti in &t.items {
                    if let syn::TraitItem::Const(syn::TraitItemConst { ident, default: Some((_, e)), .. }) = ti {
                        out.insert(ident.to_string(), source::text(e));
                    }
                }
            }
        }
    }
    out
}

/// Render the page.
pub fn page(ws: &Workspace) -> anyhow::Result<String> {
    let ops = ops(ws)?;
    let defaults = defaults(ws);
    let roots: Vec<Decl> = ops.iter().flat_map(|o| [o.request.clone(), o.reply.clone()]).collect();
    let (_, carried) = render::closure(ws, roots);
    let linked: BTreeSet<String> = ops.iter().flat_map(|o| [o.request.anchor(), o.reply.anchor()]).chain(carried.iter().map(|d| d.anchor())).collect();
    let cols = Columns { serde_name: false };
    let mut out = format!("{}\n# {TITLE}\n\n", crate::MARKER);
    out.push_str("Every Node RPC op: each `impl NodeProtocol` in RDM's non-test source, sorted by op code. A body's variants and fields are listed in declaration order, which is their postcard position. The workspace types the bodies carry follow, sorted by name.\n\n");
    out.push_str("| op | name | family | request | reply |\n|---|---|---|---|---|\n");
    for o in &ops {
        out.push_str(&format!(
            "| {} | {} | {} | [`{}`](#{}) | [`{}`](#{}) |\n",
            render::code(o.consts.get("OP").map(String::as_str).unwrap_or_default()),
            render::code(&o.name()),
            render::code(&o.family),
            o.request.name(),
            o.request.anchor(),
            o.reply.name(),
            o.reply.anchor()
        ));
    }
    out.push('\n');
    for o in &ops {
        out.push_str(&format!("## {} `{}`\n\n`impl NodeProtocol for {}` · `{}` · {}\n\n", o.consts.get("OP").map(String::as_str).unwrap_or_default(), o.name(), o.family, o.file, o.package));
        out.push_str("| const | value |\n|---|---|\n");
        for c in CONSTS {
            let value = match (o.consts.get(c), defaults.get(c)) {
                (Some(v), _) => render::code(v),
                (None, Some(d)) => format!("trait default {}", render::code(d)),
                (None, None) => "not set".into(),
            };
            out.push_str(&format!("| `{c}` | {value} |\n"));
        }
        out.push('\n');
        render::type_section(&mut out, ws, &o.request, cols, &linked);
        render::type_section(&mut out, ws, &o.reply, cols, &linked);
    }
    out.push_str("## Types the bodies carry\n\nEvery workspace struct and enum a request or reply field names, followed through their own fields, sorted by name. A type outside the workspace (`String`, `u64`, `BTreeMap`) is not listed.\n\n");
    for d in &carried {
        render::type_section(&mut out, ws, d, cols, &linked);
    }
    Ok(out)
}
