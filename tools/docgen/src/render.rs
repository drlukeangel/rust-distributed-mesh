//! Shared by the pages and the variant-doc check: cell escaping, the string labels an enum's methods
//! give its variants, serde names, and the workspace types a declaration's fields carry.

use crate::source::{self, Decl, DeclItem, Workspace};
use std::collections::{BTreeMap, BTreeSet};

/// Text made safe for a table cell: `|` is escaped everywhere, and `<` / `>` outside inline code
/// (markdown would read `<name>` as an HTML tag).
pub fn cell(text: &str) -> String {
    let balanced = text.matches('`').count() % 2 == 0;
    let mut out = String::new();
    for (i, part) in text.split('`').enumerate() {
        if i > 0 {
            out.push('`');
        }
        let in_code = balanced && i % 2 == 1;
        for c in part.chars() {
            match c {
                '|' => out.push_str("\\|"),
                '<' if !in_code => out.push_str("&lt;"),
                '>' if !in_code => out.push_str("&gt;"),
                c => out.push(c),
            }
        }
    }
    out
}

/// `text` as inline code, safe for a table cell.
pub fn code(text: &str) -> String {
    let target = if text.contains('`') { "``" } else { "`" };
    let pad = if target.len() > 1 { " " } else { "" };
    format!("{target}{pad}{}{pad}{target}", text.replace('|', "\\|"))
}

/// The string labels the declaring crate's methods give each variant of `decl` (an enum):
/// `fn name(&self) -> &'static str { match self { Self::A => "a", .. } }`, keyed by variant, each
/// label written `name() = "a"`. Methods are visited in file order.
pub fn labels(decl: &Decl) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let name = decl.name();
    for file in &decl.krate.files {
        for item in source::items(&file.syntax.items) {
            let syn::Item::Impl(imp) = item else { continue };
            let syn::Type::Path(ty) = &*imp.self_ty else { continue };
            if ty.path.segments.last().is_none_or(|s| s.ident != name) {
                continue;
            }
            for ii in &imp.items {
                let syn::ImplItem::Fn(f) = ii else { continue };
                if f.sig.receiver().is_none() || !matches!(&f.sig.output, syn::ReturnType::Type(_, t) if source::text(t.as_ref()) == "&'static str") {
                    continue;
                }
                let Some(m) = f.block.stmts.iter().find_map(|s| match s {
                    syn::Stmt::Expr(syn::Expr::Match(m), _) if matches!(source::text(&*m.expr).as_str(), "self" | "*self") => Some(m),
                    _ => None,
                }) else {
                    continue;
                };
                for arm in &m.arms {
                    let Some(label) = literal(&arm.body) else { continue };
                    let mut variants = Vec::new();
                    pattern_variants(&arm.pat, &mut variants);
                    for v in variants {
                        out.entry(v).or_default().push(format!("{}() = \"{label}\"", f.sig.ident));
                    }
                }
            }
        }
    }
    out
}

fn literal(e: &syn::Expr) -> Option<String> {
    match e {
        syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) => Some(s.value()),
        syn::Expr::Block(b) => match b.block.stmts.as_slice() {
            [syn::Stmt::Expr(e, None)] => literal(e),
            _ => None,
        },
        _ => None,
    }
}

fn pattern_variants(p: &syn::Pat, out: &mut Vec<String>) {
    let last = |path: &syn::Path| path.segments.last().map(|s| s.ident.to_string());
    match p {
        syn::Pat::Or(o) => o.cases.iter().for_each(|c| pattern_variants(c, out)),
        syn::Pat::Path(pp) => out.extend(last(&pp.path)),
        syn::Pat::Struct(ps) => out.extend(last(&ps.path)),
        syn::Pat::TupleStruct(pt) => out.extend(last(&pt.path)),
        syn::Pat::Ident(pi) => out.push(pi.ident.to_string()),
        syn::Pat::Paren(pp) => pattern_variants(&pp.pat, out),
        syn::Pat::Reference(r) => pattern_variants(&r.pat, out),
        _ => {}
    }
}

/// How a variant is named in serde's self-describing formats: its `rename`, else the
/// container's `rename_all` applied, else the ident. `None` when the container's `rename_all`
/// is one this reader does not compute (the attribute is printed instead).
pub fn serde_name(container: &[syn::Attribute], variant: &syn::Variant) -> Option<String> {
    if let Some(r) = source::serde_value(&variant.attrs, "rename") {
        return Some(r);
    }
    let ident = variant.ident.to_string();
    let snake = || {
        let mut s = String::new();
        for (i, c) in ident.chars().enumerate() {
            if c.is_uppercase() && i > 0 {
                s.push('_');
            }
            s.push(c.to_ascii_lowercase());
        }
        s
    };
    Some(match source::serde_value(container, "rename_all").as_deref() {
        None => ident.clone(),
        Some("lowercase") => ident.to_ascii_lowercase(),
        Some("UPPERCASE") => ident.to_ascii_uppercase(),
        Some("snake_case") => snake(),
        Some("kebab-case") => snake().replace('_', "-"),
        Some("SCREAMING_SNAKE_CASE") => snake().to_ascii_uppercase(),
        Some("SCREAMING-KEBAB-CASE") => snake().replace('_', "-").to_ascii_uppercase(),
        Some("camelCase") => ident[..1].to_ascii_lowercase() + &ident[1..],
        Some("PascalCase") => ident.clone(),
        Some(_) => return None,
    })
}

/// The workspace declarations a field type written in `decl`'s file names, in mention order,
/// each once.
pub fn carried<'w>(ws: &'w Workspace, decl: &Decl<'w>, ty: &syn::Type) -> Vec<Decl<'w>> {
    let mut paths = Vec::new();
    source::type_paths(ty, &mut paths);
    let mut seen = BTreeSet::new();
    paths.iter().filter_map(|p| ws.resolve(decl.krate, decl.file, &decl.module, p)).filter(|d| seen.insert(d.key())).collect()
}

/// Whether a field travels through a `#[serde(with = ...)]` module: its wire shape is that
/// module's, not its Rust type's.
pub fn with_module(f: &syn::Field) -> bool {
    source::serde_value(&f.attrs, "with").is_some()
}

/// The fields of a struct or enum declaration (every variant's), in declaration order.
pub fn fields<'a>(decl: &'a Decl) -> Vec<&'a syn::Field> {
    match &decl.item {
        DeclItem::Enum(e) => e.variants.iter().filter(|v| !source::serde_attrs(&v.attrs).iter().any(|a| a == "skip")).flat_map(|v| v.fields.iter()).collect(),
        DeclItem::Struct(s) => s.fields.iter().collect(),
        DeclItem::Macro { .. } => Vec::new(),
    }
}

/// `roots` and every workspace type their fields carry, transitively; a field that travels
/// through a `serde(with)` module is not followed (its wire shape is the module's). Roots first in
/// the given order, then the carried types sorted by name and package.
pub fn closure<'w>(ws: &'w Workspace, roots: Vec<Decl<'w>>) -> (Vec<Decl<'w>>, Vec<Decl<'w>>) {
    let mut seen: BTreeSet<_> = roots.iter().map(|d| d.key()).collect();
    let mut queue: Vec<Decl<'w>> = roots.clone();
    let mut carried_all = Vec::new();
    while let Some(d) = queue.pop() {
        for f in fields(&d) {
            if with_module(f) {
                continue;
            }
            for c in carried(ws, &d, &f.ty) {
                if seen.insert(c.key()) {
                    queue.push(c.clone());
                    carried_all.push(c);
                }
            }
        }
    }
    carried_all.sort_by_key(|d| d.key());
    (roots, carried_all)
}
