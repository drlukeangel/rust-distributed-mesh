//! Markdown rendering shared by the pages: cell escaping, the string labels an enum's methods give
//! its variants, and one table per type with variants and fields in declaration order.

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

/// A doc line for a cell: rustdoc intra-doc links (``[`Self::NodeDrained`]``, `[Frame]`) become
/// their inner text, since the page has no target for them, then [`cell`] escaping. A bracket
/// inside inline code, or one that opens a real link (`](` or `][`), is kept.
pub fn doc(text: &str) -> String {
    let text = decite(text);
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    let (mut i, mut in_code) = (0, false);
    while i < chars.len() {
        let c = chars[i];
        if c == '`' {
            in_code = !in_code;
        }
        if c == '[' && !in_code {
            // The matching `]`, skipping any backticked span inside the brackets.
            let (mut j, mut code) = (i + 1, false);
            while j < chars.len() && (chars[j] != ']' || code) {
                if chars[j] == '`' {
                    code = !code;
                }
                j += 1;
            }
            if j < chars.len() {
                match chars.get(j + 1) {
                    // A real link: `[text](target)` or `[text][label]`, kept whole.
                    Some(&open @ ('(' | '[')) => {
                        let close = if open == '(' { ')' } else { ']' };
                        let end = chars[j + 1..].iter().position(|&c| c == close).map_or(chars.len(), |k| j + 1 + k + 1);
                        out.extend(&chars[i..end]);
                        i = end;
                    }
                    _ => {
                        out.extend(&chars[i + 1..j]);
                        i = j + 1;
                    }
                }
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    cell(&out)
}

/// A doc line with its citation forms written out. rafka-v2's doc gate reads `<path>.<ext>:<line>`,
/// a bare `:<line>` and `<path>.<ext>::<symbol>` as citations into rafka-v2's own tree, and an RDM
/// path never resolves there. So `x.rs:12` is written `x.rs line 12`, a bare `:12` (after a
/// character that is not a word character, `/` or `.`) is written `line 12`, and `x.rs::name` is
/// written `x.rs :: name`. A colon between digits (`127.0.0.1:41000`, `1:2`) and a Rust path
/// (`crate::wire`) are not citations and stay as they are.
pub fn decite(text: &str) -> String {
    const EXTS: [&str; 5] = ["rs", "json", "yaml", "sh", "py"];
    let path_char = |c: char| c.is_alphanumeric() || matches!(c, '_' | '/' | '.' | '-');
    let chars: Vec<char> = text.chars().collect();
    let ends_with_path = |end: usize| {
        EXTS.iter().any(|ext| {
            let tail: Vec<char> = format!(".{ext}").chars().collect();
            end > tail.len() && chars[end - tail.len()..end] == tail[..] && path_char(chars[end - tail.len() - 1])
        })
    };
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == ':' {
            let digit_next = chars.get(i + 1).is_some_and(char::is_ascii_digit);
            if digit_next && ends_with_path(i) {
                out.push_str(" line ");
                i += 1;
                continue;
            }
            if digit_next && (i == 0 || !(chars[i - 1].is_alphanumeric() || matches!(chars[i - 1], '_' | '/' | '.'))) {
                out.push_str("line ");
                i += 1;
                continue;
            }
            if chars.get(i + 1) == Some(&':') && chars.get(i + 2).is_some_and(|n| n.is_ascii_alphabetic() || *n == '_') && ends_with_path(i) {
                out.push_str(" :: ");
                i += 2;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

/// `text` as inline code, safe for a table cell.
pub fn code(text: &str) -> String {
    let fence = if text.contains('`') { "``" } else { "`" };
    let pad = if fence.len() > 1 { " " } else { "" };
    format!("{fence}{pad}{}{pad}{fence}", text.replace('|', "\\|"))
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

/// Which optional columns a type table carries.
#[derive(Clone, Copy)]
pub struct Columns {
    /// The serde name of each variant (statuses).
    pub serde_name: bool,
}

/// One type's section: anchor, heading, site line, doc line, container serde attributes and its
/// table, variants and fields in declaration order. A field type that names a type with a section
/// in `linked` (by anchor) links to it.
pub fn type_section(out: &mut String, ws: &Workspace, decl: &Decl, cols: Columns, linked: &BTreeSet<String>) {
    out.push_str(&format!("<a id=\"{}\"></a>\n\n### `{}`\n\n", decl.anchor(), decl.name()));
    let (attrs, shape) = match &decl.item {
        DeclItem::Enum(e) => (&e.attrs[..], format!("enum · {} variants", e.variants.len())),
        DeclItem::Struct(s) => (&s.attrs[..], match &s.fields {
            syn::Fields::Named(f) => format!("struct · {} fields", f.named.len()),
            syn::Fields::Unnamed(f) => format!("tuple struct · {} fields", f.unnamed.len()),
            syn::Fields::Unit => "unit struct".to_string(),
        }),
        DeclItem::Macro { mac, .. } => (&mac.attrs[..], format!("declared by `{}!`", source::text(&mac.mac.path))),
    };
    out.push_str(&format!("`{}` · {shape} · {}\n\n", decl.file.path, decl.krate.package));
    let doc = match &decl.item {
        DeclItem::Macro { mac, .. } => source::macro_doc_line(mac),
        _ => source::doc_line(attrs),
    };
    if !doc.is_empty() {
        out.push_str(&format!("{}\n\n", self::doc(&doc)));
    }
    let serde = source::serde_attrs(attrs);
    if !serde.is_empty() {
        out.push_str(&format!("serde: {}\n\n", serde.iter().map(|s| code(s)).collect::<Vec<_>>().join(", ")));
    }
    match &decl.item {
        DeclItem::Enum(e) => enum_table(out, ws, decl, e, cols, linked),
        DeclItem::Struct(s) if !matches!(s.fields, syn::Fields::Unit) => {
            out.push_str("| # | field | type | serde | doc |\n|---|---|---|---|---|\n");
            for (j, f) in s.fields.iter().enumerate() {
                out.push_str(&format!("| {j} | {} |\n", field_cells(ws, decl, f, j, linked).join(" | ")));
            }
            out.push('\n');
        }
        _ => {}
    }
}

fn enum_table(out: &mut String, ws: &Workspace, decl: &Decl, e: &syn::ItemEnum, cols: Columns, linked: &BTreeSet<String>) {
    let labels = labels(decl);
    let mut head = vec!["#", "variant · field"];
    if cols.serde_name {
        head.push("serde name");
    }
    head.extend(["type", "serde"]);
    if !labels.is_empty() {
        head.push("labels");
    }
    head.push("doc");
    out.push_str(&format!("| {} |\n|{}\n", head.join(" | "), "---|".repeat(head.len())));
    for (i, v) in e.variants.iter().enumerate() {
        let mut row = vec![i.to_string(), format!("**`{}`**{}", v.ident, v.discriminant.as_ref().map(|(_, d)| format!(" = {}", code(&source::text(d)))).unwrap_or_default())];
        if cols.serde_name {
            row.push(match (source::derives(&e.attrs, "Serialize"), serde_name(&e.attrs, v)) {
                (false, _) => "(not serialized)".into(),
                (true, Some(n)) => code(&n),
                (true, None) => "(rename_all not computed)".into(),
            });
        }
        row.push(String::new());
        row.push(source::serde_attrs(&v.attrs).iter().map(|s| code(s)).collect::<Vec<_>>().join(", "));
        if !labels.is_empty() {
            row.push(labels.get(&v.ident.to_string()).map(|l| l.iter().map(|x| code(x)).collect::<Vec<_>>().join("<br>")).unwrap_or_default());
        }
        row.push(doc(&source::doc_line(&v.attrs)));
        out.push_str(&format!("| {} |\n", row.join(" | ")));
        for (j, f) in v.fields.iter().enumerate() {
            let cells = field_cells(ws, decl, f, j, linked);
            let mut row = vec![format!("{i}.{j}"), cells[0].clone()];
            if cols.serde_name {
                row.push(String::new());
            }
            row.extend([cells[1].clone(), cells[2].clone()]);
            if !labels.is_empty() {
                row.push(String::new());
            }
            row.push(cells[3].clone());
            out.push_str(&format!("| {} |\n", row.join(" | ")));
        }
    }
    out.push('\n');
}

/// A field's cells: name, type (with links), serde attributes, doc line.
fn field_cells(ws: &Workspace, decl: &Decl, f: &syn::Field, j: usize, linked: &BTreeSet<String>) -> [String; 4] {
    let name = f.ident.as_ref().map(|i| format!("`{i}`")).unwrap_or_else(|| format!("`{j}`"));
    let mut ty = code(&source::text(&f.ty));
    // A field with a serde `with` module travels in that module's shape: no link to its Rust type.
    let links: Vec<String> = if with_module(f) { Vec::new() } else { carried(ws, decl, &f.ty) }
        .into_iter()
        .filter(|d| linked.contains(&d.anchor()))
        .map(|d| format!("[`{}`](#{})", d.name(), d.anchor()))
        .collect();
    if !links.is_empty() {
        ty.push_str(&format!(" → {}", links.join(", ")));
    }
    let serde = source::serde_attrs(&f.attrs).iter().map(|s| code(s)).collect::<Vec<_>>().join(", ");
    [name, ty, serde, doc(&source::doc_line(&f.attrs))]
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
        DeclItem::Enum(e) => e.variants.iter().flat_map(|v| v.fields.iter()).collect(),
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
