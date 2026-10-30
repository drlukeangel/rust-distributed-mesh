//! The RDM workspace as parsed source: every member's `src/` files, `#[cfg(test)]` and `#[test]`
//! items removed (rafka-mesh-audit's `test_only`), plus the declaration and `use` lookups the
//! readers share. No reader sees test code.

use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};
use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The parsed workspace.
pub struct Workspace {
    /// Every member, sorted by package name.
    pub crates: Vec<Crate>,
}

/// One workspace member.
pub struct Crate {
    /// Its package name (`rafka-node-rpc-contract`).
    pub package: String,
    /// Its library ident as other crates name it (`rdm_core`).
    pub ident: String,
    /// Its `src/` files, sorted by path.
    pub files: Vec<SourceFile>,
    /// Module names the crate declares (file stems and inline `mod`s): a path starting with one
    /// of them is the crate's own.
    pub modules: BTreeSet<String>,
}

/// One parsed source file with its test items removed.
pub struct SourceFile {
    /// Its path relative to the workspace root, `/`-separated.
    pub path: String,
    /// Its module path within its crate (`src/role.rs` is `["role"]`, `src/lib.rs` is `[]`).
    pub module: Vec<String>,
    /// The parsed file.
    pub syntax: syn::File,
    /// What its `use` items bring into scope, per module (a child module does not inherit its
    /// parent's): module -> name -> the full path it names, beginning with a crate ident (`crate`,
    /// `self` and `super` resolved to this crate).
    pub uses: BTreeMap<Vec<String>, BTreeMap<String, Vec<String>>>,
    /// The full paths it glob-imports (`use x::*`), per module.
    pub globs: BTreeMap<Vec<String>, Vec<Vec<String>>>,
}

impl SourceFile {
    /// The full path `name` is imported as in `module`.
    pub fn use_of(&self, module: &[String], name: &str) -> Option<&Vec<String>> {
        self.uses.get(module)?.get(name)
    }
}

/// A struct, enum or macro-declared type found in the workspace.
#[derive(Clone)]
pub struct Decl<'w> {
    /// The crate that declares it.
    pub krate: &'w Crate,
    /// The file that declares it.
    pub file: &'w SourceFile,
    /// Its module path within the crate (the file's, then any inline `mod`s).
    pub module: Vec<String>,
    /// The declaration.
    pub item: DeclItem<'w>,
}

/// What a [`Decl`] is.
#[derive(Clone)]
pub enum DeclItem<'w> {
    /// `enum`.
    Enum(&'w syn::ItemEnum),
    /// `struct`.
    Struct(&'w syn::ItemStruct),
    /// A type an item macro (`product_id!(... NodeId ...)`) declares; syn sees no item for it.
    Macro {
        /// The declared name, as the invocation's tokens hold it (its span is the source line).
        name: syn::Ident,
        /// The invocation.
        mac: &'w syn::ItemMacro,
    },
}

impl<'w> Decl<'w> {
    /// The declared name.
    pub fn name(&self) -> String {
        match &self.item {
            DeclItem::Enum(e) => e.ident.to_string(),
            DeclItem::Struct(s) => s.ident.to_string(),
            DeclItem::Macro { name, .. } => name.to_string(),
        }
    }

    /// The line the name is declared on.
    pub fn line(&self) -> usize {
        match &self.item {
            DeclItem::Enum(e) => e.ident.span().start().line,
            DeclItem::Struct(s) => s.ident.span().start().line,
            DeclItem::Macro { name, .. } => name.span().start().line,
        }
    }

    /// The stable anchor of this declaration on a page: crate, module path, name.
    pub fn anchor(&self) -> String {
        let mut parts = vec!["t".to_string(), self.krate.ident.clone()];
        parts.extend(self.module.iter().cloned());
        parts.push(self.name());
        parts.join("-")
    }

    /// One key per declaration, for dedup and ordering: name, package, module, site.
    pub fn key(&self) -> (String, String, Vec<String>, String, usize) {
        (self.name(), self.krate.package.clone(), self.module.clone(), self.file.path.clone(), self.line())
    }
}

impl Workspace {
    /// Parse every workspace member's `src/` under `root` (members read from the root manifest).
    pub fn load(root: &Path) -> anyhow::Result<Self> {
        let mut crates = Vec::new();
        for member in rafka_mesh_audit::legacy::workspace_members(root) {
            let dir = root.join(&member);
            let manifest_path = dir.join("Cargo.toml");
            let manifest = std::fs::read_to_string(&manifest_path).map_err(|e| anyhow::anyhow!("reading {}: {e}", manifest_path.display()))?;
            let (package, lib) = manifest_names(&manifest).ok_or_else(|| anyhow::anyhow!("{} names no [package] name", manifest_path.display()))?;
            let ident = lib.unwrap_or_else(|| package.replace('-', "_"));
            let mut files = Vec::new();
            collect(root, &dir.join("src"), &ident, &mut files)?;
            files.sort_by(|a, b| a.path.cmp(&b.path));
            let mut modules = BTreeSet::new();
            for f in &files {
                if let Some(stem) = Path::new(&f.path).file_stem().and_then(|s| s.to_str()) {
                    modules.insert(stem.to_string());
                }
                for item in items(&f.syntax.items) {
                    if let syn::Item::Mod(m) = item {
                        modules.insert(m.ident.to_string());
                    }
                }
            }
            crates.push(Crate { package, ident, files, modules });
        }
        crates.sort_by(|a, b| a.package.cmp(&b.package));
        Ok(Self { crates })
    }

    /// The member whose package name is `package`.
    pub fn package(&self, package: &str) -> anyhow::Result<&Crate> {
        self.crates.iter().find(|c| c.package == package).ok_or_else(|| anyhow::anyhow!("no workspace member named {package}"))
    }

    fn by_ident(&self, ident: &str) -> Option<&Crate> {
        self.crates.iter().find(|c| c.ident == ident)
    }

    /// Every struct, enum and macro-declared type named `name` in `krate`.
    pub fn declared_in<'w>(&'w self, krate: &'w Crate, name: &str) -> Vec<Decl<'w>> {
        krate.files.iter().flat_map(|f| declared_in_file(krate, f, name)).collect()
    }

    /// The declaration a type path names when written in `file` of `krate`, inside module
    /// `module` (the referring declaration's): the module's own item, then the file's `use`s, then
    /// glob imports, then a name declared once in the crate. A path through a re-export is followed
    /// to the declaring crate. A type outside the workspace (`String`, `u64`, `BTreeMap`), or a
    /// name the workspace declares ambiguously, resolves to nothing.
    pub fn resolve<'w>(&'w self, krate: &'w Crate, file: &'w SourceFile, module: &[String], path: &syn::Path) -> Option<Decl<'w>> {
        let segs: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
        let (first, name) = (segs.first()?, segs.last()?);
        if segs.len() > 1 {
            let mut full = match first.as_str() {
                "crate" => vec![krate.ident.clone()],
                "self" => std::iter::once(krate.ident.clone()).chain(module.iter().cloned()).collect(),
                "super" => std::iter::once(krate.ident.clone()).chain(module.iter().take(module.len().saturating_sub(1)).cloned()).collect(),
                _ => match file.use_of(module, first) {
                    Some(p) => self.rooted(krate, module, p),
                    None if krate.modules.contains(first) => vec![krate.ident.clone(), first.clone()],
                    None => vec![first.clone()],
                },
            };
            full.extend(segs[1..].iter().cloned());
            return self.at_path(&full, 0);
        }
        let here: Vec<Decl<'w>> = declared_in_file(krate, file, name).into_iter().filter(|d| d.module == module).collect();
        if let Some(one) = only(here) {
            return Some(one);
        }
        if let Some(p) = file.use_of(module, name) {
            return self.at_path(&self.rooted(krate, module, p), 0);
        }
        for g in file.globs.get(module).into_iter().flatten() {
            let mut full = self.rooted(krate, module, g);
            full.push(name.clone());
            if let Some(d) = self.at_path(&full, 0) {
                return Some(d);
            }
        }
        only(self.declared_in(krate, name))
    }

    /// A `use` path as a full path: one that begins with a module of `krate` (a 2018-edition
    /// relative path, `pub use role::PrimaryRole` in `lib.rs`) is the crate's, under `module`.
    fn rooted(&self, krate: &Crate, module: &[String], path: &[String]) -> Vec<String> {
        match path.first() {
            Some(first) if self.by_ident(first).is_none() && krate.modules.contains(first) => std::iter::once(krate.ident.clone()).chain(module.iter().cloned()).chain(path.iter().cloned()).collect(),
            _ => path.to_vec(),
        }
    }

    /// The declaration a full path (`[crate ident, modules.., name]`) names: the declaring module's
    /// item, else a `use` in that module re-exporting the name (followed), else the one declaration
    /// of that name in the crate.
    fn at_path<'w>(&'w self, full: &[String], depth: usize) -> Option<Decl<'w>> {
        let (krate_ident, rest) = full.split_first()?;
        let (name, module) = rest.split_last()?;
        let krate = self.by_ident(krate_ident)?;
        let declared = self.declared_in(krate, name);
        if let Some(one) = only(declared.iter().filter(|d| d.module == module).cloned().collect()) {
            return Some(one);
        }
        if depth < 6 {
            let reexport = krate.files.iter().find_map(|f| f.use_of(module, name)).map(|p| self.rooted(krate, module, p));
            if let Some(p) = reexport {
                if p.as_slice() != full {
                    return self.at_path(&p, depth + 1);
                }
            }
        }
        only(declared)
    }
}

fn declared_in_file<'w>(krate: &'w Crate, file: &'w SourceFile, name: &str) -> Vec<Decl<'w>> {
    let mut out = Vec::new();
    for (module, item) in items_in(&file.syntax.items, &file.module) {
        let decl = match item {
            syn::Item::Enum(e) if e.ident == name => Some(DeclItem::Enum(e)),
            syn::Item::Struct(s) if s.ident == name => Some(DeclItem::Struct(s)),
            syn::Item::Macro(m) if m.ident.is_none() => macro_declares(m, name).map(|ident| DeclItem::Macro { name: ident, mac: m }),
            _ => None,
        };
        out.extend(decl.map(|item| Decl { krate, file, module, item }));
    }
    out
}

/// The one element of `v`; none when it holds zero or several (an ambiguous name resolves to
/// nothing rather than to a guess).
fn only<T>(mut v: Vec<T>) -> Option<T> {
    if v.len() == 1 {
        v.pop()
    } else {
        None
    }
}

/// The ident an item macro invocation declares, when it is `name`: the invocation's first
/// top-level ident, attributes skipped (`product_id!(/// doc\n NodeId, "node id", Ord)`).
/// `macro_rules!` declares a macro, not a type.
fn macro_declares(m: &syn::ItemMacro, name: &str) -> Option<syn::Ident> {
    if m.mac.path.is_ident("macro_rules") {
        return None;
    }
    let first = m.mac.tokens.clone().into_iter().find_map(|t| match t {
        TokenTree::Ident(i) => Some(i),
        _ => None,
    })?;
    (first == name).then_some(first)
}

/// The first doc line inside an item macro invocation (`product_id!(/// Logical node identity ...)`).
pub fn macro_doc_line(m: &syn::ItemMacro) -> String {
    let tokens: Vec<TokenTree> = m.mac.tokens.clone().into_iter().collect();
    let mut text = String::new();
    for w in tokens.windows(2) {
        if let (TokenTree::Punct(p), TokenTree::Group(g)) = (&w[0], &w[1]) {
            if p.as_char() == '#' && g.delimiter() == Delimiter::Bracket {
                if let Ok(syn::Meta::NameValue(nv)) = syn::parse2::<syn::Meta>(g.stream()) {
                    if let (true, syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. })) = (nv.path.is_ident("doc"), &nv.value) {
                        text.push_str(&s.value());
                        text.push('\n');
                    }
                }
            }
        }
    }
    text.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or_default().to_string()
}

/// Every item of `items`, inline modules flattened, in source order.
pub fn items(items: &[syn::Item]) -> Vec<&syn::Item> {
    let mut out = Vec::new();
    for item in items {
        out.push(item);
        if let syn::Item::Mod(syn::ItemMod { content: Some((_, inner)), .. }) = item {
            out.extend(self::items(inner));
        }
    }
    out
}

/// Every item of `items` with the module path it sits in, starting from `module`.
pub fn items_in<'a>(items: &'a [syn::Item], module: &[String]) -> Vec<(Vec<String>, &'a syn::Item)> {
    let mut out = Vec::new();
    for item in items {
        out.push((module.to_vec(), item));
        if let syn::Item::Mod(syn::ItemMod { ident, content: Some((_, inner)), .. }) = item {
            let mut inner_module = module.to_vec();
            inner_module.push(ident.to_string());
            out.extend(items_in(inner, &inner_module));
        }
    }
    out
}

/// A file's module path from its path under `src/`.
fn file_module(src_rel: &Path) -> Vec<String> {
    let mut parts: Vec<String> = src_rel.iter().map(|c| c.to_string_lossy().into_owned()).collect();
    let Some(last) = parts.pop() else { return Vec::new() };
    if parts.first().is_some_and(|p| p == "bin") {
        return Vec::new();
    }
    let stem = last.trim_end_matches(".rs");
    if !matches!(stem, "lib" | "main" | "mod") {
        parts.push(stem.to_string());
    }
    parts
}

fn collect(root: &Path, dir: &Path, krate: &str, out: &mut Vec<SourceFile>) -> anyhow::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?.map(|e| e.map(|e| e.path())).collect::<Result<_, _>>()?;
    entries.sort();
    for p in entries {
        if p.is_dir() {
            collect(root, &p, krate, out)?;
        } else if p.extension().is_some_and(|e| e == "rs") {
            let path = p.strip_prefix(root)?.to_string_lossy().replace('\\', "/");
            let src_rel = path.split_once("/src/").map(|(_, r)| r.to_string()).unwrap_or_default();
            let module = file_module(Path::new(&src_rel));
            let text = std::fs::read_to_string(&p).map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;
            let mut syntax = syn::parse_file(&text).map_err(|e| anyhow::anyhow!("parsing {path}: {e}"))?;
            strip_tests(&mut syntax.items);
            let (uses, globs) = use_map(&syntax.items, krate, &module);
            out.push(SourceFile { path, module, syntax, uses, globs });
        }
    }
    Ok(())
}

/// The `[package] name` and `[lib] name` of a manifest.
fn manifest_names(manifest: &str) -> Option<(String, Option<String>)> {
    let (mut section, mut package, mut lib) = (String::new(), None, None);
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            section = line.to_string();
            continue;
        }
        let Some(value) = line.strip_prefix("name").map(str::trim_start).and_then(|r| r.strip_prefix('=')) else { continue };
        let value = value.trim().trim_matches('"').to_string();
        match section.as_str() {
            "[package]" => package = Some(value),
            "[lib]" => lib = Some(value),
            _ => {}
        }
    }
    package.map(|p| (p, lib))
}

fn item_attrs(item: &syn::Item) -> &[syn::Attribute] {
    match item {
        syn::Item::Const(i) => &i.attrs,
        syn::Item::Enum(i) => &i.attrs,
        syn::Item::ExternCrate(i) => &i.attrs,
        syn::Item::Fn(i) => &i.attrs,
        syn::Item::ForeignMod(i) => &i.attrs,
        syn::Item::Impl(i) => &i.attrs,
        syn::Item::Macro(i) => &i.attrs,
        syn::Item::Mod(i) => &i.attrs,
        syn::Item::Static(i) => &i.attrs,
        syn::Item::Struct(i) => &i.attrs,
        syn::Item::Trait(i) => &i.attrs,
        syn::Item::TraitAlias(i) => &i.attrs,
        syn::Item::Type(i) => &i.attrs,
        syn::Item::Union(i) => &i.attrs,
        syn::Item::Use(i) => &i.attrs,
        _ => &[],
    }
}

fn strip_tests(items: &mut Vec<syn::Item>) {
    use rafka_mesh_audit::telemetry::test_only;
    items.retain(|i| !test_only(item_attrs(i)));
    for item in items {
        match item {
            syn::Item::Mod(syn::ItemMod { content: Some((_, inner)), .. }) => strip_tests(inner),
            syn::Item::Impl(imp) => imp.items.retain(|ii| match ii {
                syn::ImplItem::Fn(f) => !test_only(&f.attrs),
                syn::ImplItem::Const(c) => !test_only(&c.attrs),
                _ => true,
            }),
            _ => {}
        }
    }
}

/// Uses and glob imports per module: each `use` at the module it sits in, its path made absolute
/// (`crate`, `self`, `super` resolved against that module).
#[allow(clippy::type_complexity)]
fn use_map(items: &[syn::Item], krate: &str, file_module: &[String]) -> (BTreeMap<Vec<String>, BTreeMap<String, Vec<String>>>, BTreeMap<Vec<String>, Vec<Vec<String>>>) {
    fn walk(tree: &syn::UseTree, prefix: &mut Vec<String>, uses: &mut Vec<(String, Vec<String>)>, globs: &mut Vec<Vec<String>>) {
        match tree {
            syn::UseTree::Path(p) => {
                prefix.push(p.ident.to_string());
                walk(&p.tree, prefix, uses, globs);
                prefix.pop();
            }
            syn::UseTree::Name(n) if n.ident != "self" && !prefix.is_empty() => {
                uses.push((n.ident.to_string(), prefix.iter().cloned().chain([n.ident.to_string()]).collect()));
            }
            syn::UseTree::Name(_) => {
                if let Some(last) = prefix.last() {
                    uses.push((last.clone(), prefix.clone()));
                }
            }
            syn::UseTree::Rename(r) if !prefix.is_empty() => {
                uses.push((r.rename.to_string(), prefix.iter().cloned().chain([r.ident.to_string()]).collect()));
            }
            syn::UseTree::Glob(_) if !prefix.is_empty() => globs.push(prefix.clone()),
            syn::UseTree::Group(g) => g.items.iter().for_each(|t| walk(t, prefix, uses, globs)),
            _ => {}
        }
    }
    let absolute = |module: &[String], path: Vec<String>| -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = path.as_slice();
        match rest.first().map(String::as_str) {
            Some("crate") => {
                out.push(krate.to_string());
                rest = &rest[1..];
            }
            Some("self") => {
                out.push(krate.to_string());
                out.extend(module.iter().cloned());
                rest = &rest[1..];
            }
            Some("super") => {
                let mut up = module.to_vec();
                while rest.first().is_some_and(|s| s == "super") {
                    up.pop();
                    rest = &rest[1..];
                }
                out.push(krate.to_string());
                out.extend(up);
            }
            _ => {}
        }
        out.extend(rest.iter().cloned());
        out
    };
    let (mut uses, mut globs): (BTreeMap<Vec<String>, BTreeMap<String, Vec<String>>>, BTreeMap<Vec<String>, Vec<Vec<String>>>) = (BTreeMap::new(), BTreeMap::new());
    for (module, item) in items_in(items, file_module) {
        if let syn::Item::Use(u) = item {
            let (mut found, mut found_globs) = (Vec::new(), Vec::new());
            walk(&u.tree, &mut Vec::new(), &mut found, &mut found_globs);
            let at = uses.entry(module.clone()).or_default();
            for (name, path) in found {
                at.insert(name, absolute(&module, path));
            }
            globs.entry(module.clone()).or_default().extend(found_globs.into_iter().map(|g| absolute(&module, g)));
        }
    }
    (uses, globs)
}

/// The first non-empty line of an item's doc comment, trimmed.
pub fn doc_line(attrs: &[syn::Attribute]) -> String {
    let mut text = String::new();
    for a in attrs.iter().filter(|a| a.path().is_ident("doc")) {
        if let syn::Meta::NameValue(syn::MetaNameValue { value: syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }), .. }) = &a.meta {
            text.push_str(&s.value());
            text.push('\n');
        }
    }
    text.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or_default().to_string()
}

/// Whether an item's `#[derive(...)]` names `name` (`Serialize`, `serde::Serialize`).
pub fn derives(attrs: &[syn::Attribute], name: &str) -> bool {
    attrs.iter().filter(|a| a.path().is_ident("derive")).any(|a| {
        a.parse_args_with(syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated)
            .is_ok_and(|paths| paths.iter().any(|p| p.segments.last().is_some_and(|s| s.ident == name)))
    })
}

/// Every `#[serde(...)]` attribute's contents, as written.
pub fn serde_attrs(attrs: &[syn::Attribute]) -> Vec<String> {
    attrs
        .iter()
        .filter(|a| a.path().is_ident("serde"))
        .filter_map(|a| match &a.meta {
            syn::Meta::List(l) => Some(tokens_text(l.tokens.clone())),
            _ => None,
        })
        .collect()
}

/// The string value of `#[serde(<key> = "...")]`, when an attribute sets it.
pub fn serde_value(attrs: &[syn::Attribute], key: &str) -> Option<String> {
    let mut found = None;
    for a in attrs.iter().filter(|a| a.path().is_ident("serde")) {
        let _ = a.parse_nested_meta(|m| {
            if m.input.peek(syn::Token![=]) {
                let v: syn::Expr = m.value()?.parse()?;
                if m.path.is_ident(key) {
                    if let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) = v {
                        found = Some(s.value());
                    }
                }
            } else if m.input.peek(syn::token::Paren) {
                m.parse_nested_meta(|inner| {
                    if inner.input.peek(syn::Token![=]) {
                        let _: syn::Expr = inner.value()?.parse()?;
                    }
                    Ok(())
                })?;
            }
            Ok(())
        });
    }
    found
}

/// A type, path or expression as compact source text: `Option<Vec<NodeId>>`, `&'static str`.
pub fn text(t: &impl ToTokens) -> String {
    tokens_text(t.to_token_stream())
}

/// A token stream as compact source text: a space between two words, after `,` and `;`,
/// around `+` and a lone `=`, and around a brace group.
pub fn tokens_text(tokens: TokenStream) -> String {
    let word = |t: &TokenTree| matches!(t, TokenTree::Ident(_) | TokenTree::Literal(_));
    let punct = |t: &TokenTree, c: char| matches!(t, TokenTree::Punct(p) if p.as_char() == c);
    let lone_eq = |t: &TokenTree| matches!(t, TokenTree::Punct(p) if p.as_char() == '=' && p.spacing() == Spacing::Alone);
    let brace = |t: &TokenTree| matches!(t, TokenTree::Group(g) if g.delimiter() == Delimiter::Brace);
    let operand = |t: &TokenTree| matches!(t, TokenTree::Ident(_) | TokenTree::Literal(_) | TokenTree::Group(_));
    let mut out = String::new();
    let mut prev: Option<TokenTree> = None;
    // A binary `*`, `/` or `%` (one that follows an operand) is spaced on both sides.
    let mut after_binop = false;
    for t in tokens {
        let binop = matches!(&t, TokenTree::Punct(q) if matches!(q.as_char(), '*' | '/' | '%')) && prev.as_ref().is_some_and(operand);
        if let Some(p) = &prev {
            let space = binop
                || after_binop
                || (word(p) && word(&t))
                || punct(p, ',')
                || punct(p, ';')
                || punct(p, '+')
                || punct(&t, '+')
                || lone_eq(p)
                || (lone_eq(&t) && !matches!(p, TokenTree::Punct(_)))
                || (matches!(p, TokenTree::Group(_)) && word(&t))
                || brace(p)
                || (brace(&t) && !matches!(p, TokenTree::Punct(_)));
            if space {
                out.push(' ');
            }
        }
        match &t {
            TokenTree::Group(g) => {
                let inner = tokens_text(g.stream());
                match g.delimiter() {
                    Delimiter::Parenthesis => out.push_str(&format!("({inner})")),
                    Delimiter::Bracket => out.push_str(&format!("[{inner}]")),
                    Delimiter::Brace if inner.is_empty() => out.push_str("{}"),
                    Delimiter::Brace => out.push_str(&format!("{{ {inner} }}")),
                    Delimiter::None => out.push_str(&inner),
                }
            }
            TokenTree::Punct(p) => out.push(p.as_char()),
            other => out.push_str(&other.to_string()),
        }
        after_binop = binop;
        prev = Some(t);
    }
    out
}

/// Every type path a type mentions, generic arguments included (`Option<Vec<NodeId>>` gives
/// `Option<..>`, `Vec<..>` and `NodeId`).
pub fn type_paths(ty: &syn::Type, out: &mut Vec<syn::Path>) {
    match ty {
        syn::Type::Path(p) => {
            out.push(p.path.clone());
            for seg in &p.path.segments {
                if let syn::PathArguments::AngleBracketed(a) = &seg.arguments {
                    for arg in &a.args {
                        if let syn::GenericArgument::Type(t) = arg {
                            type_paths(t, out);
                        }
                    }
                }
            }
        }
        syn::Type::Reference(r) => type_paths(&r.elem, out),
        syn::Type::Array(a) => type_paths(&a.elem, out),
        syn::Type::Slice(s) => type_paths(&s.elem, out),
        syn::Type::Paren(p) => type_paths(&p.elem, out),
        syn::Type::Group(g) => type_paths(&g.elem, out),
        syn::Type::Tuple(t) => t.elems.iter().for_each(|e| type_paths(e, out)),
        _ => {}
    }
}
