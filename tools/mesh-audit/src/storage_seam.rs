//! Storage seam gate (i143 1b): every durable read and write of RDM's product crates goes through the
//! six storage boundaries, and the file store is reached only by the file backend that implements
//! them. The release gate proves it by compiling the workspace without the `file-backend` feature;
//! this rule holds it on every landing without a second compile: no product source names the file
//! store (`FileRecords`, `StoreRoot`, `FileBackend`, `FileNodeBackend`, `FileJournal`, any
//! `File…Storage`) outside the file backend's own modules and outside `cfg(test)`.
//!
//! The file backend's own modules: each crate's `storage_backend.rs` (the file construction), the
//! `File*` implementations' files (`storage.rs`, `fabric_storage.rs`, `status_storage.rs`,
//! `build_state.rs`, `build_lanes.rs`), `record_store.rs`, `store_root.rs` and `durable_file.rs`.
//! Names are read from the syntax tree, so a comment, a doc or a string naming them is not a reach;
//! an item gated on `test` (`#[cfg(test)]`, `#[cfg(all(test, …))]`, `#[test]`, `#[tokio::test]`) is
//! skipped whole.

use std::fmt;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

/// The product crates' sources the rule reads.
pub const PRODUCT: &[&str] = &["crates/rdm-admin/src", "crates/rdm-node/src", "crates/rdm-core/src", "crates/rdm-transport/src", "crates/rdm-client/src"];

/// The file backend's own modules, by file name within a product crate's `src`.
pub const FILE_BACKEND: &[&str] = &["storage_backend.rs", "storage.rs", "fabric_storage.rs", "status_storage.rs", "build_state.rs", "build_lanes.rs", "record_store.rs", "store_root.rs", "durable_file.rs"];

/// The file store's names other than a `File…Storage`.
pub const NAMES: &[&str] = &["FileRecords", "StoreRoot", "FileBackend", "FileNodeBackend", "FileJournal"];

/// A product source names the file store outside the file backend's modules and outside `cfg(test)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The file, relative to the workspace root.
    pub file: String,
    /// The line of the name.
    pub line: usize,
    /// The name.
    pub name: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{} names `{}`: the file store is the file backend's alone (storage_backend.rs, the File* storage files, record_store.rs, store_root.rs, durable_file.rs) or a cfg(test) item's; reach it through the six storage boundaries",
            self.file, self.line, self.name
        )
    }
}

/// Whether `name` names the file store.
pub fn is_file_store_name(name: &str) -> bool {
    NAMES.contains(&name) || (name.starts_with("File") && name.ends_with("Storage") && name.len() > "FileStorage".len())
}

/// Whether `attrs` gate their item on `test`.
fn test_gated(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        if a.path().segments.last().is_some_and(|s| s.ident == "test") {
            return true;
        }
        let (true, syn::Meta::List(list)) = (a.path().is_ident("cfg"), &a.meta) else { return false };
        let tokens = list.tokens.to_string();
        tokens.split(|c: char| !c.is_alphanumeric() && c != '_').any(|w| w == "test") && !tokens.replace(' ', "").contains("not(test")
    })
}

struct Names {
    found: Vec<(usize, String)>,
}

impl<'ast> Visit<'ast> for Names {
    fn visit_item(&mut self, i: &'ast syn::Item) {
        let attrs: &[syn::Attribute] = match i {
            syn::Item::Const(x) => &x.attrs,
            syn::Item::Enum(x) => &x.attrs,
            syn::Item::ExternCrate(x) => &x.attrs,
            syn::Item::Fn(x) => &x.attrs,
            syn::Item::ForeignMod(x) => &x.attrs,
            syn::Item::Impl(x) => &x.attrs,
            syn::Item::Macro(x) => &x.attrs,
            syn::Item::Mod(x) => &x.attrs,
            syn::Item::Static(x) => &x.attrs,
            syn::Item::Struct(x) => &x.attrs,
            syn::Item::Trait(x) => &x.attrs,
            syn::Item::TraitAlias(x) => &x.attrs,
            syn::Item::Type(x) => &x.attrs,
            syn::Item::Union(x) => &x.attrs,
            syn::Item::Use(x) => &x.attrs,
            _ => &[],
        };
        if !test_gated(attrs) {
            syn::visit::visit_item(self, i);
        }
    }
    fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
        if !test_gated(&f.attrs) {
            syn::visit::visit_impl_item_fn(self, f);
        }
    }
    fn visit_trait_item_fn(&mut self, f: &'ast syn::TraitItemFn) {
        if !test_gated(&f.attrs) {
            syn::visit::visit_trait_item_fn(self, f);
        }
    }
    fn visit_stmt(&mut self, s: &'ast syn::Stmt) {
        if let syn::Stmt::Local(l) = s {
            if test_gated(&l.attrs) {
                return;
            }
        }
        syn::visit::visit_stmt(self, s);
    }
    fn visit_expr(&mut self, e: &'ast syn::Expr) {
        let gated = match e {
            syn::Expr::Block(b) => test_gated(&b.attrs),
            syn::Expr::Call(c) => test_gated(&c.attrs),
            syn::Expr::MethodCall(c) => test_gated(&c.attrs),
            _ => false,
        };
        if !gated {
            syn::visit::visit_expr(self, e);
        }
    }
    fn visit_ident(&mut self, i: &'ast proc_macro2::Ident) {
        let name = i.to_string();
        if is_file_store_name(&name) {
            self.found.push((i.span().start().line, name));
        }
    }
}

/// The file-store names `text` holds outside `cfg(test)`, with their lines. A file that does not
/// parse is answered with none: the compiler, not this rule, refuses it.
pub fn names_in(text: &str) -> Vec<(usize, String)> {
    let Ok(file) = syn::parse_file(text) else { return Vec::new() };
    let mut names = Names { found: Vec::new() };
    names.visit_file(&file);
    names.found
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// Every violation in the workspace at `root`.
pub fn check(root: &Path) -> Vec<Violation> {
    let mut files = Vec::new();
    for src in PRODUCT {
        rust_files(&root.join(src), &mut files);
    }
    files.sort();
    let mut out = Vec::new();
    for f in files {
        if f.file_name().and_then(|n| n.to_str()).is_some_and(|n| FILE_BACKEND.contains(&n)) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&f) else { continue };
        let rel = f.strip_prefix(root).unwrap_or(&f).display().to_string();
        for (line, name) in names_in(&text) {
            out.push(Violation { file: rel.clone(), line, name });
        }
    }
    out
}
