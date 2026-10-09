//! Source-level span ownership: parse Rust, inspect span macros and named instruments,
//! and exclude test-only items. Comments and assertion strings are not emitters.
use std::path::Path;
use proc_macro2::{TokenStream, TokenTree};
use syn::visit::Visit;

#[derive(Debug, serde::Serialize)]
pub struct Emitter {
    pub file: String,
    pub line: usize,
    pub name: String,
}

fn test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().segments.last().is_some_and(|s| s.ident == "test")
            || (a.path().is_ident("cfg") && a.parse_args::<syn::Path>().is_ok_and(|p| p.is_ident("test")))
    })
}

struct Emitters<'a> {
    file: &'a str,
    out: &'a mut Vec<Emitter>,
}
impl Emitters<'_> {
    fn name(&mut self, tokens: TokenStream) {
        let tokens: Vec<_> = tokens.into_iter().collect();
        let mut at = 0;
        // Optional target:/parent: arguments are not the name. Their expression groups
        // are atomic tokens, so commas within a group do not end an argument.
        while matches!(tokens.get(at), Some(TokenTree::Ident(i)) if i == "target" || i == "parent") {
            while at < tokens.len() {
                at += 1;
                if matches!(tokens.get(at - 1), Some(TokenTree::Punct(p)) if p.as_char() == ',') { break; }
            }
        }
        while at < tokens.len() {
            match &tokens[at] {
                TokenTree::Literal(l) => {
                    if let Ok(name) = syn::parse_str::<syn::LitStr>(&l.to_string()) {
                        self.record(name.value(), l.span().start().line);
                        return;
                    }
                }
                TokenTree::Ident(i) if i == "concat" => {
                    if let Some(TokenTree::Group(g)) = tokens.get(at + 2) {
                        let mut name = String::new();
                        let mut line = None;
                        for t in g.stream() {
                            if let TokenTree::Literal(l) = t {
                                if let Ok(part) = syn::parse_str::<syn::LitStr>(&l.to_string()) {
                                    line.get_or_insert(l.span().start().line);
                                    name.push_str(&part.value());
                                }
                            }
                        }
                        if let Some(line) = line { self.record(name, line); }
                        return;
                    }
                }
                _ => {}
            }
            at += 1;
        }
    }
    fn record(&mut self, name: String, line: usize) {
        self.out.push(Emitter { file: self.file.into(), line, name });
    }
    fn unnamed(&mut self, attrs: &[syn::Attribute], default: &syn::Ident) {
        for attr in attrs {
            if attr.path().segments.last().is_some_and(|s| s.ident == "instrument") {
                if let syn::Meta::List(list) = &attr.meta {
                    let tokens: Vec<_> = list.tokens.clone().into_iter().collect();
                    let named = tokens.windows(2).any(|w| matches!(&w[0], TokenTree::Ident(i) if i == "name")
                        && matches!(&w[1], TokenTree::Punct(p) if p.as_char() == '='));
                    if !named { self.record(default.to_string(), default.span().start().line); }
                } else {
                    self.record(default.to_string(), default.span().start().line);
                }
            }
        }
    }
    fn nested(&mut self, tokens: TokenStream) {
        let tokens: Vec<_> = tokens.into_iter().collect();
        for (at, t) in tokens.iter().enumerate() {
            if let TokenTree::Ident(i) = t {
                if span_macro(&i.to_string()) && matches!(tokens.get(at + 1), Some(TokenTree::Punct(p)) if p.as_char() == '!') {
                    if let Some(TokenTree::Group(g)) = tokens.get(at + 2) { self.name(g.stream()); }
                }
            }
            if let TokenTree::Group(g) = t { self.nested(g.stream()); }
        }
    }

}
fn span_macro(name: &str) -> bool {
    matches!(name, "span" | "info_span" | "debug_span" | "trace_span" | "warn_span" | "error_span")
}
impl<'ast> Visit<'ast> for Emitters<'_> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !test_only(&item.attrs) { syn::visit::visit_item_mod(self, item); }
    }
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if !test_only(&item.attrs) { self.unnamed(&item.attrs, &item.sig.ident); syn::visit::visit_item_fn(self, item); }
    }
    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if !test_only(&item.attrs) { self.unnamed(&item.attrs, &item.sig.ident); syn::visit::visit_impl_item_fn(self, item); }
    }
    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        if !test_only(&item.attrs) { syn::visit::visit_item_impl(self, item); }
    }
    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        if mac.path.segments.last().is_some_and(|s| span_macro(&s.ident.to_string())) {
            self.name(mac.tokens.clone());
        } else {
            // Span macros inside select!, macro_rules! and other macro bodies still emit.
            self.nested(mac.tokens.clone());
        }
    }
    fn visit_attribute(&mut self, attr: &'ast syn::Attribute) {
        if attr.path().segments.last().is_some_and(|s| s.ident == "instrument") {
            if let syn::Meta::List(list) = &attr.meta {
                let tokens: Vec<_> = list.tokens.clone().into_iter().collect();
                for window in tokens.windows(3) {
                    if matches!(&window[0], TokenTree::Ident(i) if i == "name")
                        && matches!(&window[1], TokenTree::Punct(p) if p.as_char() == '=') {
                        self.name(std::iter::once(window[2].clone()).collect());
                    }
                }
            }
        }
    }
}

/// All runtime Rust emitters in crates and tools, including newly added crates. Integration
/// tests, examples, benches and cfg(test) items are excluded. Chaos is returned for census;
/// the lock exempts only its explicitly customer-owned namespace in its own crate.
pub fn emitters(root: &Path) -> anyhow::Result<Vec<Emitter>> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<Emitter>) -> anyhow::Result<()> {
        if !dir.exists() { return Ok(()); }
        for entry in std::fs::read_dir(dir)? {
            let p = entry?.path();
            if p.is_dir() {
                if !matches!(p.file_name().and_then(|n| n.to_str()), Some("target" | ".git" | "tests" | "examples" | "benches")) {
                    walk(root, &p, out)?;
                }
            } else if p.extension().is_some_and(|e| e == "rs") && p.components().any(|c| c.as_os_str() == "src") {
                let file = p.strip_prefix(root)?.to_string_lossy().replace('\\', "/");
                let source = std::fs::read_to_string(&p)?;
                let syntax = syn::parse_file(&source).map_err(|e| anyhow::anyhow!("{file}: {e}"))?;
                Emitters { file: &file, out }.visit_file(&syntax);
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    for dir in ["crates", "tools", "demo"] { walk(root, &root.join(dir), &mut out)?; }
    out.sort_by(|a, b| (&a.file, a.line).cmp(&(&b.file, b.line)));
    Ok(out)
}
