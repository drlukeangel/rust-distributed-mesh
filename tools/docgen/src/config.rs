//! config.md: every `RDM_*` / `RAFKA_*` environment name in RDM's non-test source, each place the
//! source names it (the literal, or a const that holds it), the item it sits in and the call it is
//! handed to.

use crate::source::{self, Workspace};
use std::collections::BTreeMap;
use syn::parse::Parser;
use syn::visit::Visit;

/// The page title.
pub const TITLE: &str = "Environment names";

/// An `RDM_*` or `RAFKA_*` name: the prefix, then upper-case ASCII, digits and `_`.
fn env_name(s: &str) -> bool {
    s.strip_prefix("RDM_").or_else(|| s.strip_prefix("RAFKA_")).is_some_and(|r| !r.is_empty() && r.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Site {
    name: String,
    file: String,
    line: usize,
    within: String,
    via: String,
}

struct Sites<'a> {
    file: &'a str,
    consts: &'a BTreeMap<String, String>,
    within: Vec<String>,
    impl_ty: Vec<String>,
    via: Vec<String>,
    out: &'a mut Vec<Site>,
}

impl Sites<'_> {
    fn record(&mut self, name: String, line: usize, through: Option<&str>) {
        let via = match (through, self.via.last()) {
            (Some(c), Some(v)) => format!("`{c}`, {v}"),
            (Some(c), None) => format!("`{c}`"),
            (None, Some(v)) => v.clone(),
            (None, None) => "literal".into(),
        };
        let within = self.within.last().cloned().unwrap_or_else(|| "module".into());
        self.out.push(Site { name, file: self.file.into(), line, within, via });
    }

    fn scoped(&mut self, within: String, f: impl FnOnce(&mut Self)) {
        self.within.push(within);
        f(self);
        self.within.pop();
    }

    fn handed(&mut self, via: String, f: impl FnOnce(&mut Self)) {
        self.via.push(via);
        f(self);
        self.via.pop();
    }
}

impl<'ast> Visit<'ast> for Sites<'_> {
    fn visit_item_fn(&mut self, i: &'ast syn::ItemFn) {
        self.scoped(format!("fn `{}`", i.sig.ident), |s| syn::visit::visit_item_fn(s, i));
    }
    fn visit_item_impl(&mut self, i: &'ast syn::ItemImpl) {
        self.impl_ty.push(source::text(&*i.self_ty));
        syn::visit::visit_item_impl(self, i);
        self.impl_ty.pop();
    }
    fn visit_impl_item_fn(&mut self, i: &'ast syn::ImplItemFn) {
        let ty = self.impl_ty.last().cloned().unwrap_or_default();
        self.scoped(format!("fn `{ty}::{}`", i.sig.ident), |s| syn::visit::visit_impl_item_fn(s, i));
    }
    fn visit_item_const(&mut self, i: &'ast syn::ItemConst) {
        self.scoped(format!("const `{}`", i.ident), |s| syn::visit::visit_item_const(s, i));
    }
    fn visit_impl_item_const(&mut self, i: &'ast syn::ImplItemConst) {
        let ty = self.impl_ty.last().cloned().unwrap_or_default();
        self.scoped(format!("const `{ty}::{}`", i.ident), |s| syn::visit::visit_impl_item_const(s, i));
    }
    fn visit_item_static(&mut self, i: &'ast syn::ItemStatic) {
        self.scoped(format!("static `{}`", i.ident), |s| syn::visit::visit_item_static(s, i));
    }
    fn visit_expr_call(&mut self, c: &'ast syn::ExprCall) {
        self.visit_expr(&c.func);
        let callee = source::text(&*c.func);
        for a in &c.args {
            self.handed(format!("argument to `{callee}`"), |s| s.visit_expr(a));
        }
    }
    fn visit_expr_method_call(&mut self, m: &'ast syn::ExprMethodCall) {
        self.visit_expr(&m.receiver);
        for a in &m.args {
            self.handed(format!("argument to `.{}`", m.method), |s| s.visit_expr(a));
        }
    }
    fn visit_lit_str(&mut self, l: &'ast syn::LitStr) {
        if env_name(&l.value()) {
            self.record(l.value(), l.span().start().line, None);
        }
    }
    fn visit_expr_path(&mut self, p: &'ast syn::ExprPath) {
        if let Some(last) = p.path.segments.last() {
            if let Some(name) = self.consts.get(&last.ident.to_string()) {
                self.record(name.clone(), last.ident.span().start().line, Some(&last.ident.to_string()));
            }
        }
        syn::visit::visit_expr_path(self, p);
    }
    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        let name = source::text(&m.path);
        self.handed(format!("in `{name}!`"), |s| {
            match syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated.parse2(m.tokens.clone()) {
                Ok(exprs) => exprs.iter().for_each(|e| s.visit_expr(e)),
                Err(_) => s.tokens(m.tokens.clone()),
            }
        });
    }
}

impl Sites<'_> {
    /// A macro body that does not parse as expressions: its string literals and const idents.
    fn tokens(&mut self, tokens: proc_macro2::TokenStream) {
        for t in tokens {
            match t {
                proc_macro2::TokenTree::Group(g) => self.tokens(g.stream()),
                proc_macro2::TokenTree::Literal(l) => {
                    if let Ok(s) = syn::parse_str::<syn::LitStr>(&l.to_string()) {
                        if env_name(&s.value()) {
                            self.record(s.value(), l.span().start().line, None);
                        }
                    }
                }
                proc_macro2::TokenTree::Ident(i) => {
                    if let Some(name) = self.consts.get(&i.to_string()) {
                        self.record(name.clone(), i.span().start().line, Some(&i.to_string()));
                    }
                }
                _ => {}
            }
        }
    }
}

/// Render the page.
pub fn page(ws: &Workspace) -> anyhow::Result<String> {
    // The consts (and statics) whose value is an env name: a use of one names the env.
    let mut consts = BTreeMap::new();
    for krate in &ws.crates {
        for file in &krate.files {
            for item in source::items(&file.syntax.items) {
                let mut take = |ident: &syn::Ident, expr: &syn::Expr| {
                    if let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) = expr {
                        if env_name(&s.value()) {
                            consts.insert(ident.to_string(), s.value());
                        }
                    }
                };
                match item {
                    syn::Item::Const(c) => take(&c.ident, &c.expr),
                    syn::Item::Static(s) => take(&s.ident, &s.expr),
                    syn::Item::Impl(imp) => imp.items.iter().for_each(|ii| {
                        if let syn::ImplItem::Const(c) = ii {
                            take(&c.ident, &c.expr)
                        }
                    }),
                    _ => {}
                }
            }
        }
    }
    let mut sites = Vec::new();
    for krate in &ws.crates {
        for file in &krate.files {
            Sites { file: &file.path, consts: &consts, within: Vec::new(), impl_ty: Vec::new(), via: Vec::new(), out: &mut sites }.visit_file(&file.syntax);
        }
    }
    sites.sort();
    sites.dedup();
    let names: std::collections::BTreeSet<&str> = sites.iter().map(|s| s.name.as_str()).collect();
    let mut out = format!("{}\n# {TITLE}\n\n", crate::MARKER);
    out.push_str(&format!(
        "Every `RDM_*` and `RAFKA_*` environment name in RDM's non-test source ({} names). Each row is one place the source names it: the literal itself, or a const holding it (`via` names the const), the item it sits in, and the call or macro it is handed to. Sorted by name, then site.\n\n",
        names.len()
    ));
    out.push_str("| name | where | in | via |\n|---|---|---|---|\n");
    for s in &sites {
        out.push_str(&format!("| `{}` | `{}:{}` | {} | {} |\n", s.name, s.file, s.line, crate::render::cell(&s.within), crate::render::cell(&s.via)));
    }
    Ok(out)
}
