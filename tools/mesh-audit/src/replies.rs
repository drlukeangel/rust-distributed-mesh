//! Shared reply construction belongs to the outcome library, never to a runtime handler.
use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use syn::visit::{self, Visit};

const REPLIES: &[&str] = &["StatusReply", "NodeCommandReply", "BuildReply", "BuildClaimReply", "BuildFactsReply", "JoinReply", "NodePrimaryUpdateReply", "TopologyReply", "ForwardReply", "PingReply"];

const BOUNDED: &[&str] = &["no_successor", "incomplete", "failed", "not_primary", "fabric_not_ready", "source_failed", "record_absent", "config_row_refused", "config_not_applied", "blocked", "rejected_tags", "rejected_fabric_not_ready", "node_row_refused", "rejected_name", "rejected_admitting_connections", "busy_holding_connections", "name_not_applied", "incomplete_manifest"];

fn shared(variant: &str) -> bool {
    matches!(variant, "Applied" | "AlreadyApplied" | "RequestNotHandled" | "NotReady" | "Busy" | "ConnectionsDeleting" | "PeerUnresolved" | "Malformed" | "Unauthorized" | "Failed" | "StartFailed" | "NotForwardable" | "InnerNotSent" | "InnerIndeterminate" | "InnerUnserved" | "InnerRejectedStale" | "ConnectionLost" | "NoInnerBudget" | "NameHeld" | "ConfigRowRefused" | "ConfigNotApplied" | "NameNotApplied" | "NodeRowRefused" | "UnknownMesh" | "ReadOverBound" | "BusyHoldingConnections" | "NotFabricPrimary" | "NotExecutor" | "StaleClaim" | "UnknownBuild" | "BuildInProgress" | "AttemptTaken" | "RoleYielded" | "CannotDelete" | "NotClaimant" | "JoinMismatch" | "NotPrimary" | "DeploymentAbandoned" | "CertRefused" | "Lost" | "NotOpen" | "AlreadyClosed" | "NotHolder" | "NotSuccessor" | "RecordAbsent" | "StoreFailed") || variant.starts_with("Rejected")
}
fn fixture(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| a.path().segments.last().is_some_and(|s| s.ident == "test") || (a.path().is_ident("cfg") && a.meta.require_list().is_ok_and(|m| m.tokens.to_string() == "test")))
}

/// A direct shared-outcome construction outside its codec or library.
#[derive(Debug, PartialEq, Eq)]
pub struct Violation {
    /// Source path.
    pub file: PathBuf,
    /// One-based line.
    pub line: usize,
    /// Zero-based source column.
    pub column: usize,
    /// The reply and variant.
    pub constructor: String,
}

/// Inspect Rust syntax: patterns and classifiers read variants; expressions construct them.
pub fn scan(file: &Path, source: &str) -> Vec<Violation> {
    struct Scanner<'a> { file: &'a Path, out: Vec<Violation>, aliases: BTreeMap<String, Vec<String>> }
    fn imports(t: &syn::UseTree, prefix: Vec<String>, aliases: &mut BTreeMap<String, Vec<String>>) {
        match t {
            syn::UseTree::Path(p) => { let mut path=prefix; path.push(p.ident.to_string()); imports(&p.tree,path,aliases); },
            syn::UseTree::Name(n) => { let mut path=prefix; path.push(n.ident.to_string()); aliases.insert(n.ident.to_string(),path); },
            syn::UseTree::Rename(n) => { let mut path=prefix; path.push(n.ident.to_string()); aliases.insert(n.rename.to_string(),path); },
            syn::UseTree::Group(g) => for t in &g.items { imports(t,prefix.clone(),aliases); },
            syn::UseTree::Glob(_) => { aliases.insert("*".into(),prefix); },
        }
    }
    impl Scanner<'_> {
        fn construction(&mut self, p: &syn::Path) {
            let mut parts: Vec<_> = p.segments.iter().map(|s| s.ident.to_string()).collect();
            if let Some(first)=parts.first().and_then(|n| self.aliases.get(n)) { let mut resolved=first.clone(); resolved.extend(parts.into_iter().skip(1)); parts=resolved; }
            if parts.len() == 1 && shared(&parts[0]) { if let Some(glob)=self.aliases.get("*") { let mut path=glob.clone(); path.extend(parts); parts=path; } }
            if parts.len() < 2 { return; }
            let reply = &parts[parts.len()-2]; let variant = &parts[parts.len()-1];
            if REPLIES.contains(&reply.as_str()) && (shared(variant) || BOUNDED.contains(&variant.as_str())) {
                self.out.push(Violation { file: self.file.to_owned(), line: p.segments.first().unwrap().ident.span().start().line, column: p.segments.first().unwrap().ident.span().start().column, constructor: format!("{reply}::{variant}") });
            }
        }
    }
    impl<'ast> Visit<'ast> for Scanner<'_> {
        fn visit_item_use(&mut self, u: &'ast syn::ItemUse) { imports(&u.tree,Vec::new(),&mut self.aliases); }
        fn visit_item_type(&mut self, t: &'ast syn::ItemType) {
            if let syn::Type::Path(p)=t.ty.as_ref() { self.aliases.insert(t.ident.to_string(),p.path.segments.iter().map(|s|s.ident.to_string()).collect()); }
        }
        fn visit_macro(&mut self, m: &'ast syn::Macro) {
            if m.path.segments.last().is_some_and(|s| ["matches","assert","assert_eq","assert_ne","debug_assert","debug_assert_eq","debug_assert_ne"].contains(&s.ident.to_string().as_str())) { return; }
            fn tokens(s: &mut Scanner<'_>, ts: proc_macro2::TokenStream) {
                let v: Vec<_>=ts.into_iter().collect();
                let mut i=0;
                while i<v.len() {
                    if let proc_macro2::TokenTree::Group(g)=&v[i] { tokens(s,g.stream()); }
                    if matches!(&v[i], proc_macro2::TokenTree::Ident(_)) {
                        let mut path=proc_macro2::TokenStream::new(); path.extend([v[i].clone()]); let mut j=i+1;
                        while j+2<v.len() && matches!(&v[j],proc_macro2::TokenTree::Punct(p) if p.as_char()==':') && matches!(&v[j+1],proc_macro2::TokenTree::Punct(p) if p.as_char()==':') && matches!(&v[j+2],proc_macro2::TokenTree::Ident(_)) { path.extend(v[j..j+3].iter().cloned()); j+=3; }
                        if let Ok(p)=syn::parse2::<syn::Path>(path) { s.construction(&p); }
                        i=j;
                    } else { i+=1; }
                }
            }
            tokens(self,m.tokens.clone());
        }
        fn visit_pat(&mut self, _p: &'ast syn::Pat) {}
        fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) { if !fixture(&f.attrs) { visit::visit_item_fn(self, f); } }
        fn visit_item_mod(&mut self, m: &'ast syn::ItemMod) { if !fixture(&m.attrs) { visit::visit_item_mod(self, m); } }
        fn visit_item_impl(&mut self, i: &'ast syn::ItemImpl) {
            let codec = i.trait_.as_ref().is_some_and(|(_,p,_)| p.segments.last().is_some_and(|s| s.ident == "NodeProtocol"));
            let constructor = matches!(i.self_ty.as_ref(), syn::Type::Path(p) if p.path.segments.last().is_some_and(|s| REPLIES.contains(&s.ident.to_string().as_str())));
            if !codec && !constructor { visit::visit_item_impl(self,i); }
        }
        fn visit_expr_struct(&mut self, e: &'ast syn::ExprStruct) { self.construction(&e.path); visit::visit_expr_struct(self,e); }
        fn visit_expr_path(&mut self, e: &'ast syn::ExprPath) { self.construction(&e.path); visit::visit_expr_path(self,e); }
    }
    let mut s = Scanner { file, out: Vec::new(), aliases: BTreeMap::new() };
    s.visit_file(&syn::parse_file(source).unwrap_or_else(|e| panic!("{}: {e}", file.display())));
    s.out
}

/// Walk every workspace source directory, including the test kit's runtime handlers.
pub fn check(root: &Path) -> Vec<Violation> {
    fn walk(p: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(p).unwrap() {
            let p=e.unwrap().path();
            if p.is_dir() { walk(&p,out); } else if p.extension().is_some_and(|e| e == "rs") { out.push(p); }
        }
    }
    let mut files=Vec::new();
    for group in ["crates","tools"] {
        for e in std::fs::read_dir(root.join(group)).unwrap() {
            let src=e.unwrap().path().join("src"); if src.is_dir() { walk(&src,&mut files); }
        }
    }
    files.sort();
    files.into_iter().filter(|p| *p != root.join("crates/rdm-core/src/reply.rs")).flat_map(|p| scan(&p,&std::fs::read_to_string(&p).unwrap())).collect()
}
