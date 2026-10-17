//! Shared reply construction belongs to the outcome library, never to a runtime handler.
use std::path::{Path, PathBuf};
use syn::visit::{self, Visit};

const REPLIES: &[&str] = &["StatusReply", "NodeCommandReply", "BuildReply", "BuildClaimReply", "BuildFactsReply", "JoinReply", "NodePrimaryUpdateReply", "TopologyReply", "ForwardReply", "PingReply"];

fn shared(variant: &str) -> bool {
    matches!(variant, "Applied" | "AlreadyApplied" | "RequestNotHandled" | "NotReady" | "Busy" | "ConnectionsDeleting" | "PeerUnresolved" | "Malformed" | "Unauthorized" | "Failed" | "StartFailed" | "NotForwardable" | "InnerNotSent" | "InnerIndeterminate" | "InnerUnserved" | "InnerRejectedStale" | "CarrierEdgeLost" | "NoInnerBudget" | "NameHeld" | "ConfigRowRefused" | "ConfigNotApplied" | "NameNotApplied" | "NodeRowRefused" | "UnknownMesh" | "ReadOverBound" | "BusyHoldingConnections" | "NotFabricPrimary" | "NotExecutor" | "StaleClaim" | "UnknownBuild" | "BuildInProgress" | "AttemptTaken" | "SeatYielded" | "CannotDelete" | "NotClaimant" | "JoinMismatch" | "NotAuthority" | "DeploymentAbandoned" | "CertRefused" | "Lost" | "NotOpen" | "AlreadyClosed" | "NotHolder" | "NotSuccessor" | "RecordAbsent" | "StoreFailed") || variant.starts_with("Rejected")
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
    struct Scanner<'a> { file: &'a Path, out: Vec<Violation> }
    impl Scanner<'_> {
        fn construction(&mut self, p: &syn::Path) {
            let parts: Vec<_> = p.segments.iter().map(|s| s.ident.to_string()).collect();
            if parts.len() < 2 { return; }
            let reply = &parts[parts.len()-2]; let variant = &parts[parts.len()-1];
            if REPLIES.contains(&reply.as_str()) && shared(variant) {
                self.out.push(Violation { file: self.file.to_owned(), line: p.segments[parts.len()-2].ident.span().start().line, column: p.segments[parts.len()-2].ident.span().start().column, constructor: format!("{reply}::{variant}") });
            }
        }
    }
    impl<'ast> Visit<'ast> for Scanner<'_> {
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
    let mut s = Scanner { file, out: Vec::new() };
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
