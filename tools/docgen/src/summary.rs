//! SUMMARY.md: the mdBook site's chapter list, built from rafka-v2's docs. The lifecycle index first,
//! then each of its `##` sections as a part holding the pages it links (each page once, in the
//! order the index first links it), then every other architecture page sorted by path, then the
//! generated reference pages.

use crate::Page;
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

/// The lifecycle index, relative to the docs directory.
pub const INDEX: &str = "architecture/fabric-mesh-node-lifecycles.md";

/// Write `docs/SUMMARY.md`; the reference pages sit in `out`, which must be inside `docs`.
pub fn write(docs: &Path, out: &Path, pages: &[Page]) -> anyhow::Result<()> {
    let reference = out.strip_prefix(docs).map_err(|_| anyhow::anyhow!("the pages directory {} is not inside the docs directory {}", out.display(), docs.display()))?;
    let reference = reference.to_string_lossy().replace('\\', "/");
    let index_path = docs.join(INDEX);
    let index = std::fs::read_to_string(&index_path).map_err(|e| anyhow::anyhow!("reading the lifecycle index {}: {e}", index_path.display()))?;
    let mut seen = BTreeSet::from([INDEX.to_string()]);
    let mut parts: Vec<(String, Vec<String>)> = Vec::new();
    let mut fence = false;
    for line in index.lines() {
        if line.trim_start().starts_with("```") {
            fence = !fence;
            continue;
        }
        if fence {
            continue;
        }
        if let Some(h) = line.strip_prefix("## ") {
            parts.push((h.trim().to_string(), Vec::new()));
            continue;
        }
        for target in links(line) {
            let Some(rel) = local_page(docs, target) else { continue };
            if seen.insert(rel.clone()) {
                if parts.is_empty() {
                    parts.push((title(docs, INDEX), Vec::new()));
                }
                parts.last_mut().expect("a part is open").1.push(rel);
            }
        }
    }
    let mut rest = Vec::new();
    markdown_files(docs, &docs.join("architecture"), &mut rest)?;
    rest.sort();
    rest.retain(|r| !seen.contains(r));
    parts.push(("Architecture".into(), rest));
    let mut s = format!("{}\n# Summary\n\n[{}]({INDEX})\n", crate::MARKER, escape(&title(docs, INDEX)));
    for (part, entries) in parts.iter().filter(|(_, e)| !e.is_empty()) {
        s.push_str(&format!("\n# {part}\n\n"));
        for rel in entries {
            s.push_str(&format!("- [{}]({rel})\n", escape(&title(docs, rel))));
        }
    }
    s.push_str("\n# Reference (generated)\n\n");
    for p in pages {
        s.push_str(&format!("- [{}]({reference}/{})\n", escape(p.title), p.file));
    }
    let path = docs.join("SUMMARY.md");
    std::fs::write(&path, s).map_err(|e| anyhow::anyhow!("writing {}: {e}", path.display()))
}

/// Every inline link target on a markdown line: the text between `](` and the next `)` or space.
fn links(line: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(at) = rest.find("](") {
        rest = &rest[at + 2..];
        let end = rest.find([')', ' ']).unwrap_or(rest.len());
        out.push(&rest[..end]);
        rest = &rest[end..];
    }
    out
}

/// A link from the index to a markdown page that exists under `docs`, as its docs-relative path.
fn local_page(docs: &Path, target: &str) -> Option<String> {
    if target.contains("://") || target.starts_with('#') || target.starts_with("mailto:") {
        return None;
    }
    let file = target.split('#').next()?;
    if !file.ends_with(".md") {
        return None;
    }
    let mut rel = PathBuf::new();
    for c in Path::new(INDEX).parent()?.join(file).components() {
        match c {
            Component::Normal(n) => rel.push(n),
            Component::ParentDir if rel.pop() => {}
            Component::CurDir => {}
            _ => return None,
        }
    }
    docs.join(&rel).is_file().then(|| rel.to_string_lossy().replace('\\', "/"))
}

/// A page's title: its first `# ` heading, else its file stem.
fn title(docs: &Path, rel: &str) -> String {
    std::fs::read_to_string(docs.join(rel))
        .ok()
        .and_then(|t| t.lines().find_map(|l| l.strip_prefix("# ").map(|h| h.trim().to_string())))
        .unwrap_or_else(|| Path::new(rel).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default())
}

fn escape(text: &str) -> String {
    text.replace('[', "\\[").replace(']', "\\]")
}

fn markdown_files(docs: &Path, dir: &Path, out: &mut Vec<String>) -> anyhow::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        if p.is_dir() {
            markdown_files(docs, &p, out)?;
        } else if p.extension().is_some_and(|x| x == "md") {
            out.push(p.strip_prefix(docs)?.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}
