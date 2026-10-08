//! Explicit executable bindings: the launch seam an external consumer uses to say which
//! executable each launch template runs.
//!
//! A binding set is a file (`RDM_EXECUTABLE_BINDINGS` names it) mapping every declared launch id
//! (a [`crate::NodeKind`] label; the label carries no behaviour here) to one executable path, its
//! expected sha256, and optionally the container image it must run in. The set names the candidate
//! (the exact source and build) its executables were built from.
//!
//! In this mode node-admin launches ONLY what is bound: a launch id with no binding is refused by
//! name and nothing falls back to a built-in binary. [`BindingSet::validate`] refuses every defect
//! by name and is the one check both the harness (before it starts anything) and node-admin
//! (before it opens a provider) run. A launch re-runs [`Validated::resolve`], which re-hashes the
//! file, so a file replaced after validation is refused at the launch that would have run it.

use crate::NodeKind;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

/// The environment variable naming the binding file.
pub const ENV_EXECUTABLE_BINDINGS: &str = "RDM_EXECUTABLE_BINDINGS";
/// The environment variable carrying the candidate sha the runner expects the set to be built from.
pub const ENV_EXECUTABLE_CANDIDATE: &str = "RDM_EXECUTABLE_CANDIDATE";

/// The exact source and build the bound executables were built from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Candidate {
    pub sha: String,
    pub build: String,
}

/// One launch id's executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub launch_id: String,
    pub executable: PathBuf,
    /// Lowercase hex sha256 of the executable's bytes.
    pub sha256: String,
    /// The image the executable must run in (a container provider); absent for a host process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
}

/// The binding file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingSet {
    pub candidate: Candidate,
    /// Every launch id the consumer declares; each is bound exactly once.
    pub launch_ids: Vec<String>,
    pub bindings: Vec<Binding>,
}

/// What the set is validated against.
#[derive(Debug, Clone)]
pub struct Expect<'a> {
    /// The candidate sha the runner was given.
    pub candidate_sha: &'a str,
    /// Launch ids the run will launch; each must be declared and bound. `node_admin` is always required.
    pub required: &'a [&'a str],
    pub provider_image: ProviderImage<'a>,
}

/// What the provider runs a node in, as far as the caller knows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderImage<'a> {
    /// The provider is not known yet; [`Validated::check_image`] runs once it is.
    Unchecked,
    /// A host process: no binding may name an image.
    Process,
    /// A container provider: every node runs in this image and every binding must name it.
    Container(&'a str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingError {
    Unreadable { path: PathBuf, reason: String },
    Parse { path: PathBuf, reason: String },
    UnknownLaunchId { launch_id: String },
    DuplicateDeclaration { launch_id: String },
    DuplicateBinding { launch_id: String },
    UndeclaredBinding { launch_id: String },
    MissingBinding { launch_id: String },
    NotRequiredByTheSet { launch_id: String },
    ExecutableAbsent { launch_id: String, path: PathBuf, reason: String },
    NotExecutable { launch_id: String, path: PathBuf },
    BadHash { launch_id: String, value: String },
    HashMismatch { launch_id: String, path: PathBuf, expected: String, actual: String },
    CandidateMismatch { expected: String, bound: String },
    ImageMismatch { launch_id: String, bound: String, provider: String },
    ImageUnderProcessProvider { launch_id: String, bound: String },
    NoImageBound { launch_id: String, provider: String },
    Unbound { launch_id: String },
}

impl fmt::Display for BindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable { path, reason } => write!(f, "binding file {} cannot be read: {reason}", path.display()),
            Self::Parse { path, reason } => write!(f, "binding file {} does not parse: {reason}", path.display()),
            Self::UnknownLaunchId { launch_id } => write!(f, "launch id `{launch_id}` is not a launch template of this mesh product"),
            Self::DuplicateDeclaration { launch_id } => write!(f, "launch id `{launch_id}` is declared more than once"),
            Self::DuplicateBinding { launch_id } => write!(f, "launch id `{launch_id}` is bound more than once"),
            Self::UndeclaredBinding { launch_id } => write!(f, "launch id `{launch_id}` is bound but not declared in launch_ids"),
            Self::MissingBinding { launch_id } => write!(f, "launch id `{launch_id}` is declared but has no binding"),
            Self::NotRequiredByTheSet { launch_id } => write!(f, "launch id `{launch_id}` is required by this run but the binding set does not declare it"),
            Self::ExecutableAbsent { launch_id, path, reason } => write!(f, "launch id `{launch_id}`: executable {} cannot be read: {reason}", path.display()),
            Self::NotExecutable { launch_id, path } => write!(f, "launch id `{launch_id}`: {} is not an executable file", path.display()),
            Self::BadHash { launch_id, value } => write!(f, "launch id `{launch_id}`: expected sha256 `{value}` is not 64 lowercase hex characters"),
            Self::HashMismatch { launch_id, path, expected, actual } => write!(f, "launch id `{launch_id}`: {} hashes to {actual}, the binding expects {expected}", path.display()),
            Self::CandidateMismatch { expected, bound } => write!(f, "the binding set was built from candidate {bound}, this run is of candidate {expected}"),
            Self::ImageMismatch { launch_id, bound, provider } => write!(f, "launch id `{launch_id}`: bound to image {bound}, the provider runs every node in {provider}"),
            Self::ImageUnderProcessProvider { launch_id, bound } => write!(f, "launch id `{launch_id}`: bound to image {bound}, but the provider runs host processes"),
            Self::NoImageBound { launch_id, provider } => write!(f, "launch id `{launch_id}`: the provider runs nodes in image {provider} and the binding names no image"),
            Self::Unbound { launch_id } => write!(f, "launch id `{launch_id}` has no explicit binding; an explicit binding set never falls back to a built-in executable"),
        }
    }
}

impl std::error::Error for BindingError {}

/// The sha256 of a file's bytes, lowercase hex.
pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(hex::encode(h.finalize()))
}

fn is_executable(path: &Path) -> std::io::Result<bool> {
    use std::os::unix::fs::PermissionsExt;
    let m = std::fs::metadata(path)?;
    Ok(m.is_file() && m.permissions().mode() & 0o111 != 0)
}

impl BindingSet {
    pub fn load(path: &Path) -> Result<Self, BindingError> {
        let bytes = std::fs::read(path).map_err(|e| BindingError::Unreadable { path: path.to_path_buf(), reason: e.to_string() })?;
        serde_json::from_slice(&bytes).map_err(|e| BindingError::Parse { path: path.to_path_buf(), reason: e.to_string() })
    }

    /// Every defect refused by name, before any provider action: map shape, files, hashes,
    /// candidate, image.
    pub fn validate(&self, expect: &Expect) -> Result<Validated, BindingError> {
        let known: BTreeSet<&str> = NodeKind::ALL.iter().map(|k| k.name()).collect();
        let mut declared = BTreeSet::new();
        for id in &self.launch_ids {
            if !known.contains(id.as_str()) {
                return Err(BindingError::UnknownLaunchId { launch_id: id.clone() });
            }
            if !declared.insert(id.as_str()) {
                return Err(BindingError::DuplicateDeclaration { launch_id: id.clone() });
            }
        }
        let mut bound: BTreeMap<&str, &Binding> = BTreeMap::new();
        for b in &self.bindings {
            if !known.contains(b.launch_id.as_str()) {
                return Err(BindingError::UnknownLaunchId { launch_id: b.launch_id.clone() });
            }
            if bound.insert(b.launch_id.as_str(), b).is_some() {
                return Err(BindingError::DuplicateBinding { launch_id: b.launch_id.clone() });
            }
            if !declared.contains(b.launch_id.as_str()) {
                return Err(BindingError::UndeclaredBinding { launch_id: b.launch_id.clone() });
            }
        }
        for id in &self.launch_ids {
            if !bound.contains_key(id.as_str()) {
                return Err(BindingError::MissingBinding { launch_id: id.clone() });
            }
        }
        for id in std::iter::once(&NodeKind::NodeAdmin.name()).chain(expect.required.iter()) {
            if !declared.contains(id) {
                return Err(BindingError::NotRequiredByTheSet { launch_id: (*id).to_string() });
            }
        }
        if self.candidate.sha != expect.candidate_sha {
            return Err(BindingError::CandidateMismatch { expected: expect.candidate_sha.to_string(), bound: self.candidate.sha.clone() });
        }
        let mut hashes = BTreeMap::new();
        for b in &self.bindings {
            Self::check_image(b, expect.provider_image)?;
            hashes.insert(b.launch_id.clone(), Self::check_file(b)?);
        }
        Ok(Validated { set: self.clone(), hashes })
    }

    fn check_image(b: &Binding, provider: ProviderImage) -> Result<(), BindingError> {
        let id = || b.launch_id.clone();
        match (&b.image, provider) {
            (_, ProviderImage::Unchecked) => Ok(()),
            (None, ProviderImage::Process) => Ok(()),
            (Some(bound), ProviderImage::Process) => Err(BindingError::ImageUnderProcessProvider { launch_id: id(), bound: bound.clone() }),
            (None, ProviderImage::Container(p)) => Err(BindingError::NoImageBound { launch_id: id(), provider: p.to_string() }),
            (Some(bound), ProviderImage::Container(p)) if bound != p => Err(BindingError::ImageMismatch { launch_id: id(), bound: bound.clone(), provider: p.to_string() }),
            (Some(_), ProviderImage::Container(_)) => Ok(()),
        }
    }

    fn check_file(b: &Binding) -> Result<String, BindingError> {
        let id = &b.launch_id;
        let absent = |e: std::io::Error| BindingError::ExecutableAbsent { launch_id: id.clone(), path: b.executable.clone(), reason: e.to_string() };
        if b.sha256.len() != 64 || !b.sha256.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)) {
            return Err(BindingError::BadHash { launch_id: id.clone(), value: b.sha256.clone() });
        }
        if !is_executable(&b.executable).map_err(absent)? {
            return Err(BindingError::NotExecutable { launch_id: id.clone(), path: b.executable.clone() });
        }
        let actual = sha256_file(&b.executable).map_err(absent)?;
        if actual != b.sha256 {
            return Err(BindingError::HashMismatch { launch_id: id.clone(), path: b.executable.clone(), expected: b.sha256.clone(), actual });
        }
        Ok(actual)
    }
}

/// A binding set that passed [`BindingSet::validate`].
#[derive(Debug, Clone)]
pub struct Validated {
    set: BindingSet,
    hashes: BTreeMap<String, String>,
}

/// The executable a launch will run, as bound and as re-verified at the launch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resolved {
    pub launch_id: String,
    pub executable: PathBuf,
    pub sha256: String,
    pub image: Option<String>,
    pub candidate: Candidate,
}

impl Validated {
    pub fn set(&self) -> &BindingSet {
        &self.set
    }

    /// The binding set against the provider the run turned out to use.
    pub fn check_image(&self, provider: ProviderImage) -> Result<(), BindingError> {
        self.set.bindings.iter().try_for_each(|b| BindingSet::check_image(b, provider))
    }

    /// The executable `kind` launches: its binding, re-hashed now. An unbound kind and a file
    /// that changed since validation are refused by name; nothing falls back.
    pub fn resolve(&self, kind: NodeKind) -> Result<Resolved, BindingError> {
        let id = kind.name();
        let b = self.set.bindings.iter().find(|b| b.launch_id == id).ok_or_else(|| BindingError::Unbound { launch_id: id.into() })?;
        let sha256 = Self::recheck(b)?;
        Ok(Resolved { launch_id: id.into(), executable: b.executable.clone(), sha256, image: b.image.clone(), candidate: self.set.candidate.clone() })
    }

    fn recheck(b: &Binding) -> Result<String, BindingError> {
        BindingSet::check_file(b)
    }

    /// Every binding with the hash validation observed, for the run's manifest.
    pub fn receipt(&self) -> serde_json::Value {
        serde_json::json!({
            "candidate": self.set.candidate,
            "bindings": self.set.bindings.iter().map(|b| serde_json::json!({
                "launch_id": b.launch_id, "executable": b.executable, "sha256": self.hashes[&b.launch_id], "image": b.image,
            })).collect::<Vec<_>>(),
        })
    }

    /// Every directory an executable lives in, so a container that must read them can mount them.
    pub fn executable_dirs(&self) -> BTreeSet<PathBuf> {
        self.set.bindings.iter().filter_map(|b| b.executable.parent().map(Path::to_path_buf)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    struct Dir(PathBuf);
    impl Dir {
        fn new(tag: &str) -> Self {
            let d = std::env::temp_dir().join(format!("binding-{tag}-{}-{}", std::process::id(), rand::random::<u32>()));
            std::fs::create_dir_all(&d).unwrap();
            Self(d)
        }
        fn exe(&self, name: &str, body: &str) -> Binding {
            let p = self.0.join(name);
            std::fs::write(&p, body).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
            Binding { launch_id: name.into(), executable: p.clone(), sha256: sha256_file(&p).unwrap(), image: None }
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn set(bindings: Vec<Binding>) -> BindingSet {
        BindingSet { candidate: Candidate { sha: "c0ffee".into(), build: "b1".into() }, launch_ids: bindings.iter().map(|b| b.launch_id.clone()).collect(), bindings }
    }

    fn expect() -> Expect<'static> {
        Expect { candidate_sha: "c0ffee", required: &["broker"], provider_image: ProviderImage::Process }
    }

    /// CONTRACT: a complete, hash-true, candidate-true set validates, and resolving a launch id returns its bound path and hash.
    #[test]
    fn complete_set_validates_and_resolves_each_launch_id() {
        let d = Dir::new("ok");
        let s = set(vec![d.exe("node_admin", "a"), d.exe("broker", "b")]);
        let v = s.validate(&expect()).unwrap();
        let r = v.resolve(NodeKind::Broker).unwrap();
        assert_eq!(r.executable, s.bindings[1].executable);
        assert_eq!(r.sha256, s.bindings[1].sha256);
        assert_eq!(v.resolve(NodeKind::Gateway), Err(BindingError::Unbound { launch_id: "gateway".into() }));
    }

    /// CONTRACT: every planted defect of a binding set is refused by its own named rule.
    #[test]
    fn every_defect_is_refused_by_its_own_name() {
        let d = Dir::new("bad");
        let good = || set(vec![d.exe("node_admin", "a"), d.exe("broker", "b")]);
        let e = expect();
        let mut s = good();
        s.bindings[1].executable = d.0.join("nope");
        assert!(matches!(s.validate(&e), Err(BindingError::ExecutableAbsent { .. })));
        let s = good();
        std::fs::set_permissions(&s.bindings[1].executable, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(s.validate(&e), Err(BindingError::NotExecutable { .. })));
        let mut s = good();
        s.bindings[1].sha256 = "0".repeat(64);
        assert!(matches!(s.validate(&e), Err(BindingError::HashMismatch { .. })));
        let mut s = good();
        s.bindings[1].sha256 = "xyz".into();
        assert!(matches!(s.validate(&e), Err(BindingError::BadHash { .. })));
        let mut s = good();
        s.bindings.push(s.bindings[1].clone());
        assert!(matches!(s.validate(&e), Err(BindingError::DuplicateBinding { .. })));
        let mut s = good();
        s.bindings.pop();
        assert!(matches!(s.validate(&e), Err(BindingError::MissingBinding { .. })));
        let mut s = good();
        s.launch_ids.pop();
        assert!(matches!(s.validate(&e), Err(BindingError::UndeclaredBinding { .. })));
        let mut s = good();
        s.candidate.sha = "deadbeef".into();
        assert!(matches!(s.validate(&e), Err(BindingError::CandidateMismatch { .. })));
        let mut s = good();
        s.bindings[1].launch_id = "bridge".into();
        assert!(matches!(s.validate(&e), Err(BindingError::UnknownLaunchId { .. })));
        let s = good();
        assert!(matches!(s.validate(&Expect { required: &["gateway"], ..expect() }), Err(BindingError::NotRequiredByTheSet { .. })));
        assert!(matches!(s.validate(&Expect { provider_image: ProviderImage::Container("img:1"), ..expect() }), Err(BindingError::NoImageBound { .. })));
        let mut s = good();
        s.bindings[0].image = Some("img:2".into());
        s.bindings[1].image = Some("img:1".into());
        assert!(matches!(s.validate(&Expect { provider_image: ProviderImage::Container("img:1"), ..expect() }), Err(BindingError::ImageMismatch { .. })));
        assert!(matches!(s.validate(&expect()), Err(BindingError::ImageUnderProcessProvider { .. })));
    }

    /// CONTRACT: an executable replaced after validation is refused at the launch that would have run it.
    #[test]
    fn replaced_executable_is_refused_at_launch() {
        let d = Dir::new("swap");
        let s = set(vec![d.exe("node_admin", "a"), d.exe("broker", "b")]);
        let v = s.validate(&expect()).unwrap();
        std::fs::write(&s.bindings[1].executable, "tampered").unwrap();
        assert!(matches!(v.resolve(NodeKind::Broker), Err(BindingError::HashMismatch { .. })));
    }
}
