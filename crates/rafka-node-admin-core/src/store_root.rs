//! The fabric's one shared store root (R14).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::FabricId;

    /// CONTRACT (R14 items 1 and 8): a node's boot opens its stores under `<store_dir>/<fabric_id>/`
    /// and nothing else: a `store_dir` that is missing, or is not a readable directory, fails the
    /// boot by name, naming the path and the cause, and nothing is created in its place.
    #[test]
    fn a_missing_or_unreadable_store_dir_fails_the_boot_by_name_and_creates_nothing() {
        let d = crate::record_store::tempdir("store-root");
        let fabric = FabricId::mint();
        let missing = d.join("no-such-store");
        let e = StoreRoot::open(&missing, &fabric).unwrap_err().to_string();
        assert!(e.contains(&missing.display().to_string()) && e.contains("store_dir") && e.contains("No such file"), "{e}");
        assert!(!missing.exists(), "the boot created the store_dir it was refused for");
        let file = d.join("a-file");
        std::fs::write(&file, b"x").unwrap();
        let e = StoreRoot::open(&file, &fabric).unwrap_err().to_string();
        assert!(e.contains(&file.display().to_string()) && e.contains("not a directory"), "{e}");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = StoreRoot::open(&store, &fabric).unwrap();
        assert_eq!(root.dir(), store.join(fabric.to_string()));
        assert!(root.dir().is_dir());
        assert_eq!(root.connections(&"mesh1.rpc.1".parse().unwrap()), store.join(fabric.to_string()).join("connections").join("mesh1.rpc.1"));
        assert_eq!(root.status("mesh2"), store.join(fabric.to_string()).join("status").join("mesh2"));
    }
}
