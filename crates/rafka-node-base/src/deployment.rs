//! Deployment mode — gates dev-only env-var overrides (RAFKA_DEV_*).
//! Defaults to `Prod` so a production manifest that forgets to set
//! `RAFKA_DEPLOYMENT` gets safe behavior.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deployment {
    Dev,
    Staging,
    Prod,
}

impl Deployment {
    /// Parse `RAFKA_DEPLOYMENT` from the process env. Anything other than
    /// "dev" / "staging" (case-insensitive) is treated as `Prod` — including
    /// unset, empty, and garbage values. Fail-safe to prod.
    pub fn from_env() -> Self {
        match std::env::var("RAFKA_DEPLOYMENT")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "dev" => Deployment::Dev,
            "staging" => Deployment::Staging,
            _ => Deployment::Prod,
        }
    }

    /// True when `RAFKA_DEV_*` overrides are honored.
    pub fn allows_dev_overrides(self) -> bool {
        matches!(self, Deployment::Dev | Deployment::Staging)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_env<F: FnOnce()>(key: &str, value: Option<&str>, f: F) {
        // Tests run in parallel; we serialize on a static mutex so one
        // test's env mutation doesn't leak into another.
        use std::sync::Mutex;
        static LOCK: Mutex<()> = Mutex::new(());
        let _g = LOCK.lock().unwrap();
        let prev = std::env::var(key).ok();
        match value {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        f();
        match prev {
            Some(p) => std::env::set_var(key, p),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn unset_defaults_to_prod() {
        with_env("RAFKA_DEPLOYMENT", None, || {
            assert_eq!(Deployment::from_env(), Deployment::Prod);
            assert!(!Deployment::from_env().allows_dev_overrides());
        });
    }

    #[test]
    fn explicit_prod() {
        with_env("RAFKA_DEPLOYMENT", Some("prod"), || {
            assert_eq!(Deployment::from_env(), Deployment::Prod);
            assert!(!Deployment::from_env().allows_dev_overrides());
        });
    }

    #[test]
    fn dev_allows_overrides() {
        with_env("RAFKA_DEPLOYMENT", Some("dev"), || {
            assert_eq!(Deployment::from_env(), Deployment::Dev);
            assert!(Deployment::from_env().allows_dev_overrides());
        });
    }

    #[test]
    fn staging_allows_overrides() {
        with_env("RAFKA_DEPLOYMENT", Some("staging"), || {
            assert_eq!(Deployment::from_env(), Deployment::Staging);
            assert!(Deployment::from_env().allows_dev_overrides());
        });
    }

    #[test]
    fn unknown_value_defaults_to_prod() {
        with_env("RAFKA_DEPLOYMENT", Some("banana"), || {
            assert_eq!(Deployment::from_env(), Deployment::Prod);
        });
    }

    #[test]
    fn case_insensitive() {
        with_env("RAFKA_DEPLOYMENT", Some("DEV"), || {
            assert_eq!(Deployment::from_env(), Deployment::Dev);
        });
    }
}
