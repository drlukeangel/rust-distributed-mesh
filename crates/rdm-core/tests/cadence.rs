//! The fabric's three cadences (`rdm_core::cadence`) are read once from the process
//! environment. A variable that is unset takes its default; a variable that is set and is not a
//! whole number of milliseconds is refused by name, with its value, and is never replaced by the
//! default.
//!
//! The cadences are read once per process, so each cell is its own process (`own_process`), started
//! with the environment its subject reads: no cell sets or removes a variable.

use rdm_core::cadence;
use std::ffi::OsStr;
use std::time::Duration;

const STALENESS: &str = "RDM_STALENESS_MS";
const GOSSIP: &str = "RDM_GOSSIP_INTERVAL_MS";
const BACKBONE: &str = "RDM_BACKBONE_INTERVAL_MS";

/// What a reader answers: the cadence, or the message of the refusal it panicked with.
fn read(reader: fn() -> Duration) -> Result<Duration, String> {
    std::panic::catch_unwind(reader).map_err(|p| {
        p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| (*s).to_string())).unwrap_or_else(|| "a panic with no message".to_string())
    })
}

fn env<'a>(staleness: Option<&'a str>, gossip: Option<&'a str>, backbone: Option<&'a str>) -> [(&'static str, Option<&'a OsStr>); 3] {
    [(STALENESS, staleness.map(OsStr::new)), (GOSSIP, gossip.map(OsStr::new)), (BACKBONE, backbone.map(OsStr::new))]
}

/// CONTRACT: a staleness floor that is set to something that is not a whole number of milliseconds
/// (`30s`) is refused by name at the first cadence read of the process, whichever cadence that read
/// asks for: the refusal names `RDM_STALENESS_MS` and the value `"30s"`. What must NOT happen: the
/// default 30 s taken in its place, the process running on a floor nobody configured.
#[test]
fn a_malformed_staleness_floor_is_refused_by_name_at_the_first_cadence_read() {
    let cell = "a_malformed_staleness_floor_is_refused_by_name_at_the_first_cadence_read";
    if crate::own_process::delegated_with_env(module_path!(), cell, &env(Some("30s"), None, None)) {
        return;
    }
    let refusal = read(cadence::gossip_interval).expect_err("a malformed staleness floor was taken as a gossip interval of the default");
    assert!(refusal.contains(STALENESS) && refusal.contains("\"30s\""), "the refusal does not name the variable and its value: {refusal}");
}

/// CONTRACT: a gossip interval of `fast` is refused by name at the first cadence read (here the
/// backbone interval's): the refusal names `RDM_GOSSIP_INTERVAL_MS` and `"fast"`, not the variable
/// that read asked for. What must NOT happen: the default 2 s taken in its place.
#[test]
fn a_malformed_gossip_interval_is_refused_by_name_at_the_first_cadence_read() {
    let cell = "a_malformed_gossip_interval_is_refused_by_name_at_the_first_cadence_read";
    if crate::own_process::delegated_with_env(module_path!(), cell, &env(None, Some("fast"), None)) {
        return;
    }
    let refusal = read(cadence::backbone_gossip_interval).expect_err("a malformed gossip interval was taken as a backbone interval of the default");
    assert!(refusal.contains(GOSSIP) && refusal.contains("\"fast\"") && !refusal.contains(BACKBONE), "the refusal does not name the variable and its value: {refusal}");
}

/// CONTRACT: a backbone interval of `1.5` (not a whole number of milliseconds) is refused by name at
/// the first cadence read, here the staleness floor's: the refusal names `RDM_BACKBONE_INTERVAL_MS`
/// and `"1.5"`. A backbone task reads its interval long after boot, so the process refuses it at
/// its first cadence read, when it can still stop. What must NOT happen: the default 2 s taken in
/// its place.
#[test]
fn a_malformed_backbone_interval_is_refused_by_name_at_the_first_cadence_read() {
    let cell = "a_malformed_backbone_interval_is_refused_by_name_at_the_first_cadence_read";
    if crate::own_process::delegated_with_env(module_path!(), cell, &env(None, None, Some("1.5"))) {
        return;
    }
    let refusal = read(cadence::staleness_floor).expect_err("a malformed backbone interval was taken as a staleness floor of the default");
    assert!(refusal.contains(BACKBONE) && refusal.contains("\"1.5\"") && !refusal.contains(STALENESS), "the refusal does not name the variable and its value: {refusal}");
}

/// CONTRACT: every malformed cadence is named: a set-but-empty staleness floor, a negative gossip
/// interval and a backbone interval with a unit each appear in the one refusal with their own
/// values, and every reader refuses with it. What must NOT happen: the first malformed variable
/// named alone, leaving the next to be found by the next start.
#[test]
fn every_malformed_cadence_is_named_in_the_refusal_of_every_reader() {
    let cell = "every_malformed_cadence_is_named_in_the_refusal_of_every_reader";
    if crate::own_process::delegated_with_env(module_path!(), cell, &env(Some(""), Some("-5"), Some("2s"))) {
        return;
    }
    for (reader, name) in [(cadence::staleness_floor as fn() -> Duration, "staleness_floor"), (cadence::gossip_interval, "gossip_interval"), (cadence::backbone_gossip_interval, "backbone_gossip_interval")] {
        let refusal = read(reader).expect_err(&format!("{name} took a cadence from an environment in which all three are malformed"));
        for (variable, value) in [(STALENESS, "\"\""), (GOSSIP, "\"-5\""), (BACKBONE, "\"2s\"")] {
            assert!(refusal.contains(&format!("{variable}={value}")), "{name}'s refusal does not name {variable}={value}: {refusal}");
        }
    }
}

/// CONTRACT: a whole number of milliseconds is the cadence, and an unset variable is its default:
/// a staleness floor of `1500` is 1.5 s, a backbone interval of `250` is 250 ms, and the unset
/// gossip interval is 2 s. What must NOT happen: a valid value refused, or an unset one.
#[test]
fn a_whole_number_of_milliseconds_is_the_cadence_and_an_unset_variable_its_default() {
    let cell = "a_whole_number_of_milliseconds_is_the_cadence_and_an_unset_variable_its_default";
    if crate::own_process::delegated_with_env(module_path!(), cell, &env(Some("1500"), None, Some("250"))) {
        return;
    }
    assert_eq!(read(cadence::staleness_floor), Ok(Duration::from_millis(1500)));
    assert_eq!(read(cadence::gossip_interval), Ok(Duration::from_secs(2)));
    assert_eq!(read(cadence::backbone_gossip_interval), Ok(Duration::from_millis(250)));
}
