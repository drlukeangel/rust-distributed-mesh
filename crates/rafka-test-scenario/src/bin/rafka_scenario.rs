//! `rafka-scenario <scenario.yaml> [--provider process|container]`
//!
//! Runs one declarative scenario against a live estate it bootstraps through
//! node-admin Build (PRD §20). Exits 1 naming every failed step.

use rafka_test_scenario::{runner, scenario::Scenario};

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mut file = None;
    let mut provider = None;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--provider" => provider = Some(args.next().unwrap_or_else(|| usage("--provider needs a value"))),
            _ if file.is_none() => file = Some(a),
            _ => usage(&format!("unexpected argument {a}")),
        }
    }
    let file = file.unwrap_or_else(|| usage("a scenario file is required"));
    let text = std::fs::read_to_string(&file).unwrap_or_else(|e| usage(&format!("cannot read {file}: {e}")));
    let scenario = Scenario::parse(&text).unwrap_or_else(|e| usage(&format!("{file}: {e}")));
    let test = std::path::Path::new(&file).file_stem().and_then(|s| s.to_str()).unwrap_or("scenario").to_string();
    let report = runner::run(&scenario, provider.as_deref(), &test).await;
    for f in &report.failures {
        eprintln!("rafka-scenario: FAILED {}: {}", f.step, f.reason);
    }
    eprintln!("rafka-scenario: build {} — {} failure(s)", report.build_id, report.failures.len());
    std::process::exit(if report.failures.is_empty() { 0 } else { 1 });
}

fn usage(msg: &str) -> ! {
    eprintln!("rafka-scenario: {msg}\nusage: rafka-scenario <scenario.yaml> [--provider process|container]");
    std::process::exit(2)
}
