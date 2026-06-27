use crate::duration_db::{DurationDb, Environment};
use clap::Parser;
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "quokka db", about = "Introspect the quokka database")]
pub struct DbCli {
    /// Path to the duration/flake database.
    #[arg(long, short = 'd')]
    pub duration_db: Option<PathBuf>,

    #[command(subcommand)]
    pub command: DbCommand,
}

#[derive(Debug, Parser)]
pub enum DbCommand {
    /// Show general statistics about the database.
    Stats,
    /// List test records stored in the database.
    List {
        /// Sort by: target, name, p50, p95, runs, failures, flake-rate.
        #[arg(long, default_value = "target")]
        sort: String,

        /// Filter by environment: local or remote.
        #[arg(long)]
        env: Option<String>,

        /// Filter target by substring.
        #[arg(long)]
        target: Option<String>,

        /// Filter test name by substring.
        #[arg(long)]
        name: Option<String>,

        /// Output format: text or json.
        #[arg(long, default_value = "text")]
        format: String,
    },
}

#[derive(Debug, serde::Serialize)]
struct ListRow {
    key: u64,
    target: String,
    name: String,
    variant: String,
    p50_ms: Option<u64>,
    p95_ms: Option<u64>,
    runs: u64,
    failures: u64,
    flake_rate: f64,
}

fn resolve_db_path(duration_db: Option<PathBuf>) -> PathBuf {
    if let Some(path) = duration_db {
        path
    } else if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        home.join(".quokka")
    } else {
        std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|dir| dir.join("quokka-db")))
            .unwrap_or_else(|| PathBuf::from(".quokka"))
    }
}

pub fn run_db_command(cli: DbCli) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = resolve_db_path(cli.duration_db);
    match cli.command {
        DbCommand::Stats => run_stats(db_path),
        DbCommand::List {
            sort,
            env,
            target,
            name,
            format,
        } => run_list(db_path, sort, env, target, name, format),
    }
}

fn run_stats(db_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let db = DurationDb::load(db_path.clone());
    let mut out = std::io::BufWriter::new(std::io::stdout());

    // File sizes on disk
    let perf_bin_sz = std::fs::metadata(db_path.join("perf.bin"))
        .map(|m| m.len())
        .unwrap_or(0);
    let perf_log_sz = std::fs::metadata(db_path.join("perf.log"))
        .map(|m| m.len())
        .unwrap_or(0);
    let flake_bin_sz = std::fs::metadata(db_path.join("flake.bin"))
        .map(|m| m.len())
        .unwrap_or(0);
    let flake_log_sz = std::fs::metadata(db_path.join("flake.log"))
        .map(|m| m.len())
        .unwrap_or(0);
    let names_bin_sz = std::fs::metadata(db_path.join("names.bin"))
        .map(|m| m.len())
        .unwrap_or(0);
    let names_log_sz = std::fs::metadata(db_path.join("names.log"))
        .map(|m| m.len())
        .unwrap_or(0);

    let total_size =
        perf_bin_sz + perf_log_sz + flake_bin_sz + flake_log_sz + names_bin_sz + names_log_sz;

    let keys = db.all_keys();
    let total_keys = keys.len();

    let mut named_count = 0;
    for &key in &keys {
        if db.get_name(key).is_some() {
            named_count += 1;
        }
    }

    // Aggregate total runs and failures
    let mut total_runs = 0;
    let mut total_failures = 0;
    for &key in &keys {
        if let Some(fr) = db.get_flake_record(None, key) {
            total_runs += fr.runs;
            total_failures += fr.failures;
        }
    }

    let mut print_stats = || -> std::io::Result<()> {
        writeln!(out, "Database directory: {}", db_path.display())?;
        writeln!(
            out,
            "Total size on disk:  {:.2} KB",
            total_size as f64 / 1024.0
        )?;
        writeln!(out, "Total distinct keys: {}", total_keys)?;
        writeln!(out, "Named test records:  {}", named_count)?;
        writeln!(out, "Total test runs:     {}", total_runs)?;
        writeln!(out, "Total failures:      {}", total_failures)?;
        out.flush()?;
        Ok(())
    };

    if let Err(e) = print_stats() {
        if e.kind() != std::io::ErrorKind::BrokenPipe {
            return Err(e.into());
        }
    }

    Ok(())
}

fn run_list(
    db_path: PathBuf,
    sort: String,
    env: Option<String>,
    target_filter: Option<String>,
    name_filter: Option<String>,
    format: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = DurationDb::load(db_path);

    let filter_env = match env.as_deref() {
        Some("local") => Some(Environment::Local),
        Some("remote") => Some(Environment::Remote),
        None => None,
        Some(other) => {
            return Err(format!("Invalid env '{}': expected 'local' or 'remote'", other).into());
        }
    };

    let mut rows = Vec::new();
    for key in db.all_keys() {
        let name_opt = db.get_name(key);
        let target = name_opt
            .map(|n| n.target.clone())
            .unwrap_or_else(|| format!("<unknown:0x{:x}>", key));
        let name = name_opt
            .map(|n| n.name.clone())
            .unwrap_or_else(|| "".to_owned());
        let variant = name_opt
            .map(|n| n.variant.identity().unwrap_or_else(|| "default".to_owned()))
            .unwrap_or_else(|| "".to_owned());

        // Filter target/name
        if let Some(ref target_pat) = target_filter {
            if !target.contains(target_pat) {
                continue;
            }
        }
        if let Some(ref name_pat) = name_filter {
            if !name.contains(name_pat) {
                continue;
            }
        }

        let est = db.estimate_by_key(filter_env, key);
        let (p50_ms, p95_ms) = match est {
            crate::duration_db::DurationEstimate::Measured { p50_ms, p95_ms } => {
                (Some(p50_ms), Some(p95_ms))
            }
            crate::duration_db::DurationEstimate::Unseen => (None, None),
        };

        let (runs, failures, flake_rate) = match db.get_flake_record(filter_env, key) {
            Some(fr) => {
                let frate = if fr.runs > 0 {
                    (fr.failures as f64 / fr.runs as f64) * 100.0
                } else {
                    0.0
                };
                (fr.runs, fr.failures, frate)
            }
            None => (0, 0, 0.0),
        };

        rows.push(ListRow {
            key,
            target,
            name,
            variant,
            p50_ms,
            p95_ms,
            runs,
            failures,
            flake_rate,
        });
    }

    match sort.as_str() {
        "target" => rows.sort_by(|a, b| a.target.cmp(&b.target).then_with(|| a.name.cmp(&b.name))),
        "name" => rows.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.target.cmp(&b.target))),
        "p50" => rows.sort_by(|a, b| b.p50_ms.unwrap_or(0).cmp(&a.p50_ms.unwrap_or(0))),
        "p95" => rows.sort_by(|a, b| b.p95_ms.unwrap_or(0).cmp(&a.p95_ms.unwrap_or(0))),
        "runs" => rows.sort_by(|a, b| b.runs.cmp(&a.runs)),
        "failures" => rows.sort_by(|a, b| b.failures.cmp(&a.failures)),
        "flake-rate" => rows.sort_by(|a, b| {
            b.flake_rate
                .partial_cmp(&a.flake_rate)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        other => {
            return Err(format!("Invalid sort option '{}'", other).into());
        }
    }

    let mut out = std::io::BufWriter::new(std::io::stdout());

    if format == "json" {
        let json_str = serde_json::to_string_pretty(&rows)?;
        use std::io::Write;
        if let Err(e) = writeln!(out, "{}", json_str) {
            if e.kind() == std::io::ErrorKind::BrokenPipe {
                return Ok(());
            }
            return Err(e.into());
        }
    } else {
        if rows.is_empty() {
            use std::io::Write;
            let _ = writeln!(out, "No records found.");
            let _ = out.flush();
            return Ok(());
        }

        let headers = [
            "Target",
            "Test Name",
            "Variant",
            "p50 (ms)",
            "p95 (ms)",
            "Runs",
            "Failures",
            "Flake %",
        ];
        let mut col_widths = headers.iter().map(|h| h.len()).collect::<Vec<_>>();

        for r in &rows {
            col_widths[0] = col_widths[0].max(r.target.len());
            col_widths[1] = col_widths[1].max(r.name.len());
            col_widths[2] = col_widths[2].max(r.variant.len());
            col_widths[3] = col_widths[3].max(r.p50_ms.map(|v| v.to_string().len()).unwrap_or(4));
            col_widths[4] = col_widths[4].max(r.p95_ms.map(|v| v.to_string().len()).unwrap_or(4));
            col_widths[5] = col_widths[5].max(r.runs.to_string().len());
            col_widths[6] = col_widths[6].max(r.failures.to_string().len());
            col_widths[7] = col_widths[7].max(format!("{:.1}%", r.flake_rate).len());
        }

        let mut print_row = |vals: &[String]| -> std::io::Result<()> {
            use std::io::Write;
            for (i, val) in vals.iter().enumerate() {
                let width = col_widths[i];
                let is_numeric = i >= 3;
                if is_numeric {
                    write!(out, "{:>width$} ", val, width = width)?;
                } else {
                    write!(out, "{:<width$} ", val, width = width)?;
                }
            }
            writeln!(out)?;
            Ok(())
        };

        let handle_err = |e: std::io::Error| -> Result<(), Box<dyn std::error::Error>> {
            if e.kind() == std::io::ErrorKind::BrokenPipe {
                Ok(())
            } else {
                Err(e.into())
            }
        };

        if let Err(e) = print_row(&headers.iter().map(|h| h.to_string()).collect::<Vec<_>>()) {
            return handle_err(e);
        }

        let separator = col_widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>();
        if let Err(e) = print_row(&separator) {
            return handle_err(e);
        }

        for r in &rows {
            let p50_str = r
                .p50_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "N/A".to_owned());
            let p95_str = r
                .p95_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "N/A".to_owned());
            let flake_str = format!("{:.1}%", r.flake_rate);

            let vals = vec![
                r.target.clone(),
                r.name.clone(),
                r.variant.clone(),
                p50_str,
                p95_str,
                r.runs.to_string(),
                r.failures.to_string(),
                flake_str,
            ];
            if let Err(e) = print_row(&vals) {
                return handle_err(e);
            }
        }
    }

    let _ = out.flush();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::TestIdentity;
    use crate::variant::Variant;
    use std::time::Duration;

    #[test]
    fn test_db_cli_stats_and_list() {
        let dir = std::env::temp_dir().join(format!("quokka-db-cli-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let tid1 = TestIdentity {
            target: "//src:quokka-lib-test".to_owned(),
            name: "test_1".to_owned(),
            variant: Variant::Default,
        };
        let tid2 = TestIdentity {
            target: "//src:quokka-lib-test".to_owned(),
            name: "test_2_flaky".to_owned(),
            variant: Variant::Asan,
        };

        {
            let mut db = DurationDb::load(dir.clone());
            db.record_at(
                crate::duration_db::Environment::Local,
                &tid1,
                Duration::from_millis(150),
                false,
                None,
                1000,
            );
            db.record_at(
                crate::duration_db::Environment::Local,
                &tid2,
                Duration::from_millis(300),
                true,
                Some(crate::result::FailureClass::Fail),
                1001,
            );
            db.record_at(
                crate::duration_db::Environment::Local,
                &tid2,
                Duration::from_millis(320),
                false,
                None,
                1002,
            );
            db.flush().unwrap();
        }

        // Test stats
        assert!(run_stats(dir.clone()).is_ok());

        // Test list text format
        assert!(
            run_list(
                dir.clone(),
                "target".to_owned(),
                None,
                None,
                None,
                "text".to_owned(),
            )
            .is_ok()
        );

        // Test list JSON format
        assert!(
            run_list(
                dir.clone(),
                "flake-rate".to_owned(),
                Some("local".to_owned()),
                Some("quokka".to_owned()),
                Some("flaky".to_owned()),
                "json".to_owned(),
            )
            .is_ok()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
