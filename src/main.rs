//! `quokka` binary entry point.
//!
//! buck2 launches this as its external test executor. The process exit code
//! signals only the runner's own health; the test verdict is delivered to buck2
//! via `end_of_test_results` (see [`quokka::run`]).

use std::path::PathBuf;
use std::process::ExitCode;

use quokka::cli;
use quokka::run::run;

/// Default location for the advisory duration/flake metadata store when
/// `--duration-db` is not supplied: `$HOME/.quokka` when possible, otherwise a
/// directory next to the runner binary.
///
/// buck2 invokes this runner as its `v2_test_executor` with only an executable
/// path — it cannot pass `--duration-db` — so this default is how the DB is
/// maintained in the production `buck2 test` path. The home-scoped path shares
/// history across runner binary upgrades; the binary-adjacent fallback keeps
/// history stable when `HOME` is unavailable. Returns `None` only if neither
/// path can be resolved, in which case duration metadata stays disabled.
fn default_duration_db() -> Option<PathBuf> {
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        Some(home.join(".quokka"))
    } else {
        std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|dir| dir.join("quokka-db")))
    }
}

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "quokka",
    about = "Quokka: A better external test runner for Buck2"
)]
struct QuokkaCli {
    #[command(subcommand)]
    subcommand: Subcommands,
}

#[derive(clap::Subcommand)]
enum Subcommands {
    /// Database introspection commands
    Db(quokka::db_cli::DbCli),
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().collect();

    let has_db_cmd = if let Some(dash_dash_idx) = argv.iter().position(|s| s == "--") {
        argv[..dash_dash_idx].iter().any(|s| s == "db")
    } else {
        argv.iter().any(|s| s == "db")
    };

    if has_db_cmd {
        match QuokkaCli::try_parse_from(&argv) {
            Ok(cli) => {
                let Subcommands::Db(db_cli) = cli.subcommand;
                if let Err(e) = quokka::db_cli::run_db_command(db_cli) {
                    eprintln!("quokka db error: {e}");
                    return ExitCode::from(1);
                }
                return ExitCode::SUCCESS;
            }
            Err(e) => {
                e.exit();
            }
        }
    }

    let mut invocation = match cli::parse(argv) {
        Ok(invocation) => invocation,
        Err(e) => {
            eprintln!("quokka: argument error: {e}");
            return ExitCode::from(2);
        }
    };

    invocation
        .config
        .duration_db
        .resolve_auto(default_duration_db());

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("quokka: failed to start tokio runtime: {e}");
            return ExitCode::from(1);
        }
    };

    match runtime.block_on(run(
        invocation.transport,
        invocation.config,
        invocation.context,
    )) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("quokka: fatal: {e:#}");
            ExitCode::from(1)
        }
    }
}
