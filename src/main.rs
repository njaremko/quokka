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
/// `--duration-db` is not supplied: a directory next to the runner binary.
///
/// buck2 invokes this runner as its `v2_test_executor` with only an executable
/// path — it cannot pass `--duration-db` — so this default is how the DB is
/// maintained in the production `buck2 test` path. Placing it beside the binary
/// (e.g. `.tmp/quokka-db` next to `.tmp/quokka`) keeps
/// it stable across runs, out of `buck-out`, and within the same git-ignored
/// `.tmp` tree. Returns `None` only if the executable path is unavailable, in
/// which case the DB stays ephemeral.
fn default_duration_db() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("quokka-db")))
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().collect();
    let mut invocation = match cli::parse(argv) {
        Ok(invocation) => invocation,
        Err(e) => {
            eprintln!("quokka: argument error: {e}");
            return ExitCode::from(2);
        }
    };

    if invocation.config.duration_db.is_none() {
        invocation.config.duration_db = default_duration_db();
    }

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

    match runtime.block_on(run(invocation.transport, invocation.config, invocation.context)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("quokka: fatal: {e:#}");
            ExitCode::from(1)
        }
    }
}
