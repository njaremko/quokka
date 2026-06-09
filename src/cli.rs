//! Command-line parsing matching buck2's exact launch contract.
//!
//! buck2 invokes the runner as:
//! ```text
//! <runner> --buck-trace-id <id> --config-entry host=<p> [--config-entry ...] \
//!          --executor-fd <N> --orchestrator-fd <N> -- <tpx_args...>
//! ```
//! (TCP mode substitutes `--executor-addr`/`--orchestrator-addr` for the fds.)
//! The `tpx_args` ALWAYS begin `["ignored", "--buck-test-info", "ignored", ...]`
//! — `ignored` is a placeholder argv[0] that clap consumes as the program name.
//!
//! We split argv on the first `--`: the left is the outer flags, the right is
//! the tpx args (a separate clap parse).

use std::path::PathBuf;
use std::time::Duration;

use std::num::NonZeroU32;
use std::num::NonZeroUsize;

use clap::Parser;

use crate::batching::{BatchFailurePolicy, BatchMode};
use crate::listing::IgnoredPolicy;
use crate::translator::{RunFormat, ListFormat};
use crate::variant::{RepeatKind, Variant};

/// Outer flags buck2 passes before `--`.
#[derive(Debug, Parser)]
#[command(disable_help_flag = true)]
struct OuterCli {
    #[arg(long)]
    buck_trace_id: Option<String>,

    /// Repeated `key=value` context entries (host=…, config=…, …).
    #[arg(long = "config-entry")]
    config_entry: Vec<String>,

    #[arg(long)]
    executor_fd: Option<i32>,
    #[arg(long)]
    orchestrator_fd: Option<i32>,
    #[arg(long)]
    executor_addr: Option<String>,
    #[arg(long)]
    orchestrator_addr: Option<String>,
}

/// tpx args buck2 passes after `--` (and runner feature flags routed here via
/// `buck2 test ... -- <flags>`).
#[derive(Debug, Parser)]
#[command(disable_help_flag = true)]
struct TpxConfig {
    /// Ignored; present for buck1/tpx compatibility (buck2 always sends it).
    #[arg(long, hide = true)]
    buck_test_info: Option<String>,

    /// Extra environment, `--env NAME=VALUE`, added to every test.
    #[arg(long)]
    env: Vec<String>,

    /// Extra verbatim args appended to every test command.
    #[arg(long, num_args = 1.., allow_hyphen_values = true)]
    test_arg: Vec<String>,

    /// Default per-test timeout, seconds.
    #[arg(long, default_value_t = 600)]
    timeout: u64,

    /// Listing action timeout, seconds.
    #[arg(long, default_value_t = 600)]
    listing_timeout: u64,

    #[arg(long)]
    include_ignored: bool,
    #[arg(long)]
    ignored_only: bool,

    /// `text` or `json`.
    #[arg(long, default_value = "json")]
    list_format: String,
    #[arg(long, default_value = "json")]
    run_format: String,

    /// `fixed-chunk`, `per-test`, `duration-bucketed`, or `target`.
    #[arg(long, default_value = "fixed-chunk")]
    batch_mode: String,
    /// Tests per action for `--batch-mode fixed-chunk` (clamped to >=1). Default
    /// tuned against vm2 (4660 tests) on remote execution: per-test costs ~13x
    /// (one `Execute2` round trip per test), while 32–128 tests/chunk sit on a
    /// flat optimum. 32 is chosen as the low end of that plateau so smaller
    /// crates keep enough chunks to parallelize across remote workers.
    #[arg(long, default_value_t = 32)]
    chunk_size: usize,
    #[arg(long, default_value_t = 20)]
    batch_threshold_ms: u64,
    /// What to do when a multi-member batch fails to attribute a result to every
    /// member: `isolate` (re-run failing members singly) or `fail-all`.
    #[arg(long, default_value = "isolate")]
    batch_failure_policy: String,

    #[arg(long, default_value = "default")]
    variant: String,
    /// Stress repetitions (0 = run once).
    #[arg(long, default_value_t = 0)]
    stress: u32,
    /// Repetitions applied to a target carrying the `rust:stress` label when no
    /// global `--stress` is set.
    #[arg(long, default_value_t = 50)]
    stress_label_reps: u32,

    #[arg(long, default_value_t = 0)]
    shard_index: u16,
    #[arg(long, default_value_t = 1)]
    shard_count: u16,

    /// Force every action onto the local-debug executor.
    #[arg(long)]
    local_debug: bool,

    /// Attempts granted to `rust:flaky`/`rust:retry` targets (>=1).
    #[arg(long, default_value_t = 3)]
    flaky_attempts: u32,

    /// Directory for the advisory duration/flake metadata store. When omitted,
    /// the binary defaults to a directory next to itself (see
    /// `default_duration_db` in main.rs) so the DB is maintained even when buck2
    /// invokes the runner with only an executable path.
    #[arg(long)]
    duration_db: Option<PathBuf>,

    /// Captured logs larger than this many bytes are uploaded to CAS.
    #[arg(long, default_value_t = 65_536)]
    cas_inline_limit: usize,

}

/// How buck2 connected the runner's two service channels.
#[derive(Debug, Clone)]
pub enum Transport {
    UnixFds {
        executor_fd: i32,
        orchestrator_fd: i32,
    },
    Tcp {
        executor_addr: String,
        orchestrator_addr: String,
    },
}

/// CI shard selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardSpec {
    pub index: u16,
    pub count: u16,
}

impl ShardSpec {
    pub fn is_sharded(&self) -> bool {
        self.count > 1
    }
}

/// Concurrency limits for the scheduler.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerLimits {
    pub max_inflight_listings: usize,
    pub max_inflight_test_actions: usize,
    pub max_inflight_per_target: usize,
    pub max_report_queue: usize,
}

impl Default for SchedulerLimits {
    fn default() -> Self {
        Self {
            max_inflight_listings: 256,
            max_inflight_test_actions: 2_000,
            max_inflight_per_target: 128,
            max_report_queue: 1_024,
        }
    }
}

/// Fully resolved, typed runner configuration.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub per_test_timeout: Duration,
    pub listing_timeout: Duration,
    pub ignored: IgnoredPolicy,
    pub list_format: ListFormat,
    pub run_format: RunFormat,
    pub batch_mode: BatchMode,
    pub batch_failure_policy: BatchFailurePolicy,
    pub variant: Variant,
    pub stress: RepeatKind,
    /// Repetitions for a `rust:stress`-labelled target when `--stress` is unset.
    pub stress_label_reps: NonZeroU32,
    pub shard: ShardSpec,
    pub local_debug: bool,
    pub flaky_attempts: u32,
    pub limits: SchedulerLimits,
    pub cas_inline_limit: usize,
    pub duration_db: Option<PathBuf>,
    pub extra_test_args: Vec<String>,
    pub extra_env: Vec<(String, String)>,
    pub quokka_config: crate::config::QuokkaConfig,
}

/// Contextual metadata for the test session.
#[derive(Debug, Clone)]
pub struct SessionContext {
    pub host_platform: Option<String>,
    pub trace_id: Option<String>,
}

/// A parsed invocation: how to connect, plus the resolved config.
#[derive(Debug)]
pub struct Invocation {
    pub transport: Transport,
    pub config: RunnerConfig,
    pub context: SessionContext,
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(
        "missing transport: provide --executor-fd/--orchestrator-fd or --executor-addr/--orchestrator-addr"
    )]
    MissingTransport,
    #[error("invalid {field}: `{value}`")]
    InvalidValue { field: &'static str, value: String },
    #[error(
        "--ignored-only requires --list-format json: stable text `--list` cannot \
         report a test's ignored status, so an ignored-only run would list and run zero tests"
    )]
    IgnoredOnlyNeedsJsonListing,
    #[error("invalid --env entry `{0}` (expected NAME=VALUE)")]
    InvalidEnv(String),
    #[error("--shard-index {index} is out of range for --shard-count {count}")]
    ShardOutOfRange { index: u16, count: u16 },
    #[error(transparent)]
    Parse(#[from] clap::Error),
}

/// Parse a full argv (including argv[0]) into an [`Invocation`].
pub fn parse(argv: Vec<String>) -> Result<Invocation, CliError> {
    let split = argv.iter().position(|a| a == "--");
    let (left, right) = match split {
        Some(i) => (argv[..i].to_vec(), argv[i + 1..].to_vec()),
        None => (argv, Vec::new()),
    };

    let outer = OuterCli::try_parse_from(left)?;
    // `right[0]` is the placeholder program name buck2 inserts ("ignored");
    // clap's try_parse_from consumes it. If there were no tpx args at all,
    // synthesize a program name so defaults parse.
    let tpx = if right.is_empty() {
        TpxConfig::try_parse_from(["tpx"])?
    } else {
        TpxConfig::try_parse_from(right)?
    };

    let transport = resolve_transport(&outer)?;
    let (config, context) = resolve_config(&outer, tpx)?;
    Ok(Invocation { transport, config, context })
}

fn resolve_transport(outer: &OuterCli) -> Result<Transport, CliError> {
    match (
        outer.executor_fd,
        outer.orchestrator_fd,
        &outer.executor_addr,
        &outer.orchestrator_addr,
    ) {
        (Some(executor_fd), Some(orchestrator_fd), _, _) => Ok(Transport::UnixFds {
            executor_fd,
            orchestrator_fd,
        }),
        (_, _, Some(executor_addr), Some(orchestrator_addr)) => Ok(Transport::Tcp {
            executor_addr: executor_addr.clone(),
            orchestrator_addr: orchestrator_addr.clone(),
        }),
        _ => Err(CliError::MissingTransport),
    }
}

fn resolve_config(outer: &OuterCli, tpx: TpxConfig) -> Result<(RunnerConfig, SessionContext), CliError> {
    let ignored = match (tpx.include_ignored, tpx.ignored_only) {
        (false, false) => IgnoredPolicy::ExcludeIgnored,
        (true, false) => IgnoredPolicy::IncludeIgnored,
        (false, true) => IgnoredPolicy::IgnoredOnly,
        (true, true) => {
            return Err(CliError::InvalidValue {
                field: "ignored",
                value: "--include-ignored with --ignored-only".to_owned(),
            });
        }
    };

    let list_format = match tpx.list_format.as_str() {
        "text" => ListFormat::Text,
        "json" => ListFormat::Json,
        other => {
            return Err(CliError::InvalidValue {
                field: "list-format",
                value: other.to_owned(),
            });
        }
    };
    // Text `--list` reports every test with `ignored == false`, so an
    // ignored-only policy would post-filter the listing down to nothing and
    // report a vacuous pass. The JSON listing carries the real ignored flag.
    if ignored == IgnoredPolicy::IgnoredOnly && list_format == ListFormat::Text {
        return Err(CliError::IgnoredOnlyNeedsJsonListing);
    }
    let run_format = match tpx.run_format.as_str() {
        "text" => RunFormat::Text,
        "json" => RunFormat::Json,
        other => {
            return Err(CliError::InvalidValue {
                field: "run-format",
                value: other.to_owned(),
            });
        }
    };
    let batch_mode = match tpx.batch_mode.as_str() {
        "per-test" => BatchMode::PerTest,
        "duration-bucketed" => BatchMode::DurationBucketed {
            p50_lt_ms: tpx.batch_threshold_ms,
        },
        "fixed-chunk" => BatchMode::FixedChunk {
            size: NonZeroUsize::new(tpx.chunk_size.max(1)).expect("clamped to >=1"),
        },
        "target" => BatchMode::Target,
        other => {
            return Err(CliError::InvalidValue {
                field: "batch-mode",
                value: other.to_owned(),
            });
        }
    };
    let batch_failure_policy = match tpx.batch_failure_policy.as_str() {
        "isolate" => BatchFailurePolicy::RerunPerTestToIsolate,
        "fail-all" => BatchFailurePolicy::FailAll,
        other => {
            return Err(CliError::InvalidValue {
                field: "batch-failure-policy",
                value: other.to_owned(),
            });
        }
    };

    let variant = Variant::parse(&tpx.variant);
    let stress = match NonZeroU32::new(tpx.stress) {
        Some(n) => RepeatKind::Stress(n),
        None => RepeatKind::Once,
    };
    let stress_label_reps = NonZeroU32::new(tpx.stress_label_reps).unwrap_or(NonZeroU32::MIN);

    let shard = ShardSpec {
        index: tpx.shard_index,
        count: tpx.shard_count.max(1),
    };
    if shard.index >= shard.count {
        return Err(CliError::ShardOutOfRange {
            index: shard.index,
            count: shard.count,
        });
    }

    let mut extra_env = Vec::with_capacity(tpx.env.len());
    for entry in &tpx.env {
        match entry.split_once('=') {
            Some((k, v)) => extra_env.push((k.to_owned(), v.to_owned())),
            None => return Err(CliError::InvalidEnv(entry.clone())),
        }
    }

    let host_platform = outer
        .config_entry
        .iter()
        .find_map(|e| e.strip_prefix("host=").map(str::to_owned));

    let config = RunnerConfig {
        per_test_timeout: Duration::from_secs(tpx.timeout),
        listing_timeout: Duration::from_secs(tpx.listing_timeout),
        ignored,
        list_format,
        run_format,
        batch_mode,
        batch_failure_policy,
        variant,
        stress,
        stress_label_reps,
        shard,
        local_debug: tpx.local_debug,
        flaky_attempts: tpx.flaky_attempts.max(1),
        limits: SchedulerLimits::default(),
        cas_inline_limit: tpx.cas_inline_limit,
        duration_db: tpx.duration_db,
        extra_test_args: tpx.test_arg,
        extra_env,
        quokka_config: crate::config::load_config(),
    };

    let context = SessionContext {
        host_platform,
        trace_id: outer.buck_trace_id.clone(),
    };

    Ok((config, context))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_buck2_exact_arg_shape() {
        // The literal vector buck2 sends in the external-executor case.
        let inv = parse(argv(&[
            "quokka",
            "--buck-trace-id",
            "abc-123",
            "--config-entry",
            "host=linux",
            "--config-entry",
            "config=foo;bar",
            "--executor-fd",
            "7",
            "--orchestrator-fd",
            "9",
            "--",
            "ignored",
            "--buck-test-info",
            "ignored",
        ]))
        .expect("must parse buck2's arg shape");
        match inv.transport {
            Transport::UnixFds {
                executor_fd,
                orchestrator_fd,
            } => {
                assert_eq!(executor_fd, 7);
                assert_eq!(orchestrator_fd, 9);
            }
            _ => panic!("expected unix fds"),
        }
        assert_eq!(inv.context.trace_id.as_deref(), Some("abc-123"));
        assert_eq!(inv.context.host_platform.as_deref(), Some("linux"));
        // Defaults.
        assert_eq!(inv.config.per_test_timeout, Duration::from_secs(600));
        assert_eq!(inv.config.ignored, IgnoredPolicy::ExcludeIgnored);
        assert_eq!(inv.config.run_format, RunFormat::Json);
        // Default batching chunks tests to amortize per-action overhead.
        assert_eq!(
            inv.config.batch_mode,
            BatchMode::FixedChunk {
                size: NonZeroUsize::new(32).unwrap()
            }
        );
    }

    #[test]
    fn fixed_chunk_size_is_configurable() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "7",
            "--orchestrator-fd",
            "9",
            "--",
            "ignored",
            "--chunk-size",
            "32",
        ]))
        .unwrap();
        assert_eq!(
            inv.config.batch_mode,
            BatchMode::FixedChunk {
                size: NonZeroUsize::new(32).unwrap()
            }
        );
    }

    #[test]
    fn tcp_transport_and_feature_flags() {
        let inv = parse(argv(&[
            "runner",
            "--executor-addr",
            "127.0.0.1:5001",
            "--orchestrator-addr",
            "127.0.0.1:5002",
            "--",
            "ignored",
            "--include-ignored",
            "--batch-mode",
            "duration-bucketed",
            "--batch-threshold-ms",
            "30",
            "--stress",
            "5",
            "--shard-index",
            "1",
            "--shard-count",
            "4",
            "--local-debug",
        ]))
        .unwrap();
        assert!(matches!(inv.transport, Transport::Tcp { .. }));
        assert_eq!(inv.config.ignored, IgnoredPolicy::IncludeIgnored);
        assert_eq!(
            inv.config.batch_mode,
            BatchMode::DurationBucketed { p50_lt_ms: 30 }
        );
        assert!(inv.config.stress.is_stress());
        assert_eq!(inv.config.shard.index, 1);
        assert_eq!(inv.config.shard.count, 4);
        assert!(inv.config.local_debug);
    }

    #[test]
    fn ignored_only_with_text_listing_is_rejected() {
        let err = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--ignored-only",
            "--list-format",
            "text",
        ]))
        .unwrap_err();
        assert!(matches!(err, CliError::IgnoredOnlyNeedsJsonListing));
    }

    #[test]
    fn extra_env_is_parsed() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--env",
            "RUST_LOG=debug",
        ]))
        .unwrap();
        assert_eq!(
            inv.config.extra_env,
            vec![("RUST_LOG".to_owned(), "debug".to_owned())]
        );
    }

    #[test]
    fn missing_transport_errors() {
        let err = parse(argv(&["runner", "--", "ignored"])).unwrap_err();
        assert!(matches!(err, CliError::MissingTransport));
    }

    #[test]
    fn shard_out_of_range_errors() {
        let err = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--shard-index",
            "4",
            "--shard-count",
            "4",
        ]))
        .unwrap_err();
        assert!(matches!(err, CliError::ShardOutOfRange { .. }));
    }
}
