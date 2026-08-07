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

use std::path::{Path, PathBuf};
use std::time::Duration;

use std::num::NonZeroU32;
use std::num::NonZeroUsize;

use clap::Parser;

use crate::batching::{BatchFailurePolicy, BatchMode};
use crate::listing::IgnoredPolicy;
use crate::translator::{
    ListFormat, RunFormat, libtest_user_ignored_policies, libtest_user_requests_help,
    libtest_user_requests_listing_only, libtest_user_usage_error,
};
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

    /// Declared output env, `--declared-output-env NAME=OUTPUT_DIR`, added to
    /// every test action as an env var whose value is a Buck-declared output
    /// directory.
    #[arg(long = "declared-output-env")]
    declared_output_env: Vec<String>,

    /// Extra verbatim args appended to every test command.
    #[arg(long, allow_hyphen_values = true)]
    test_arg: Vec<String>,

    /// Default per-test timeout, seconds.
    #[arg(long, default_value_t = 600)]
    timeout: u64,

    /// Listing action timeout, seconds.
    #[arg(long, default_value_t = 600)]
    listing_timeout: u64,

    /// Remote Linux memory-cgroup unit: `logical-test` or `action`.
    #[arg(long, default_value = "action")]
    cgroup_granularity: String,

    /// Exact `target|test|memory.max` overrides for singleton test actions.
    #[arg(long = "cgroup-memory-max-override")]
    cgroup_memory_max_overrides: Vec<String>,

    #[arg(long)]
    include_ignored: bool,
    #[arg(long = "ignored")]
    ignored: bool,
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

    /// Disable the advisory duration/flake metadata store. This also disables
    /// duration-DB-driven cache busting for unseen tests.
    #[arg(long)]
    no_duration_db: bool,

    /// Write one bounded, structured result for this runner session. Reporting
    /// is observational and does not change the test verdict.
    #[arg(long)]
    ci_test_report_json: Option<PathBuf>,

    /// Captured logs larger than this many bytes are uploaded to CAS.
    #[arg(long, default_value_t = 65_536)]
    cas_inline_limit: usize,

    /// Maximum test actions quokka may keep in flight across all targets.
    #[arg(long, default_value_t = 2_000)]
    max_inflight_test_actions: usize,

    /// Maximum listing actions quokka may keep in flight.
    #[arg(long, default_value_t = 256)]
    max_inflight_listings: usize,

    /// Maximum test actions quokka may keep in flight for one target. This is
    /// separate from `--batch-mode`: per-test batching still cannot use more
    /// remote workers than this target-local scheduler limit permits.
    #[arg(long, default_value_t = 128)]
    max_inflight_per_target: usize,

    #[arg(long = "force-run-in-process")]
    libtest_force_run_in_process: bool,
    #[arg(long = "exclude-should-panic")]
    libtest_exclude_should_panic: bool,
    #[arg(long = "test")]
    libtest_test: bool,
    #[arg(long = "bench")]
    libtest_bench: bool,
    #[arg(long = "list")]
    libtest_list: bool,
    #[arg(long = "fail-fast")]
    libtest_fail_fast: bool,
    #[arg(short = 'h', long = "help")]
    libtest_help: bool,
    #[arg(long = "logfile")]
    libtest_logfile: Option<String>,
    #[arg(long = "no-capture", alias = "nocapture")]
    libtest_no_capture: bool,
    #[arg(long = "test-threads")]
    libtest_test_threads: Option<String>,
    #[arg(long = "skip")]
    libtest_skip: Vec<String>,
    #[arg(short = 'q', long = "quiet")]
    libtest_quiet: bool,
    #[arg(long = "exact")]
    libtest_exact: bool,
    #[arg(long = "color")]
    libtest_color: Option<String>,
    #[arg(long = "format")]
    libtest_format: Option<String>,
    #[arg(long = "show-output")]
    libtest_show_output: bool,
    #[arg(short = 'Z', value_name = "FLAG")]
    libtest_unstable_options: Vec<String>,
    #[arg(long = "report-time")]
    libtest_report_time: bool,
    #[arg(long = "ensure-time")]
    libtest_ensure_time: bool,
    #[arg(long = "shuffle")]
    libtest_shuffle: bool,
    #[arg(long = "shuffle-seed")]
    libtest_shuffle_seed: Option<String>,
    #[arg(value_name = "FILTER")]
    libtest_filters: Vec<String>,
}

impl TpxConfig {
    fn direct_libtest_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        args.extend(self.libtest_filters.iter().cloned());
        if self.libtest_exact {
            args.push("--exact".to_owned());
        }
        for skip in &self.libtest_skip {
            args.push("--skip".to_owned());
            args.push(skip.clone());
        }
        if self.libtest_force_run_in_process {
            args.push("--force-run-in-process".to_owned());
        }
        if self.libtest_exclude_should_panic {
            args.push("--exclude-should-panic".to_owned());
        }
        if self.libtest_test {
            args.push("--test".to_owned());
        }
        if self.libtest_bench {
            args.push("--bench".to_owned());
        }
        if self.libtest_list {
            args.push("--list".to_owned());
        }
        if self.libtest_fail_fast {
            args.push("--fail-fast".to_owned());
        }
        if self.libtest_help {
            args.push("--help".to_owned());
        }
        if let Some(path) = &self.libtest_logfile {
            args.push("--logfile".to_owned());
            args.push(path.clone());
        }
        if self.libtest_no_capture {
            args.push("--no-capture".to_owned());
        }
        if let Some(threads) = &self.libtest_test_threads {
            args.push("--test-threads".to_owned());
            args.push(threads.clone());
        }
        if self.libtest_quiet {
            args.push("--quiet".to_owned());
        }
        if let Some(color) = &self.libtest_color {
            args.push("--color".to_owned());
            args.push(color.clone());
        }
        if let Some(format) = &self.libtest_format {
            args.push("--format".to_owned());
            args.push(format.clone());
        }
        if self.libtest_show_output {
            args.push("--show-output".to_owned());
        }
        for flag in &self.libtest_unstable_options {
            args.push("-Z".to_owned());
            args.push(flag.clone());
        }
        if self.libtest_report_time {
            args.push("--report-time".to_owned());
        }
        if self.libtest_ensure_time {
            args.push("--ensure-time".to_owned());
        }
        if self.libtest_shuffle {
            args.push("--shuffle".to_owned());
        }
        if let Some(seed) = &self.libtest_shuffle_seed {
            args.push("--shuffle-seed".to_owned());
            args.push(seed.clone());
        }
        args
    }
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

/// Duration database state after CLI parsing and binary-level defaulting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurationDbConfig {
    Auto,
    Persistent(PathBuf),
    Disabled,
}

impl DurationDbConfig {
    pub fn resolve_auto(&mut self, path: Option<PathBuf>) {
        if matches!(self, Self::Auto) {
            if let Some(path) = path {
                *self = Self::Persistent(path);
            }
        }
    }

    pub fn persistent_path(&self) -> Option<&Path> {
        match self {
            Self::Persistent(path) => Some(path.as_path()),
            Self::Auto | Self::Disabled => None,
        }
    }

    pub fn cache_busts_unseen_tests(&self) -> bool {
        matches!(self, Self::Persistent(_))
    }
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
    pub cgroup_granularity: crate::execution::CgroupGranularity,
    pub cgroup_memory_max: crate::execution::CgroupMemoryMax,
    pub cgroup_memory_max_overrides: Vec<crate::execution::CgroupMemoryMaxOverride>,
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
    pub duration_db: DurationDbConfig,
    pub ci_test_report_json: Option<PathBuf>,
    pub libtest_help_only: bool,
    pub libtest_usage_error: Option<String>,
    pub libtest_list_only: bool,
    /// Libtest flags written directly after Buck's runner separator. These are
    /// only forwarded to libtest/doctest targets, unlike `--test-arg`, which is
    /// deliberately framework-agnostic.
    pub direct_libtest_args: Vec<String>,
    pub extra_test_args: Vec<String>,
    pub extra_env: Vec<(String, String)>,
    pub declared_output_env: Vec<(String, String)>,
    pub quokka_config: crate::config::QuokkaConfig,
}

impl RunnerConfig {
    pub fn effective_libtest_args(&self) -> Vec<String> {
        let mut args = self.extra_test_args.clone();
        args.extend(self.direct_libtest_args.iter().cloned());
        args
    }
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
    #[error(
        "invalid --declared-output-env entry `{0}` (expected NAME=OUTPUT_DIR with both sides non-empty)"
    )]
    InvalidDeclaredOutputEnv(String),
    #[error("--shard-index {index} is out of range for --shard-count {count}")]
    ShardOutOfRange { index: u16, count: u16 },
    #[error("--duration-db and --no-duration-db cannot be used together")]
    ConflictingDurationDbFlags,
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
    Ok(Invocation {
        transport,
        config,
        context,
    })
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

fn resolve_config(
    outer: &OuterCli,
    tpx: TpxConfig,
) -> Result<(RunnerConfig, SessionContext), CliError> {
    let extra_test_args = tpx.test_arg.clone();
    let direct_libtest_args = tpx.direct_libtest_args();
    let mut effective_libtest_args = extra_test_args.clone();
    effective_libtest_args.extend(direct_libtest_args.iter().cloned());

    let mut ignored = IgnoredPolicy::ExcludeIgnored;
    let mut ignored_policies = Vec::new();
    if tpx.include_ignored {
        ignored_policies.push(IgnoredPolicy::IncludeIgnored);
    }
    if tpx.ignored || tpx.ignored_only {
        ignored_policies.push(IgnoredPolicy::IgnoredOnly);
    }
    ignored_policies.extend(libtest_user_ignored_policies(&effective_libtest_args));
    for policy in ignored_policies {
        if ignored == IgnoredPolicy::ExcludeIgnored {
            ignored = policy;
        } else if ignored != policy {
            return Err(CliError::InvalidValue {
                field: "ignored",
                value: "--include-ignored with --ignored".to_owned(),
            });
        }
    }

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
    let duration_db = if tpx.no_duration_db {
        if tpx.duration_db.is_some() {
            return Err(CliError::ConflictingDurationDbFlags);
        }
        DurationDbConfig::Disabled
    } else {
        match tpx.duration_db {
            Some(path) => DurationDbConfig::Persistent(path),
            None => DurationDbConfig::Auto,
        }
    };
    let cgroup_granularity = match tpx.cgroup_granularity.as_str() {
        "logical-test" => crate::execution::CgroupGranularity::LogicalTest,
        "action" => crate::execution::CgroupGranularity::Action,
        other => {
            return Err(CliError::InvalidValue {
                field: "cgroup-granularity",
                value: other.to_owned(),
            });
        }
    };
    let cgroup_memory_max_raw = match std::env::var("NOBIE_RUST_TEST_CGROUP_MEMORY_MAX_BYTES") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => {
            crate::execution::DEFAULT_CGROUP_MEMORY_MAX_BYTES.to_string()
        }
        Err(std::env::VarError::NotUnicode(value)) => {
            return Err(CliError::InvalidValue {
                field: "NOBIE_RUST_TEST_CGROUP_MEMORY_MAX_BYTES",
                value: value.to_string_lossy().into_owned(),
            });
        }
    };
    let cgroup_memory_max = crate::execution::CgroupMemoryMax::parse(&cgroup_memory_max_raw)
        .ok_or_else(|| CliError::InvalidValue {
            field: "NOBIE_RUST_TEST_CGROUP_MEMORY_MAX_BYTES",
            value: cgroup_memory_max_raw.clone(),
        })?;
    let mut cgroup_memory_max_overrides = Vec::with_capacity(tpx.cgroup_memory_max_overrides.len());
    for raw_override in &tpx.cgroup_memory_max_overrides {
        let parsed =
            crate::execution::CgroupMemoryMaxOverride::parse(raw_override).ok_or_else(|| {
                CliError::InvalidValue {
                    field: "cgroup-memory-max-override",
                    value: raw_override.clone(),
                }
            })?;
        if cgroup_memory_max_overrides.iter().any(
            |existing: &crate::execution::CgroupMemoryMaxOverride| {
                existing.target == parsed.target && existing.test == parsed.test
            },
        ) {
            return Err(CliError::InvalidValue {
                field: "cgroup-memory-max-override",
                value: raw_override.clone(),
            });
        }
        cgroup_memory_max_overrides.push(parsed);
    }

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

    let mut declared_output_env = Vec::with_capacity(tpx.declared_output_env.len());
    for entry in &tpx.declared_output_env {
        match entry.split_once('=') {
            Some((k, v)) if !k.is_empty() && !v.is_empty() => {
                declared_output_env.push((k.to_owned(), v.to_owned()))
            }
            _ => return Err(CliError::InvalidDeclaredOutputEnv(entry.clone())),
        }
    }

    let host_platform = outer
        .config_entry
        .iter()
        .find_map(|e| e.strip_prefix("host=").map(str::to_owned));

    let config = RunnerConfig {
        per_test_timeout: Duration::from_secs(tpx.timeout),
        listing_timeout: Duration::from_secs(tpx.listing_timeout),
        cgroup_granularity,
        cgroup_memory_max,
        cgroup_memory_max_overrides,
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
        limits: SchedulerLimits {
            max_inflight_listings: tpx.max_inflight_listings.max(1),
            max_inflight_test_actions: tpx.max_inflight_test_actions.max(1),
            max_inflight_per_target: tpx.max_inflight_per_target.max(1),
            ..SchedulerLimits::default()
        },
        cas_inline_limit: tpx.cas_inline_limit,
        duration_db,
        ci_test_report_json: tpx.ci_test_report_json,
        libtest_help_only: libtest_user_requests_help(&effective_libtest_args),
        libtest_usage_error: libtest_user_usage_error(&effective_libtest_args),
        libtest_list_only: libtest_user_requests_listing_only(&effective_libtest_args),
        direct_libtest_args,
        extra_test_args,
        extra_env,
        declared_output_env,
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
        assert_eq!(
            inv.config.cgroup_granularity,
            crate::execution::CgroupGranularity::Action
        );
        assert_eq!(inv.config.ignored, IgnoredPolicy::ExcludeIgnored);
        assert_eq!(inv.config.run_format, RunFormat::Json);
        // Default batching chunks tests to amortize per-action overhead.
        assert_eq!(
            inv.config.batch_mode,
            BatchMode::FixedChunk {
                size: NonZeroUsize::new(32).unwrap()
            }
        );
        // Scheduler concurrency limits fall back to their defaults.
        assert_eq!(inv.config.limits.max_inflight_listings, 256);
        assert_eq!(inv.config.limits.max_inflight_test_actions, 2_000);
        assert_eq!(inv.config.limits.max_inflight_per_target, 128);
    }

    #[test]
    fn inflight_limits_are_configurable_and_clamped() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "7",
            "--orchestrator-fd",
            "9",
            "--",
            "ignored",
            "--max-inflight-test-actions",
            "50",
            "--max-inflight-listings",
            "30720",
            "--max-inflight-per-target",
            "0",
            "--cgroup-granularity",
            "action",
        ]))
        .unwrap();
        assert_eq!(inv.config.limits.max_inflight_listings, 30_720);
        assert_eq!(inv.config.limits.max_inflight_test_actions, 50);
        // Zero would deadlock the scheduler (a semaphore with no permits), so it
        // is clamped up to one.
        assert_eq!(inv.config.limits.max_inflight_per_target, 1);
        assert_eq!(
            inv.config.cgroup_granularity,
            crate::execution::CgroupGranularity::Action
        );
    }

    #[test]
    fn parses_no_duration_db_flag() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--no-duration-db",
        ]))
        .expect("must parse duration DB disable flag");
        assert_eq!(inv.config.duration_db, DurationDbConfig::Disabled);
    }

    #[test]
    fn rejects_conflicting_duration_db_flags() {
        let err = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--duration-db",
            "/tmp/quokka-db",
            "--no-duration-db",
        ]))
        .unwrap_err();
        assert!(matches!(err, CliError::ConflictingDurationDbFlags));
    }

    #[test]
    fn duration_db_auto_resolution_preserves_disabled_state() {
        let mut auto = DurationDbConfig::Auto;
        auto.resolve_auto(Some(PathBuf::from("/tmp/quokka-db")));
        assert_eq!(
            auto,
            DurationDbConfig::Persistent(PathBuf::from("/tmp/quokka-db"))
        );

        let mut disabled = DurationDbConfig::Disabled;
        disabled.resolve_auto(Some(PathBuf::from("/tmp/quokka-db")));
        assert_eq!(disabled, DurationDbConfig::Disabled);
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
    fn standard_libtest_args_are_accepted_directly() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "alpha",
            "--exact",
            "--skip",
            "beta",
            "--no-capture",
            "--ignored",
        ]))
        .unwrap();
        assert_eq!(inv.config.ignored, IgnoredPolicy::IgnoredOnly);
        assert_eq!(
            inv.config.direct_libtest_args,
            vec![
                "alpha".to_owned(),
                "--exact".to_owned(),
                "--skip".to_owned(),
                "beta".to_owned(),
                "--no-capture".to_owned()
            ]
        );
        assert!(inv.config.extra_test_args.is_empty());
    }

    #[test]
    fn buck_test_arg_equals_shape_is_passthrough() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--test-arg=--list",
        ]))
        .unwrap();
        assert!(inv.config.libtest_list_only);
        assert_eq!(inv.config.extra_test_args, vec!["--list".to_owned()]);
    }

    #[test]
    fn libtest_help_is_a_runner_level_request() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--help",
            "--format",
            "json",
        ]))
        .unwrap();
        assert!(inv.config.libtest_help_only);
        assert_eq!(inv.config.libtest_usage_error, None);
    }

    #[test]
    fn libtest_format_usage_errors_match_libtest() {
        let json_without_unstable = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--format=json",
        ]))
        .unwrap();
        assert_eq!(
            json_without_unstable.config.libtest_usage_error.as_deref(),
            Some(
                "The \"json\" format is only accepted on the nightly compiler with -Z unstable-options"
            )
        );

        let junit_with_unstable = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--format",
            "junit",
            "-Z",
            "unstable-options",
        ]))
        .unwrap();
        assert_eq!(junit_with_unstable.config.libtest_usage_error, None);

        let invalid = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--format=nope",
        ]))
        .unwrap();
        assert_eq!(
            invalid.config.libtest_usage_error.as_deref(),
            Some("argument for --format must be pretty, terse, json or junit (was nope)")
        );
    }

    #[test]
    fn buck_test_arg_space_shape_accepts_following_libtest_flags() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--test-arg",
            "editor::formula_bar_f2_existing_reference_edges",
            "--exact",
            "--nocapture",
        ]))
        .unwrap();
        assert_eq!(
            inv.config.extra_test_args,
            vec!["editor::formula_bar_f2_existing_reference_edges".to_owned()]
        );
        assert_eq!(
            inv.config.direct_libtest_args,
            vec!["--exact".to_owned(), "--no-capture".to_owned()]
        );
    }

    #[test]
    fn repeated_buck_test_arg_filter_shape_is_parsed_one_value_at_a_time() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--test-arg",
            "--filter",
            "--test-arg",
            "editor::formula_bar_f2_existing_reference_edges",
            "--test-arg",
            "--exact",
            "--test-arg",
            "--nocapture",
        ]))
        .unwrap();
        assert_eq!(
            inv.config.extra_test_args,
            vec![
                "--filter".to_owned(),
                "editor::formula_bar_f2_existing_reference_edges".to_owned(),
                "--exact".to_owned(),
                "--nocapture".to_owned()
            ]
        );
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
    fn declared_output_env_is_parsed() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--declared-output-env",
            "NOBIE_RUST_TEST_ARTIFACTS_ROOT=rust-test-artifacts",
        ]))
        .unwrap();
        assert_eq!(
            inv.config.declared_output_env,
            vec![(
                "NOBIE_RUST_TEST_ARTIFACTS_ROOT".to_owned(),
                "rust-test-artifacts".to_owned()
            )]
        );
    }

    #[test]
    fn declared_output_env_rejects_empty_parts() {
        let err = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--declared-output-env",
            "=rust-test-artifacts",
        ]))
        .unwrap_err();
        assert!(matches!(err, CliError::InvalidDeclaredOutputEnv(_)));
    }

    #[test]
    fn missing_transport_errors() {
        let err = parse(argv(&["runner", "--", "ignored"])).unwrap_err();
        assert!(matches!(err, CliError::MissingTransport));
    }

    #[test]
    fn ci_test_report_path_is_explicit() {
        let inv = parse(argv(&[
            "runner",
            "--executor-fd",
            "1",
            "--orchestrator-fd",
            "2",
            "--",
            "ignored",
            "--ci-test-report-json",
            ".tmp/quokka-results.json",
        ]))
        .unwrap();
        assert_eq!(
            inv.config.ci_test_report_json,
            Some(PathBuf::from(".tmp/quokka-results.json"))
        );
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
