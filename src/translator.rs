//! Test-framework translators: turning a target's spec into listing/execution
//! commands and decoding harness output into per-test results.
//!
//! The translator is resolved once per target from the spec's `test_type` and
//! is a closed enum (no `dyn`): the set of frameworks is known. The *listing
//! strategy* is a typed state, so "fan out per test" (libtest/nextest),
//! "run the whole target once" (doctest) and "run the whole binary once"
//! (custom) are distinct variants rather than vestigial no-op methods.

use rustc_hash::FxHashMap;
use serde::Deserialize;

use crate::listing::{
    IgnoredPolicy, ListingParseError, TestCase, TestCaseKind, TestCaseLocation, TestCaseMetadata,
};
use crate::result::TestVerdict;

/// Synthetic test name reported for whole-target / whole-binary runs.
pub const DOCTEST_RESULT_NAME: &str = "<doctests>";
pub const BINARY_RESULT_NAME: &str = "<binary>";

pub trait Translator: Send + Sync {
    fn declares_executor_overrides(&self) -> bool;
    fn parser_capability(&self) -> DemuxCapability;
    fn listing_strategy(&self) -> ListingStrategy;
    fn execution_args(
        &self,
        names: &[&str],
        ignored: IgnoredPolicy,
        user_args: &[String],
    ) -> Vec<String>;
    fn parse_results(&self, stdout: &[u8], stderr: &[u8]) -> FxHashMap<String, PerTestObservation>;
}

/// How a target's tests are discovered and fanned out.
pub enum ListingStrategy {
    /// List the target, then issue one (or one batched) execution per test.
    PerTestListing {
        request_args: Box<dyn Fn(IgnoredPolicy, &[String]) -> Vec<String> + Send + Sync>,
        parse: Box<
            dyn Fn(&[u8], IgnoredPolicy) -> Result<Vec<TestCase>, ListingParseError> + Send + Sync,
        >,
    },
    /// No listing; one execution covers the whole target, reported under `name`.
    WholeTarget { name: &'static str },
    /// No listing; one execution runs the whole binary, reported under `name`.
    WholeBinary { name: &'static str },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DemuxCapability {
    SingletonOnly,
    NameAttributable,
}

pub struct TranslatorRegistry {
    factories: FxHashMap<
        String,
        Box<dyn Fn(&crate::cli::RunnerConfig) -> Box<dyn Translator> + Send + Sync>,
    >,
}

impl TranslatorRegistry {
    pub fn new() -> Self {
        let mut reg = Self {
            factories: FxHashMap::default(),
        };
        reg.register("rust", |config| {
            Box::new(LibtestTranslator {
                list_format: config.list_format,
                run_format: config.run_format,
            })
        });
        reg.register("rust_doctest_v1", |config| {
            Box::new(DoctestTranslator {
                run_format: config.run_format,
            })
        });
        reg.register("custom_test_v1", |_| Box::new(CustomBinaryTranslator));
        reg
    }

    pub fn register(
        &mut self,
        test_type: &str,
        factory: impl Fn(&crate::cli::RunnerConfig) -> Box<dyn Translator> + Send + Sync + 'static,
    ) {
        self.factories
            .insert(test_type.to_owned(), Box::new(factory));
    }

    pub fn resolve(
        &self,
        test_type: &str,
        config: &crate::cli::RunnerConfig,
    ) -> Option<Box<dyn Translator>> {
        self.factories.get(test_type).map(|f| f(config))
    }
}

pub fn libtest_listing_args(
    ignored: IgnoredPolicy,
    format: ListFormat,
    user_args: &[String],
) -> Vec<String> {
    let user_args = LibtestUserArgs::parse(user_args);
    let mut args: Vec<String> = match format {
        ListFormat::Text => vec!["--list".to_owned()],
        ListFormat::Json => vec![
            "-Z".to_owned(),
            "unstable-options".to_owned(),
            "--list".to_owned(),
            "--format".to_owned(),
            "json".to_owned(),
        ],
    };
    if format == ListFormat::Text && user_args.listing_needs_unstable {
        args.push("-Z".to_owned());
        args.push("unstable-options".to_owned());
    }
    match ignored {
        IgnoredPolicy::ExcludeIgnored => {}
        IgnoredPolicy::IncludeIgnored => args.push("--include-ignored".to_owned()),
        IgnoredPolicy::IgnoredOnly => args.push("--ignored".to_owned()),
    }
    args.extend(user_args.listing_args);
    args
}

pub fn libtest_execution_args(
    names: &[&str],
    ignored: IgnoredPolicy,
    format: RunFormat,
    user_args: &[String],
) -> Vec<String> {
    let user_args = LibtestUserArgs::parse(user_args);
    let mut args: Vec<String> = names.iter().map(|s| s.to_string()).collect();
    args.push("--exact".to_owned());
    args.push("--test-threads=1".to_owned());
    args.push("--color=never".to_owned());
    match ignored {
        IgnoredPolicy::ExcludeIgnored => {}
        IgnoredPolicy::IncludeIgnored => args.push("--include-ignored".to_owned()),
        IgnoredPolicy::IgnoredOnly => args.push("--ignored".to_owned()),
    }
    if format == RunFormat::Json || user_args.execution_needs_unstable {
        for flag in ["-Z", "unstable-options", "--format", "json"] {
            if format == RunFormat::Json || flag == "-Z" || flag == "unstable-options" {
                args.push(flag.to_owned());
            }
        }
    }
    args.extend(user_args.per_test_execution_args);
    args
}

pub fn libtest_decode(
    format: RunFormat,
    stdout: &[u8],
    stderr: &[u8],
) -> FxHashMap<String, PerTestObservation> {
    let stdout = String::from_utf8_lossy(stdout);
    match format {
        RunFormat::Json => decode_json(&stdout),
        RunFormat::Text => decode_text(&stdout, &String::from_utf8_lossy(stderr)),
    }
}

pub fn parse_listing(
    format: ListFormat,
    stdout: &[u8],
    policy: IgnoredPolicy,
) -> Result<Vec<crate::listing::TestCase>, crate::listing::ListingParseError> {
    let text =
        std::str::from_utf8(stdout).map_err(|_| crate::listing::ListingParseError::NotUtf8)?;
    let all = match format {
        ListFormat::Text => parse_text(text),
        ListFormat::Json => parse_json(text),
    };
    Ok(all
        .into_iter()
        .filter(|t| policy.selects(t.ignored))
        .collect())
}

/// Parse stable `--list` text: lines of `name: test` or `name: benchmark`,
/// ignoring the trailing `N tests, M benchmarks` summary and blank lines.
fn parse_text(text: &str) -> Vec<crate::listing::TestCase> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim_end();
            let (name, kind) = line
                .strip_suffix(": test")
                .map(|name| (name, TestCaseKind::Test))
                .or_else(|| {
                    line.strip_suffix(": benchmark")
                        .map(|name| (name, TestCaseKind::Benchmark))
                })?;
            if name.is_empty() {
                return None;
            }
            Some(crate::listing::TestCase::new(name.to_owned(), kind, false))
        })
        .collect()
}

#[derive(serde::Deserialize)]
struct JsonListEvent {
    #[serde(rename = "type")]
    kind: String,
    event: Option<String>,
    name: Option<String>,
    #[serde(default)]
    ignore: bool,
    #[serde(default)]
    ignore_message: Option<String>,
    #[serde(default)]
    source_path: Option<String>,
    #[serde(default)]
    start_line: Option<u64>,
    #[serde(default)]
    start_col: Option<u64>,
    #[serde(default)]
    end_line: Option<u64>,
    #[serde(default)]
    end_col: Option<u64>,
}

/// Parse nightly JSON `--list` output: line-delimited objects, keeping
/// `{"type":"test","event":"discovered",...}` with their `ignore` flag.
fn parse_json(text: &str) -> Vec<crate::listing::TestCase> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let event: JsonListEvent = serde_json::from_str(line).ok()?;
            let kind = match event.kind.as_str() {
                "test" => TestCaseKind::Test,
                "benchmark" => TestCaseKind::Benchmark,
                _ => return None,
            };
            // discovery uses event=="discovered"; accept missing event too.
            if event.event.as_deref().is_some_and(|ev| ev != "discovered") {
                return None;
            }
            let location = match (
                event.source_path,
                event.start_line,
                event.start_col,
                event.end_line,
                event.end_col,
            ) {
                (
                    Some(source_path),
                    Some(start_line),
                    Some(start_col),
                    Some(end_line),
                    Some(end_col),
                ) => Some(TestCaseLocation {
                    source_path,
                    start_line,
                    start_col,
                    end_line,
                    end_col,
                }),
                _ => None,
            };
            Some(crate::listing::TestCase {
                name: event.name?,
                kind,
                ignored: event.ignore,
                metadata: TestCaseMetadata {
                    ignore_message: event.ignore_message,
                    location,
                },
            })
        })
        .collect()
}

pub fn libtest_list_output(user_args: &[String], tests: &[TestCase]) -> String {
    match LibtestUserArgs::parse(user_args).list_output_format() {
        LibtestListOutputFormat::Text { summary } => libtest_text_list_output(tests, summary),
        LibtestListOutputFormat::Json => libtest_json_list_output(tests),
    }
}

fn libtest_text_list_output(tests: &[TestCase], summary: bool) -> String {
    let mut out = String::new();
    for test in tests {
        out.push_str(&test.name);
        out.push_str(match test.kind {
            TestCaseKind::Test => ": test\n",
            TestCaseKind::Benchmark => ": benchmark\n",
        });
    }
    if summary {
        let test_count = tests
            .iter()
            .filter(|test| test.kind == TestCaseKind::Test)
            .count();
        let benchmark_count = tests
            .iter()
            .filter(|test| test.kind == TestCaseKind::Benchmark)
            .count();
        out.push('\n');
        out.push_str(&format!(
            "{} {}, {} {}\n",
            test_count,
            plural(test_count, "test", "tests"),
            benchmark_count,
            plural(benchmark_count, "benchmark", "benchmarks"),
        ));
    }
    out
}

fn libtest_json_list_output(tests: &[TestCase]) -> String {
    let mut out = String::new();
    out.push_str("{ \"type\": \"suite\", \"event\": \"discovery\" }\n");
    for test in tests {
        let kind = match test.kind {
            TestCaseKind::Test => "test",
            TestCaseKind::Benchmark => "benchmark",
        };
        out.push_str(&format!(
            "{{ \"type\": \"{kind}\", \"event\": \"discovered\", \"name\": {}, \"ignore\": {}",
            serde_json::to_string(&test.name).expect("string serialization cannot fail"),
            test.ignored,
        ));
        if let Some(ignore_message) = &test.metadata.ignore_message {
            out.push_str(&format!(
                ", \"ignore_message\": {}",
                serde_json::to_string(ignore_message).expect("string serialization cannot fail"),
            ));
        }
        if let Some(location) = &test.metadata.location {
            out.push_str(&format!(
                ", \"source_path\": {}, \"start_line\": {}, \"start_col\": {}, \"end_line\": {}, \"end_col\": {}",
                serde_json::to_string(&location.source_path)
                    .expect("string serialization cannot fail"),
                location.start_line,
                location.start_col,
                location.end_line,
                location.end_col,
            ));
        }
        out.push_str(" }\n");
    }
    let test_count = tests
        .iter()
        .filter(|test| test.kind == TestCaseKind::Test)
        .count();
    let benchmark_count = tests
        .iter()
        .filter(|test| test.kind == TestCaseKind::Benchmark)
        .count();
    let ignored_count = tests.iter().filter(|test| test.ignored).count();
    out.push_str(&format!(
        "{{ \"type\": \"suite\", \"event\": \"completed\", \"tests\": {test_count}, \"benchmarks\": {benchmark_count}, \"total\": {}, \"ignored\": {ignored_count} }}\n",
        test_count + benchmark_count,
    ));
    out
}

fn plural(count: usize, singular: &'static str, plural: &'static str) -> &'static str {
    if count == 1 { singular } else { plural }
}

pub fn doctest_execution_args(
    ignored: IgnoredPolicy,
    format: RunFormat,
    user_args: &[String],
) -> Vec<String> {
    let user_args = LibtestUserArgs::parse(user_args);
    let mut args = vec![];
    args.push("--test-threads=1".to_owned());
    args.push("--color=never".to_owned());
    match ignored {
        IgnoredPolicy::ExcludeIgnored => {}
        IgnoredPolicy::IncludeIgnored => args.push("--include-ignored".to_owned()),
        IgnoredPolicy::IgnoredOnly => args.push("--ignored".to_owned()),
    }
    if format == RunFormat::Json || user_args.execution_needs_unstable {
        for flag in ["-Z", "unstable-options", "--format", "json"] {
            if format == RunFormat::Json || flag == "-Z" || flag == "unstable-options" {
                args.push(flag.to_owned());
            }
        }
    }
    args.extend(user_args.whole_target_execution_args);
    args
}

pub fn custom_binary_execution_args() -> Vec<String> {
    vec![]
}

pub fn custom_binary_decode() -> FxHashMap<String, PerTestObservation> {
    FxHashMap::default()
}

/// Output format for per-test execution.
/// The format used for `--list` output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListFormat {
    /// Stable `--list` text (`name: test`). Cannot report ignored status.
    Text,
    /// Nightly `-Z unstable-options --list --format json`. Reports ignored.
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunFormat {
    Text,
    Json,
}

/// One observed per-test outcome from a (possibly batched) execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerTestObservation {
    pub status: TestVerdict,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LibtestUserArgs {
    listing_args: Vec<String>,
    per_test_execution_args: Vec<String>,
    whole_target_execution_args: Vec<String>,
    ignored_policies: Vec<IgnoredPolicy>,
    listing_only: bool,
    help: bool,
    output_format: LibtestOutputFormat,
    unstable_options: bool,
    format_error: Option<String>,
    listing_needs_unstable: bool,
    execution_needs_unstable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LibtestListOutputFormat {
    Text { summary: bool },
    Json,
}

impl Default for LibtestListOutputFormat {
    fn default() -> Self {
        Self::Text { summary: true }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum LibtestOutputFormat {
    #[default]
    Pretty,
    Terse,
    Json,
    Junit,
}

pub fn libtest_user_ignored_policies(args: &[String]) -> Vec<IgnoredPolicy> {
    LibtestUserArgs::parse(args).ignored_policies
}

pub fn libtest_user_requests_listing_only(args: &[String]) -> bool {
    LibtestUserArgs::parse(args).listing_only
}

pub fn libtest_user_requests_help(args: &[String]) -> bool {
    LibtestUserArgs::parse(args).help
}

pub fn libtest_user_usage_error(args: &[String]) -> Option<String> {
    let parsed = LibtestUserArgs::parse(args);
    if parsed.help {
        return None;
    }
    if let Some(error) = parsed.format_error {
        return Some(error);
    }
    if matches!(
        parsed.output_format,
        LibtestOutputFormat::Json | LibtestOutputFormat::Junit
    ) && !parsed.unstable_options
    {
        let format = match parsed.output_format {
            LibtestOutputFormat::Json => "json",
            LibtestOutputFormat::Junit => "junit",
            _ => unreachable!(),
        };
        return Some(format!(
            "The \"{format}\" format is only accepted on the nightly compiler with -Z unstable-options",
        ));
    }
    None
}

pub fn libtest_help_output(program: &str) -> String {
    format!(
        "\
Usage: {program} [OPTIONS] [FILTERS...]

Options:
        --include-ignored 
                        Run ignored and not ignored tests
        --ignored       Run only ignored tests
        --force-run-in-process 
                        Forces tests to run in-process when panic=abort
        --exclude-should-panic 
                        Excludes tests marked as should_panic
        --test          Run tests and not benchmarks
        --bench         Run benchmarks instead of tests
        --list          List all tests and benchmarks
        --fail-fast     Don't start new tests after the first failure
    -h, --help          Display this message
        --logfile PATH  Write logs to the specified file (deprecated)
        --no-capture    don't capture stdout/stderr of each task, allow
                        printing directly
        --test-threads n_threads
                        Number of threads used for running tests in parallel
        --skip FILTER   Skip tests whose names contain FILTER (this flag can
                        be used multiple times)
    -q, --quiet         Display one character per test instead of one line.
                        Alias to --format=terse
        --exact         Exactly match filters rather than by substring
        --color auto|always|never
                        Configure coloring of output:
                        auto = colorize if stdout is a tty and tests are run
                        on serially (default);
                        always = always colorize output;
                        never = never colorize output;
        --format pretty|terse|json|junit
                        Configure formatting of output:
                        pretty = Print verbose output;
                        terse = Display one character per test;
                        json = Output a json document;
                        junit = Output a JUnit document
        --show-output   Show captured stdout of successful tests
    -Z unstable-options Enable nightly-only flags:
                        unstable-options = Allow use of experimental features
        --report-time   Show execution time of each test.
        --ensure-time   Treat excess of the test execution time limit as
                        error.
        --shuffle       Run tests in random order
        --shuffle-seed SEED
                        Run tests in random order; seed the random number
                        generator with SEED


The FILTER string is tested against the name of all tests, and only those
tests whose names contain the filter are run. Multiple filter strings may
be passed, which will run all tests matching any of the filters.
"
    )
}

impl LibtestUserArgs {
    fn parse(args: &[String]) -> Self {
        let mut parsed = Self::default();
        let mut i = 0;
        while i < args.len() {
            let arg = args[i].as_str();
            if let Some(value) = arg.strip_prefix("--skip=") {
                parsed.push_skip(value);
                i += 1;
                continue;
            }
            if let Some(value) = arg.strip_prefix("--logfile=") {
                parsed.push_execution_value("--logfile", value);
                i += 1;
                continue;
            }
            if arg.starts_with("--test-threads=") || arg.starts_with("--color=") {
                i += 1;
                continue;
            }
            if let Some(value) = arg.strip_prefix("--format=") {
                parsed.set_output_format(value);
                i += 1;
                continue;
            }
            if let Some(value) = arg.strip_prefix("--filter=") {
                parsed.push_filter(value);
                i += 1;
                continue;
            }
            if let Some(value) = arg.strip_prefix("--shuffle-seed=") {
                parsed.push_unstable_execution_value("--shuffle-seed", value);
                i += 1;
                continue;
            }
            if let Some(value) = arg.strip_prefix("-Z") {
                if value.is_empty() {
                    if let Some(flag) = args.get(i + 1) {
                        parsed.note_unstable_option(flag);
                        i += 2;
                    } else {
                        i += 1;
                    }
                } else {
                    parsed.note_unstable_option(value);
                    i += 1;
                }
                continue;
            }

            match arg {
                "--include-ignored" => {
                    parsed.ignored_policies.push(IgnoredPolicy::IncludeIgnored);
                    i += 1;
                }
                "--ignored" => {
                    parsed.ignored_policies.push(IgnoredPolicy::IgnoredOnly);
                    i += 1;
                }
                "--exact" => {
                    parsed.listing_args.push(arg.to_owned());
                    parsed.whole_target_execution_args.push(arg.to_owned());
                    i += 1;
                }
                "--skip" => {
                    if let Some(value) = args.get(i + 1) {
                        parsed.push_skip(value);
                    }
                    i += 2;
                }
                "--list" => {
                    parsed.listing_only = true;
                    i += 1;
                }
                "--filter" => {
                    if let Some(value) = args.get(i + 1) {
                        parsed.push_filter(value);
                    }
                    i += 2;
                }
                "--test" | "--bench" => {
                    parsed.push_listing_and_execution_flag(arg);
                    i += 1;
                }
                "--exclude-should-panic" => {
                    parsed.listing_needs_unstable = true;
                    parsed.execution_needs_unstable = true;
                    parsed.push_listing_and_execution_flag(arg);
                    i += 1;
                }
                "--force-run-in-process" | "--no-capture" | "--nocapture" | "--show-output" => {
                    parsed.push_execution_flag(arg);
                    i += 1;
                }
                "--fail-fast" | "--report-time" | "--ensure-time" | "--shuffle" => {
                    parsed.execution_needs_unstable = true;
                    parsed.push_execution_flag(arg);
                    i += 1;
                }
                "--shuffle-seed" => {
                    if let Some(value) = args.get(i + 1) {
                        parsed.push_unstable_execution_value(arg, value);
                    }
                    i += 2;
                }
                "--logfile" => {
                    if let Some(value) = args.get(i + 1) {
                        parsed.push_execution_value(arg, value);
                    }
                    i += 2;
                }
                "--format" => {
                    if let Some(value) = args.get(i + 1) {
                        parsed.set_output_format(value);
                    }
                    i += 2;
                }
                "--test-threads" | "--color" => {
                    i += 2;
                }
                "-q" | "--quiet" => {
                    parsed.output_format = LibtestOutputFormat::Terse;
                    i += 1;
                }
                "-h" | "--help" => {
                    parsed.help = true;
                    i += 1;
                }
                "--" => {
                    for filter in &args[i + 1..] {
                        parsed.push_filter(filter);
                    }
                    break;
                }
                flag if flag.starts_with('-') => {
                    parsed.push_execution_flag(flag);
                    i += 1;
                }
                filter => {
                    parsed.push_filter(filter);
                    i += 1;
                }
            }
        }
        parsed
    }

    fn push_filter(&mut self, value: &str) {
        self.listing_args.push(value.to_owned());
        self.whole_target_execution_args.push(value.to_owned());
    }

    fn push_skip(&mut self, value: &str) {
        self.listing_args.push("--skip".to_owned());
        self.listing_args.push(value.to_owned());
        self.whole_target_execution_args.push("--skip".to_owned());
        self.whole_target_execution_args.push(value.to_owned());
    }

    fn push_execution_flag(&mut self, flag: &str) {
        self.per_test_execution_args.push(flag.to_owned());
        self.whole_target_execution_args.push(flag.to_owned());
    }

    fn push_listing_and_execution_flag(&mut self, flag: &str) {
        self.listing_args.push(flag.to_owned());
        self.push_execution_flag(flag);
    }

    fn push_execution_value(&mut self, flag: &str, value: &str) {
        self.per_test_execution_args.push(flag.to_owned());
        self.per_test_execution_args.push(value.to_owned());
        self.whole_target_execution_args.push(flag.to_owned());
        self.whole_target_execution_args.push(value.to_owned());
    }

    fn push_unstable_execution_value(&mut self, flag: &str, value: &str) {
        self.execution_needs_unstable = true;
        self.push_execution_value(flag, value);
    }

    fn note_unstable_option(&mut self, value: &str) {
        self.execution_needs_unstable = true;
        if value == "unstable-options" {
            self.unstable_options = true;
        }
    }

    fn set_output_format(&mut self, value: &str) {
        match value {
            "pretty" => self.output_format = LibtestOutputFormat::Pretty,
            "terse" => self.output_format = LibtestOutputFormat::Terse,
            "json" => self.output_format = LibtestOutputFormat::Json,
            "junit" => self.output_format = LibtestOutputFormat::Junit,
            other => {
                self.format_error = Some(format!(
                    "argument for --format must be pretty, terse, json or junit (was {other})",
                ));
            }
        }
    }

    fn list_output_format(&self) -> LibtestListOutputFormat {
        match self.output_format {
            LibtestOutputFormat::Pretty | LibtestOutputFormat::Junit => {
                LibtestListOutputFormat::Text { summary: true }
            }
            LibtestOutputFormat::Terse => LibtestListOutputFormat::Text { summary: false },
            LibtestOutputFormat::Json => LibtestListOutputFormat::Json,
        }
    }
}

#[derive(Deserialize)]
struct RunEvent {
    #[serde(rename = "type")]
    kind: String,
    name: Option<String>,
    event: Option<String>,
    #[serde(default)]
    stdout: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// Parse libtest `--format json` run output: one object per line, keeping
/// `{"type":"test","name":..,"event":"ok|failed|ignored",...}`.
fn decode_json(stdout: &str) -> FxHashMap<String, PerTestObservation> {
    let mut out = FxHashMap::default();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<RunEvent>(line) else {
            continue;
        };
        if event.kind == "bench" {
            if let Some(name) = event.name {
                out.insert(
                    name,
                    PerTestObservation {
                        status: TestVerdict::Pass,
                        details: String::new(),
                    },
                );
            }
            continue;
        }
        if event.kind != "test" {
            continue;
        }
        let (Some(name), Some(ev)) = (event.name, event.event) else {
            continue;
        };
        let status = match ev.as_str() {
            "ok" => TestVerdict::Pass,
            "failed" => TestVerdict::Fail,
            "ignored" => TestVerdict::Skip,
            // "started" is not terminal; anything else is unexpected.
            "started" => continue,
            _ => TestVerdict::Fatal,
        };
        let details = event.stdout.or(event.message).unwrap_or_default();
        out.insert(name, PerTestObservation { status, details });
    }
    out
}

/// Parse stable libtest text run output: `test <name> ... <status>` lines, with
/// failure detail blocks attached to failed tests.
fn decode_text(stdout: &str, stderr: &str) -> FxHashMap<String, PerTestObservation> {
    let mut out = FxHashMap::default();
    for line in stdout.lines() {
        let Some(rest) = line.strip_prefix("test ") else {
            continue;
        };
        let Some((name, status_token)) = rest.rsplit_once(" ... ") else {
            continue;
        };
        let status_token = status_token.trim();
        let status = if status_token.starts_with("bench:") {
            TestVerdict::Pass
        } else {
            match status_token.split_whitespace().next().unwrap_or_default() {
                "ok" => TestVerdict::Pass,
                "FAILED" => TestVerdict::Fail,
                "ignored" => TestVerdict::Skip,
                _ => continue,
            }
        };
        let details = if status == TestVerdict::Fail {
            extract_failure_block(stdout, name).unwrap_or_else(|| {
                if stderr.is_empty() {
                    String::new()
                } else {
                    format!("---- stderr ----\n{stderr}")
                }
            })
        } else {
            String::new()
        };
        out.insert(name.to_owned(), PerTestObservation { status, details });
    }
    out
}

/// Pull the `---- <name> stdout ----` failure block out of text run output.
fn extract_failure_block(stdout: &str, name: &str) -> Option<String> {
    let header = format!("---- {name} stdout ----");
    let start = stdout.find(&header)?;
    // `find` returns a char boundary and `header.len()` lands on one too, so the
    // tail always exists; `get` keeps the slice UTF-8-safe by construction.
    let after = stdout.get(start + header.len()..)?;
    // The block ends at the next "---- " header or the "failures:" footer.
    let end = after
        .find("\n---- ")
        .or_else(|| after.find("\nfailures:"))
        .unwrap_or(after.len());
    Some(after.get(..end)?.trim().to_owned())
}

pub struct LibtestTranslator {
    list_format: ListFormat,
    run_format: RunFormat,
}

impl Translator for LibtestTranslator {
    fn declares_executor_overrides(&self) -> bool {
        false
    }
    fn parser_capability(&self) -> DemuxCapability {
        DemuxCapability::NameAttributable
    }
    fn listing_strategy(&self) -> ListingStrategy {
        let list_format = self.list_format;
        ListingStrategy::PerTestListing {
            request_args: Box::new(move |ignored, user_args| {
                libtest_listing_args(ignored, list_format, user_args)
            }),
            parse: Box::new(move |stdout, ignored| parse_listing(list_format, stdout, ignored)),
        }
    }
    fn execution_args(
        &self,
        names: &[&str],
        ignored: IgnoredPolicy,
        user_args: &[String],
    ) -> Vec<String> {
        libtest_execution_args(names, ignored, self.run_format, user_args)
    }
    fn parse_results(&self, stdout: &[u8], stderr: &[u8]) -> FxHashMap<String, PerTestObservation> {
        libtest_decode(self.run_format, stdout, stderr)
    }
}

pub struct DoctestTranslator {
    run_format: RunFormat,
}

impl Translator for DoctestTranslator {
    fn declares_executor_overrides(&self) -> bool {
        false
    }
    fn parser_capability(&self) -> DemuxCapability {
        DemuxCapability::SingletonOnly
    }
    fn listing_strategy(&self) -> ListingStrategy {
        ListingStrategy::WholeTarget {
            name: DOCTEST_RESULT_NAME,
        }
    }
    fn execution_args(
        &self,
        _names: &[&str],
        ignored: IgnoredPolicy,
        user_args: &[String],
    ) -> Vec<String> {
        doctest_execution_args(ignored, self.run_format, user_args)
    }
    fn parse_results(&self, stdout: &[u8], stderr: &[u8]) -> FxHashMap<String, PerTestObservation> {
        libtest_decode(self.run_format, stdout, stderr)
    }
}

pub struct CustomBinaryTranslator;

impl Translator for CustomBinaryTranslator {
    fn declares_executor_overrides(&self) -> bool {
        true
    }
    fn parser_capability(&self) -> DemuxCapability {
        DemuxCapability::SingletonOnly
    }
    fn listing_strategy(&self) -> ListingStrategy {
        ListingStrategy::WholeBinary {
            name: BINARY_RESULT_NAME,
        }
    }
    fn execution_args(
        &self,
        _names: &[&str],
        _ignored: IgnoredPolicy,
        user_args: &[String],
    ) -> Vec<String> {
        let mut args = custom_binary_execution_args();
        args.extend(user_args.iter().cloned());
        args
    }
    fn parse_results(
        &self,
        _stdout: &[u8],
        _stderr: &[u8],
    ) -> FxHashMap<String, PerTestObservation> {
        custom_binary_decode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn libtest_list_output_uses_stable_text_by_default() {
        let tests = vec![
            TestCase::new("alpha_one".to_owned(), TestCaseKind::Test, false),
            TestCase::new("bench_five".to_owned(), TestCaseKind::Benchmark, false),
        ];

        assert_eq!(
            libtest_list_output(&["--list".to_owned()], &tests),
            "alpha_one: test\nbench_five: benchmark\n\n1 test, 1 benchmark\n"
        );
    }

    #[test]
    fn libtest_list_output_can_emit_json() {
        let tests = vec![TestCase {
            name: "alpha_one".to_owned(),
            kind: TestCaseKind::Test,
            ignored: true,
            metadata: TestCaseMetadata {
                ignore_message: Some("slow".to_owned()),
                location: Some(TestCaseLocation {
                    source_path: "src/lib.rs".to_owned(),
                    start_line: 10,
                    start_col: 8,
                    end_line: 10,
                    end_col: 17,
                }),
            },
        }];

        let output = libtest_list_output(
            &[
                "--list".to_owned(),
                "--format=json".to_owned(),
                "-Z".to_owned(),
                "unstable-options".to_owned(),
            ],
            &tests,
        );
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 3);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(lines[0]).unwrap(),
            serde_json::json!({ "type": "suite", "event": "discovery" })
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(lines[1]).unwrap(),
            serde_json::json!({
                "type": "test",
                "event": "discovered",
                "name": "alpha_one",
                "ignore": true,
                "ignore_message": "slow",
                "source_path": "src/lib.rs",
                "start_line": 10,
                "start_col": 8,
                "end_line": 10,
                "end_col": 17,
            })
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(lines[2]).unwrap(),
            serde_json::json!({
                "type": "suite",
                "event": "completed",
                "tests": 1,
                "benchmarks": 0,
                "total": 1,
                "ignored": 1,
            })
        );
    }

    #[test]
    fn libtest_list_output_terse_omits_summary() {
        let tests = vec![TestCase::new(
            "alpha_one".to_owned(),
            TestCaseKind::Test,
            false,
        )];

        assert_eq!(
            libtest_list_output(&["--list".to_owned(), "--format=terse".to_owned()], &tests,),
            "alpha_one: test\n"
        );
    }

    #[test]
    fn libtest_list_output_quiet_omits_summary() {
        let tests = vec![TestCase::new(
            "alpha_one".to_owned(),
            TestCaseKind::Test,
            false,
        )];

        assert_eq!(
            libtest_list_output(&["--list".to_owned(), "--quiet".to_owned()], &tests),
            "alpha_one: test\n"
        );
    }

    #[test]
    fn libtest_list_output_junit_uses_text_list_with_summary() {
        let tests = vec![TestCase::new(
            "alpha_one".to_owned(),
            TestCaseKind::Test,
            false,
        )];

        assert_eq!(
            libtest_list_output(
                &[
                    "--list".to_owned(),
                    "--format=junit".to_owned(),
                    "-Z".to_owned(),
                    "unstable-options".to_owned(),
                ],
                &tests,
            ),
            "alpha_one: test\n\n1 test, 0 benchmarks\n"
        );
    }

    #[test]
    fn libtest_filter_flag_selects_listing_without_reaching_execution() {
        let user_args = vec![
            "--filter".to_owned(),
            "alpha::one".to_owned(),
            "--exact".to_owned(),
            "--nocapture".to_owned(),
        ];

        assert_eq!(
            libtest_listing_args(IgnoredPolicy::ExcludeIgnored, ListFormat::Json, &user_args),
            vec![
                "-Z".to_owned(),
                "unstable-options".to_owned(),
                "--list".to_owned(),
                "--format".to_owned(),
                "json".to_owned(),
                "alpha::one".to_owned(),
                "--exact".to_owned(),
            ]
        );
        assert_eq!(
            libtest_execution_args(
                &["alpha::one"],
                IgnoredPolicy::ExcludeIgnored,
                RunFormat::Json,
                &user_args,
            ),
            vec![
                "alpha::one".to_owned(),
                "--exact".to_owned(),
                "--test-threads=1".to_owned(),
                "--color=never".to_owned(),
                "-Z".to_owned(),
                "unstable-options".to_owned(),
                "--format".to_owned(),
                "json".to_owned(),
                "--nocapture".to_owned(),
            ]
        );
    }

    #[test]
    fn libtest_filter_equals_shape_selects_listing() {
        assert_eq!(
            libtest_listing_args(
                IgnoredPolicy::ExcludeIgnored,
                ListFormat::Text,
                &["--filter=alpha".to_owned()],
            ),
            vec!["--list".to_owned(), "alpha".to_owned()]
        );
    }

    #[test]
    fn listing_parse_preserves_test_case_kind() {
        let text_cases = parse_listing(
            ListFormat::Text,
            b"alpha_one: test\nbench_five: benchmark\n",
            IgnoredPolicy::ExcludeIgnored,
        )
        .unwrap();
        assert_eq!(
            text_cases,
            vec![
                TestCase::new("alpha_one".to_owned(), TestCaseKind::Test, false),
                TestCase::new("bench_five".to_owned(), TestCaseKind::Benchmark, false),
            ]
        );

        let json_cases = parse_listing(
            ListFormat::Json,
            br#"{ "type": "benchmark", "event": "discovered", "name": "bench_five", "ignore": false }"#,
            IgnoredPolicy::ExcludeIgnored,
        )
        .unwrap();
        assert_eq!(
            json_cases,
            vec![TestCase::new(
                "bench_five".to_owned(),
                TestCaseKind::Benchmark,
                false,
            )]
        );
    }

    #[test]
    fn json_benchmark_event_is_a_passing_observation() {
        let output = r#"{ "type": "suite", "event": "started", "test_count": 1 }
{ "type": "test", "event": "started", "name": "bench_five" }
{ "type": "bench", "name": "bench_five", "median": 1.0, "deviation": 0.0 }
{ "type": "suite", "event": "ok", "passed": 0, "failed": 0, "ignored": 0, "measured": 1, "filtered_out": 0 }
"#;
        let observations = libtest_decode(RunFormat::Json, output.as_bytes(), b"");
        assert_eq!(
            observations.get("bench_five"),
            Some(&PerTestObservation {
                status: TestVerdict::Pass,
                details: String::new()
            })
        );
    }

    #[test]
    fn text_benchmark_and_report_time_lines_are_passing_observations() {
        let output = "\
running 2 tests
test alpha_one ... ok <0.001s>
test bench_five ... bench:           1.78 ns/iter (+/- 0.83)

test result: ok. 1 passed; 0 failed; 0 ignored; 1 measured; 0 filtered out; finished in 0.00s
";
        let observations = libtest_decode(RunFormat::Text, output.as_bytes(), b"");
        assert_eq!(
            observations.get("alpha_one"),
            Some(&PerTestObservation {
                status: TestVerdict::Pass,
                details: String::new()
            })
        );
        assert_eq!(
            observations.get("bench_five"),
            Some(&PerTestObservation {
                status: TestVerdict::Pass,
                details: String::new()
            })
        );
    }
}
