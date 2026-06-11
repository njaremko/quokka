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

use crate::listing::{IgnoredPolicy, ListingParseError, TestCase};
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
            let name = line
                .strip_suffix(": test")
                .or_else(|| line.strip_suffix(": benchmark"))?;
            if name.is_empty() {
                return None;
            }
            Some(crate::listing::TestCase {
                name: name.to_owned(),
                ignored: false,
            })
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
            if event.kind != "test" && event.kind != "benchmark" {
                return None;
            }
            // discovery uses event=="discovered"; accept missing event too.
            if event.event.as_deref().is_some_and(|ev| ev != "discovered") {
                return None;
            }
            Some(crate::listing::TestCase {
                name: event.name?,
                ignored: event.ignore,
            })
        })
        .collect()
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
    listing_needs_unstable: bool,
    execution_needs_unstable: bool,
}

pub fn libtest_user_ignored_policies(args: &[String]) -> Vec<IgnoredPolicy> {
    LibtestUserArgs::parse(args).ignored_policies
}

pub fn libtest_user_requests_listing_only(args: &[String]) -> bool {
    LibtestUserArgs::parse(args).listing_only
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
            if arg.starts_with("--test-threads=")
                || arg.starts_with("--color=")
                || arg.starts_with("--format=")
            {
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
                    if args.get(i + 1).is_some() {
                        parsed.execution_needs_unstable = true;
                        i += 2;
                    } else {
                        i += 1;
                    }
                } else {
                    parsed.execution_needs_unstable = true;
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
                "--test-threads" | "--color" | "--format" => {
                    i += 2;
                }
                "-q" | "--quiet" | "-h" | "--help" => {
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
