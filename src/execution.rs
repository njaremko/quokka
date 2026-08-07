//! Building `Execute2` requests for listing and test actions.
//!
//! The target's base command and env are *handles* (and verbatim values) that
//! buck2 expands at execute time — re-materializing artifacts and hidden inputs.
//! We therefore echo every spec handle back UNMODIFIED inside
//! `ArgValueContent::SpecValue`, and append only our own verbatim flags (with
//! `format: None`). Touching a handle would strip the test of its artifact
//! dependencies and it would fail to find its own binary.

use std::num::NonZeroU64;
use std::time::Duration;

use crate::environment::{HardwareNeeds, HostExclusivity, SchedulingProfile};
use crate::proto::test::arg_value_content::Value as ArgContent;
use crate::proto::test::external_runner_spec_value::Value as SpecValue;
use crate::proto::test::test_stage::{Item as StageItem, Listing};
use crate::proto::test::{
    ArgValue, ArgValueContent, ConfiguredTargetHandle, EnvironmentVariable, ExecuteRequest2,
    ExecutorConfigOverride, ExternalRunnerSpecValue, LocalResourceType, OutputName, TestExecutable,
    TestStage, Testing,
};
use crate::spec::{CommandArg, TargetSpec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CgroupGranularity {
    LogicalTest,
    Action,
}

pub const DEFAULT_CGROUP_MEMORY_MAX_BYTES: u64 = 1_572_864_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CgroupMemoryMax {
    Bytes(NonZeroU64),
    Unlimited,
}

impl CgroupMemoryMax {
    pub fn parse(value: &str) -> Option<Self> {
        if value == "max" {
            return Some(Self::Unlimited);
        }
        value.parse::<NonZeroU64>().ok().map(Self::Bytes)
    }

    fn shell_value(self) -> String {
        match self {
            Self::Bytes(bytes) => bytes.to_string(),
            Self::Unlimited => "max".to_owned(),
        }
    }
}

impl Default for CgroupMemoryMax {
    fn default() -> Self {
        Self::Bytes(NonZeroU64::new(DEFAULT_CGROUP_MEMORY_MAX_BYTES).unwrap())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupMemoryMaxOverride {
    pub target: String,
    pub test: String,
    pub memory_max: CgroupMemoryMax,
}

impl CgroupMemoryMaxOverride {
    pub fn parse(value: &str) -> Option<Self> {
        let (target, rest) = value.split_once('|')?;
        let (test, memory_max) = rest.split_once('|')?;
        if target.is_empty() || test.is_empty() || memory_max.contains('|') {
            return None;
        }
        Some(Self {
            target: target.to_owned(),
            test: test.to_owned(),
            memory_max: CgroupMemoryMax::parse(memory_max)?,
        })
    }
}

pub fn cgroup_memory_max_for_action(
    default: CgroupMemoryMax,
    overrides: &[CgroupMemoryMaxOverride],
    target: &str,
    tests: &[String],
) -> Result<CgroupMemoryMax, String> {
    let matches: Vec<&CgroupMemoryMaxOverride> = overrides
        .iter()
        .filter(|candidate| {
            candidate.target == target && tests.iter().any(|test| test == &candidate.test)
        })
        .collect();
    match matches.as_slice() {
        [] => Ok(default),
        [matched] if tests.len() == 1 => Ok(matched.memory_max),
        [_] => Err("a cgroup memory override requires a singleton test action".to_owned()),
        _ => Err("multiple cgroup memory overrides match one action".to_owned()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceMarkerToken(String);

impl ResourceMarkerToken {
    pub fn from_stable_fields<'a>(
        domain: &'a str,
        fields: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let mut hash = 0xcbf29ce484222325u64;
        for field in std::iter::once(domain).chain(fields) {
            for byte in field.len().to_le_bytes().iter().chain(field.as_bytes()) {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
        }
        Self(format!("{hash:016x}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl CgroupGranularity {
    fn shell_value(self) -> &'static str {
        match self {
            CgroupGranularity::LogicalTest => "logical-test",
            CgroupGranularity::Action => "action",
        }
    }
}

pub struct LogicalCommand {
    pub index: usize,
    pub appended: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceFailure {
    LogicalTimeout { index: usize, details: String },
    LogicalOom { index: usize, details: String },
    ActionTimeout { details: String },
    ActionOom { details: String },
    Setup { details: String },
    Malformed { details: String },
}

/// Convert a pure `CommandArg` back into the protobuf `ArgValue`.
fn echo_spec_value(arg: &CommandArg) -> ArgValue {
    let value = match arg {
        CommandArg::Verbatim(s) => SpecValue::Verbatim(s.clone()),
        CommandArg::ArgHandle(i) => SpecValue::ArgHandle(*i),
        CommandArg::EnvHandle(s) => SpecValue::EnvHandle(s.clone()),
    };
    ArgValue {
        content: Some(ArgValueContent {
            value: Some(ArgContent::SpecValue(ExternalRunnerSpecValue {
                value: Some(value),
            })),
        }),
        format: None,
    }
}

/// Wrap a runner-appended verbatim flag as an `ArgValue` (never sets a format).
fn appended_verbatim(arg: &str) -> ArgValue {
    ArgValue {
        content: Some(ArgValueContent {
            value: Some(ArgContent::SpecValue(ExternalRunnerSpecValue {
                value: Some(SpecValue::Verbatim(arg.to_owned())),
            })),
        }),
        format: None,
    }
}

fn declared_output_value(output_name: &str) -> ArgValue {
    ArgValue {
        content: Some(ArgValueContent {
            value: Some(ArgContent::DeclaredOutput(OutputName {
                name: output_name.to_owned(),
            })),
        }),
        format: None,
    }
}

/// Build the full command: the spec's handles/verbatims first, then appended flags.
pub fn build_cmd(spec: &TargetSpec, appended: &[String]) -> Vec<ArgValue> {
    let mut cmd = Vec::with_capacity(spec.command.len() + appended.len());
    cmd.extend(spec.command.iter().map(echo_spec_value));
    cmd.extend(appended.iter().map(|a| appended_verbatim(a)));
    cmd
}

pub fn build_supervised_cmd(
    base_command: Vec<ArgValue>,
    commands: Vec<LogicalCommand>,
    timeout: Duration,
    cgroup_granularity: CgroupGranularity,
    cgroup_memory_max: CgroupMemoryMax,
    resource_marker_token: &ResourceMarkerToken,
) -> Vec<ArgValue> {
    if commands.is_empty() {
        return base_command;
    }
    let mut script = format!(
        "{}\nquokka_cgroup_granularity={}\nquokka_logical_timeout_seconds={}\nquokka_memory_max_value={}\nquokka_resource_marker_token={}\noverall_status=0\nquokka_begin_action\n",
        include_str!("resource_supervisor.sh"),
        cgroup_granularity.shell_value(),
        timeout.as_secs(),
        shell_quote(&cgroup_memory_max.shell_value()),
        shell_quote(resource_marker_token.as_str()),
    );
    for logical in commands {
        script.push_str(&format!("quokka_run_logical {} \"$@\"", logical.index));
        for arg in logical.appended {
            script.push(' ');
            script.push_str(&shell_quote(&arg));
        }
        script.push_str(" || overall_status=$?\n");
    }
    script.push_str("quokka_finish_action \"$overall_status\"\n");

    let mut command = vec![
        appended_verbatim("/bin/sh"),
        appended_verbatim("-c"),
        appended_verbatim(&script),
        appended_verbatim("quokka-resource-supervisor"),
    ];
    command.extend(base_command);
    command
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub fn supervised_action_timeout(logical_timeout: Duration, logical_count: usize) -> Duration {
    let logical_count = u32::try_from(logical_count).unwrap_or(u32::MAX);
    logical_timeout
        .saturating_mul(logical_count)
        .saturating_add(Duration::from_secs(60))
}

pub fn parse_resource_failures(
    stderr: &[u8],
    resource_marker_token: &ResourceMarkerToken,
) -> Vec<ResourceFailure> {
    let prefix = format!("quokka resource {} ", resource_marker_token.as_str());
    String::from_utf8_lossy(stderr)
        .lines()
        .filter_map(|line| {
            let marker = line.strip_prefix(&prefix)?;
            if let Some(rest) = marker.strip_prefix("logical test timeout:") {
                return parse_logical_failure(rest, marker, true);
            }
            if let Some(rest) = marker.strip_prefix("logical test cgroup OOM:") {
                return parse_logical_failure(rest, marker, false);
            }
            if marker.starts_with("test action timeout: ") {
                return Some(ResourceFailure::ActionTimeout {
                    details: format!("quokka {marker}"),
                });
            }
            if marker.starts_with("test action cgroup OOM: ") {
                return Some(ResourceFailure::ActionOom {
                    details: format!("quokka {marker}"),
                });
            }
            if marker.starts_with("cgroup setup failure: ") {
                return Some(ResourceFailure::Setup {
                    details: format!("quokka {marker}"),
                });
            }
            None
        })
        .collect()
}

fn parse_logical_failure(rest: &str, line: &str, timeout: bool) -> Option<ResourceFailure> {
    let details = format!("quokka {line}");
    let Some(raw_index) = rest
        .split_whitespace()
        .find_map(|token| token.strip_prefix("index="))
    else {
        return Some(ResourceFailure::Malformed { details });
    };
    let index = match raw_index.parse() {
        Ok(index) => index,
        Err(_) => return Some(ResourceFailure::Malformed { details }),
    };
    if timeout {
        Some(ResourceFailure::LogicalTimeout { index, details })
    } else {
        Some(ResourceFailure::LogicalOom { index, details })
    }
}

/// Build the environment: the spec's value handles echoed unmodified, then the
/// runner's `--env NAME=VALUE` additions as verbatim values. Runner additions are
/// appended last so they take precedence (buck2 applies env in order), honoring
/// the "added to every test" contract.
pub fn build_env(spec: &TargetSpec, extra: &[(String, String)]) -> Vec<EnvironmentVariable> {
    let mut env: Vec<EnvironmentVariable> = spec
        .env
        .iter()
        .map(|(key, value)| EnvironmentVariable {
            key: key.clone(),
            value: Some(echo_spec_value(value)),
        })
        .collect();
    for (key, value) in extra {
        env.push(EnvironmentVariable {
            key: key.clone(),
            value: Some(appended_verbatim(value)),
        });
    }
    env
}

pub fn build_test_env(
    spec: &TargetSpec,
    extra: &[(String, String)],
    declared_output_env: &[(String, String)],
) -> Vec<EnvironmentVariable> {
    let mut env = build_env(spec, extra);
    for (key, output_name) in declared_output_env {
        env.push(EnvironmentVariable {
            key: key.clone(),
            value: Some(declared_output_value(output_name)),
        });
    }
    env
}

fn duration_to_proto(d: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

fn executor_override_proto(hardware: &HardwareNeeds) -> Option<ExecutorConfigOverride> {
    if hardware.local_debug {
        Some(ExecutorConfigOverride {
            name: "rust-local-debug".into(),
        })
    } else if hardware.listing_only {
        Some(ExecutorConfigOverride {
            name: "rust-listing".into(),
        })
    } else if hardware.requires_gpu {
        Some(ExecutorConfigOverride {
            name: "rust-test-gpu".into(),
        })
    } else if hardware.requires_large_mem {
        Some(ExecutorConfigOverride {
            name: "rust-test-large".into(),
        })
    } else if hardware.network_isolated {
        Some(ExecutorConfigOverride {
            name: "rust-test-network-private".into(),
        })
    } else {
        None
    }
}

fn host_sharing_proto(
    exclusivity: HostExclusivity,
) -> crate::proto::host_sharing::HostSharingRequirements {
    use crate::proto::host_sharing::host_sharing_requirements::{
        ExclusiveAccess, Requirements, Shared,
    };
    use crate::proto::host_sharing::weight_class::Value as WeightValue;
    use crate::proto::host_sharing::{HostSharingRequirements, WeightClass};

    let requirements = match exclusivity {
        HostExclusivity::Shared => Requirements::Shared(Shared {
            weight_class: Some(WeightClass {
                value: Some(WeightValue::Permits(1)),
            }),
        }),
        HostExclusivity::Exclusive => Requirements::ExclusiveAccess(ExclusiveAccess {}),
    };
    HostSharingRequirements {
        requirements: Some(requirements),
    }
}

/// Build a `Listing` stage `Execute2` request for a target.
///
/// The listing is always issued `cacheable: false` under the current runner
/// policy. Its stdout is parsed as the set of test names before Testing actions
/// are constructed.
pub fn build_listing_request(
    spec: &TargetSpec,
    listing_args: &[String],
    profile: &SchedulingProfile,
    timeout: Duration,
    cgroup_granularity: CgroupGranularity,
    cgroup_memory_max: CgroupMemoryMax,
    resource_marker_token: &ResourceMarkerToken,
    extra_env: &[(String, String)],
) -> ExecuteRequest2 {
    let stage = TestStage {
        item: Some(StageItem::Listing(Listing {
            suite: spec.suite.clone(),
            cacheable: false,
        })),
    };
    let test_executable = TestExecutable {
        stage: Some(stage),
        target: Some(ConfiguredTargetHandle { id: spec.handle.0 }),
        cmd: build_supervised_cmd(
            build_cmd(spec, &[]),
            vec![LogicalCommand {
                index: 0,
                appended: listing_args.to_vec(),
            }],
            timeout,
            cgroup_granularity,
            cgroup_memory_max,
            resource_marker_token,
        ),
        pre_create_dirs: Vec::new(),
        env: build_env(spec, extra_env),
    };
    ExecuteRequest2 {
        timeout: Some(duration_to_proto(supervised_action_timeout(timeout, 1))),
        host_sharing_requirements: Some(host_sharing_proto(profile.exclusivity)),
        test_executable: Some(test_executable),
        executor_override: executor_override_proto(&profile.hardware),
        required_local_resources: profile
            .local_resources
            .iter()
            .map(|r| LocalResourceType { name: r.clone() })
            .collect(),
        // This field is the *test-execution* caching toggle (the Testing stage);
        // the listing's own cacheability is the `cacheable: false` above. Leave
        // this at its default — it is inert for a Listing stage.
        disable_test_execution_caching: false,
    }
}

/// Inputs for a single test-execution action.
pub struct TestingRequest {
    pub target: ConfiguredTargetHandle,
    pub suite: String,
    pub testcases: Vec<String>,
    pub base_command: Vec<ArgValue>,
    pub commands: Vec<LogicalCommand>,
    pub env: Vec<EnvironmentVariable>,
    pub variant: Option<String>,
    pub repeat_count: Option<u64>,
    pub profile: SchedulingProfile,
    pub caching: crate::caching::TestExecutionCaching,
    pub logical_timeout: Duration,
    pub cgroup_granularity: CgroupGranularity,
    pub cgroup_memory_max: CgroupMemoryMax,
    pub resource_marker_token: ResourceMarkerToken,
}

/// Build a `Testing` stage `Execute2` request.
pub fn build_testing_request(req: TestingRequest) -> ExecuteRequest2 {
    let logical_count = req.commands.len();
    let stage = TestStage {
        item: Some(StageItem::Testing(Testing {
            suite: req.suite,
            testcases: req.testcases,
            variant: req.variant,
            repeat_count: req.repeat_count,
        })),
    };
    let test_executable = TestExecutable {
        stage: Some(stage),
        target: Some(req.target),
        cmd: build_supervised_cmd(
            req.base_command,
            req.commands,
            req.logical_timeout,
            req.cgroup_granularity,
            req.cgroup_memory_max,
            &req.resource_marker_token,
        ),
        pre_create_dirs: Vec::new(),
        env: req.env,
    };
    ExecuteRequest2 {
        timeout: Some(duration_to_proto(supervised_action_timeout(
            req.logical_timeout,
            logical_count,
        ))),
        host_sharing_requirements: Some(host_sharing_proto(req.profile.exclusivity)),
        test_executable: Some(test_executable),
        executor_override: executor_override_proto(&req.profile.hardware),
        required_local_resources: req
            .profile
            .local_resources
            .iter()
            .map(|r| LocalResourceType { name: r.clone() })
            .collect(),
        disable_test_execution_caching: req.caching.disable_flag(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::test::external_runner_spec_value::Value;
    use crate::variant::Variant;
    use std::sync::Arc;

    fn spec_with_handle_command() -> Arc<TargetSpec> {
        Arc::new(TargetSpec {
            handle: crate::ids::TargetHandle(9),
            suite: "foo".into(),
            display: "root//x:foo".into(),
            target_platform: None,
            test_type: "rust".into(),
            command: vec![
                CommandArg::ArgHandle(3),
                CommandArg::Verbatim("--nocapture".into()),
            ]
            .into_boxed_slice(),
            env: vec![(
                "RUST_BACKTRACE".to_owned(),
                CommandArg::Verbatim("short".into()),
            )]
            .into_boxed_slice(),
            labels: vec![],
            contacts: vec![],
            oncall: None,
        })
    }

    fn logical(appended: Vec<String>) -> Vec<LogicalCommand> {
        vec![LogicalCommand { index: 0, appended }]
    }

    #[test]
    fn cgroup_memory_override_is_exact_and_singleton() {
        let override_value =
            CgroupMemoryMaxOverride::parse("root//x:tests|suite::large|3221225472").unwrap();
        let overrides = [override_value];
        let selected = vec!["suite::large".to_owned()];
        assert_eq!(
            cgroup_memory_max_for_action(
                CgroupMemoryMax::default(),
                &overrides,
                "root//x:tests",
                &selected,
            )
            .unwrap(),
            CgroupMemoryMax::Bytes(NonZeroU64::new(3_221_225_472).unwrap()),
        );
        assert_eq!(
            cgroup_memory_max_for_action(
                CgroupMemoryMax::default(),
                &overrides,
                "root//x:tests",
                &["suite::small".to_owned()],
            )
            .unwrap(),
            CgroupMemoryMax::default(),
        );
        assert!(CgroupMemoryMaxOverride::parse("root//x:tests||3221225472").is_none());
        assert!(CgroupMemoryMaxOverride::parse("root//x:tests|suite::large|0").is_none());
    }

    #[test]
    fn cgroup_memory_override_rejects_batched_action() {
        let overrides =
            [CgroupMemoryMaxOverride::parse("root//x:tests|suite::large|3221225472").unwrap()];
        let tests = vec!["suite::large".to_owned(), "suite::small".to_owned()];
        let result = cgroup_memory_max_for_action(
            CgroupMemoryMax::default(),
            &overrides,
            "root//x:tests",
            &tests,
        );
        assert_eq!(
            result,
            Err("a cgroup memory override requires a singleton test action".to_owned())
        );
    }

    #[test]
    fn echoes_handles_unmodified_and_appends_verbatim() {
        let spec = spec_with_handle_command();
        let req = build_testing_request(TestingRequest {
            target: ConfiguredTargetHandle { id: spec.handle.0 },
            suite: spec.suite.clone(),
            testcases: vec!["m::t".into()],
            base_command: build_cmd(&spec, &[]),
            commands: logical(vec!["m::t".into(), "--exact".into()]),
            env: build_env(&spec, &[("RUST_LOG".to_owned(), "debug".to_owned())]),
            variant: Variant::Default.identity(),
            repeat_count: None,
            profile: SchedulingProfile::default(),
            caching: crate::caching::TestExecutionCaching::Enabled,
            logical_timeout: std::time::Duration::from_secs(60),
            cgroup_granularity: CgroupGranularity::LogicalTest,
            cgroup_memory_max: CgroupMemoryMax::default(),
            resource_marker_token: ResourceMarkerToken("test-1".to_owned()),
        });
        let exec = req.test_executable.unwrap();
        // The runner-supplied --env appears as a verbatim env var after the
        // spec's own (RUST_BACKTRACE) handle.
        let rust_log = exec
            .env
            .iter()
            .find(|e| e.key == "RUST_LOG")
            .expect("extra env applied");
        match &rust_log
            .value
            .as_ref()
            .unwrap()
            .content
            .as_ref()
            .unwrap()
            .value
        {
            Some(ArgContent::SpecValue(ExternalRunnerSpecValue {
                value: Some(Value::Verbatim(v)),
            })) => assert_eq!(v, "debug"),
            _ => panic!("extra env not verbatim"),
        }
        let cmd = exec.cmd;
        // The supervised command still echoes the ArgHandle(3) unmodified.
        match &cmd[4].content.as_ref().unwrap().value {
            Some(ArgContent::SpecValue(ExternalRunnerSpecValue {
                value: Some(Value::ArgHandle(h)),
            })) => assert_eq!(*h, 3),
            _ => panic!("handle not echoed"),
        }
        // Appended verbatim args carry no format.
        assert!(cmd.iter().all(|a| a.format.is_none()));
        // host sharing populated.
        assert!(req.host_sharing_requirements.is_some());
        assert!(!req.disable_test_execution_caching);
    }

    #[test]
    fn declared_output_env_uses_buck_output_handle() {
        let spec = spec_with_handle_command();
        let declared_output_env = vec![(
            "NOBIE_RUST_TEST_ARTIFACTS_ROOT".to_owned(),
            "rust-test-artifacts".to_owned(),
        )];
        let req = build_testing_request(TestingRequest {
            target: ConfiguredTargetHandle { id: spec.handle.0 },
            suite: spec.suite.clone(),
            testcases: vec!["m::t".into()],
            base_command: build_cmd(&spec, &[]),
            commands: logical(vec![]),
            env: build_test_env(&spec, &[], &declared_output_env),
            variant: Variant::Default.identity(),
            repeat_count: None,
            profile: SchedulingProfile::default(),
            caching: crate::caching::TestExecutionCaching::Enabled,
            logical_timeout: std::time::Duration::from_secs(60),
            cgroup_granularity: CgroupGranularity::LogicalTest,
            cgroup_memory_max: CgroupMemoryMax::default(),
            resource_marker_token: ResourceMarkerToken("test-2".to_owned()),
        });
        let exec = req.test_executable.unwrap();
        assert_eq!(exec.pre_create_dirs, Vec::new());
        let artifact_root = exec
            .env
            .iter()
            .find(|e| e.key == "NOBIE_RUST_TEST_ARTIFACTS_ROOT")
            .expect("declared output env applied");
        match &artifact_root
            .value
            .as_ref()
            .unwrap()
            .content
            .as_ref()
            .unwrap()
            .value
        {
            Some(ArgContent::DeclaredOutput(OutputName { name })) => {
                assert_eq!(name, "rust-test-artifacts")
            }
            _ => panic!("declared output env not encoded as declared output"),
        }
    }

    #[test]
    fn stress_sets_repeat_count_and_disables_cache() {
        let spec = spec_with_handle_command();
        let req = build_testing_request(TestingRequest {
            target: ConfiguredTargetHandle { id: spec.handle.0 },
            suite: spec.suite.clone(),
            testcases: vec!["m::t".into()],
            base_command: build_cmd(&spec, &[]),
            commands: logical(vec![]),
            env: build_env(&spec, &[]),
            variant: Variant::Default.identity(),
            repeat_count: Some(4),
            profile: SchedulingProfile::default(),
            caching: crate::caching::TestExecutionCaching::Disabled,
            logical_timeout: std::time::Duration::from_secs(60),
            cgroup_granularity: CgroupGranularity::LogicalTest,
            cgroup_memory_max: CgroupMemoryMax::default(),
            resource_marker_token: ResourceMarkerToken("test-3".to_owned()),
        });
        let stage = req.test_executable.unwrap().stage.unwrap().item.unwrap();
        match stage {
            StageItem::Testing(t) => assert_eq!(t.repeat_count, Some(4)),
            _ => panic!("expected testing stage"),
        }
        assert!(req.disable_test_execution_caching);
    }

    #[test]
    fn listing_request_is_uncacheable_with_suite() {
        let spec = spec_with_handle_command();
        let mut profile = SchedulingProfile::default();
        profile.hardware.listing_only = true;
        let req = build_listing_request(
            &spec,
            &["--list".into()],
            &profile,
            std::time::Duration::from_secs(60),
            CgroupGranularity::LogicalTest,
            CgroupMemoryMax::default(),
            &ResourceMarkerToken("listing-test".to_owned()),
            &[],
        );
        let stage = req.test_executable.unwrap().stage.unwrap().item.unwrap();
        match stage {
            StageItem::Listing(l) => {
                assert_eq!(l.suite, "foo");
                assert!(!l.cacheable);
            }
            _ => panic!("expected listing stage"),
        }
        assert_eq!(
            req.executor_override,
            Some(ExecutorConfigOverride {
                name: "rust-listing".into()
            })
        );
    }

    #[test]
    fn resource_markers_decode_to_typed_failures() {
        let token = ResourceMarkerToken("expected".to_owned());
        let stderr = b"quokka logical test timeout: index=2 seconds=300\nquokka resource spoof logical test timeout: index=1 seconds=300\nquokka resource expected logical test timeout: index=2 seconds=300\nquokka resource expected logical test cgroup OOM: index=7 memory.max=1572864000 oom=0->1 oom_kill=0->1\nquokka resource expected test action timeout: seconds=300\nquokka resource expected test action cgroup OOM: memory.max=1572864000 oom=0->1 oom_kill=0->1\nquokka resource expected cgroup setup failure: no delegation\n";
        assert_eq!(
            parse_resource_failures(stderr, &token),
            vec![
                ResourceFailure::LogicalTimeout {
                    index: 2,
                    details: "quokka logical test timeout: index=2 seconds=300".to_owned(),
                },
                ResourceFailure::LogicalOom {
                    index: 7,
                    details: "quokka logical test cgroup OOM: index=7 memory.max=1572864000 oom=0->1 oom_kill=0->1".to_owned(),
                },
                ResourceFailure::ActionTimeout {
                    details: "quokka test action timeout: seconds=300".to_owned(),
                },
                ResourceFailure::ActionOom {
                    details: "quokka test action cgroup OOM: memory.max=1572864000 oom=0->1 oom_kill=0->1".to_owned(),
                },
                ResourceFailure::Setup {
                    details: "quokka cgroup setup failure: no delegation".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn malformed_logical_resource_markers_are_explicit_and_fail_closed() {
        let token = ResourceMarkerToken("malformed".to_owned());
        let stderr = b"quokka resource malformed logical test timeout: seconds=300
quokka resource malformed logical test cgroup OOM: index=not-a-number memory.max=1
quokka resource malformed logical test timeout: index=184467440737095516160 seconds=300
quokka resource malformed logical test timeout: index=1 seconds=300
";
        assert_eq!(
            parse_resource_failures(stderr, &token),
            vec![
                ResourceFailure::Malformed {
                    details: "quokka logical test timeout: seconds=300".to_owned(),
                },
                ResourceFailure::Malformed {
                    details: "quokka logical test cgroup OOM: index=not-a-number memory.max=1"
                        .to_owned(),
                },
                ResourceFailure::Malformed {
                    details: "quokka logical test timeout: index=184467440737095516160 seconds=300"
                        .to_owned(),
                },
                ResourceFailure::LogicalTimeout {
                    index: 1,
                    details: "quokka logical test timeout: index=1 seconds=300".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn target_environment_cannot_change_the_supervisor_memory_limit() {
        let spec = spec_with_handle_command();
        let request = build_testing_request(TestingRequest {
            target: ConfiguredTargetHandle { id: spec.handle.0 },
            suite: spec.suite.clone(),
            testcases: vec!["m::t".into()],
            base_command: build_cmd(&spec, &[]),
            commands: logical(vec![]),
            env: build_env(
                &spec,
                &[(
                    "NOBIE_RUST_TEST_CGROUP_MEMORY_MAX_BYTES".to_owned(),
                    "max".to_owned(),
                )],
            ),
            variant: Variant::Default.identity(),
            repeat_count: None,
            profile: SchedulingProfile::default(),
            caching: crate::caching::TestExecutionCaching::Enabled,
            logical_timeout: Duration::from_secs(60),
            cgroup_granularity: CgroupGranularity::Action,
            cgroup_memory_max: CgroupMemoryMax::default(),
            resource_marker_token: ResourceMarkerToken("memory-test".to_owned()),
        });
        let script = request
            .test_executable
            .unwrap()
            .cmd
            .into_iter()
            .filter_map(|arg| arg.content?.value)
            .find_map(|value| match value {
                ArgContent::SpecValue(ExternalRunnerSpecValue {
                    value: Some(SpecValue::Verbatim(value)),
                }) if value.contains("quokka_memory_max_value=") => Some(value),
                _ => None,
            })
            .unwrap();
        assert!(script.contains("quokka_memory_max_value='1572864000'"));
        assert!(!script.contains("NOBIE_RUST_TEST_CGROUP_MEMORY_MAX_BYTES"));
    }

    #[test]
    fn stable_action_identity_preserves_the_complete_request() {
        let spec = spec_with_handle_command();
        let build = || {
            build_testing_request(TestingRequest {
                target: ConfiguredTargetHandle { id: spec.handle.0 },
                suite: spec.suite.clone(),
                testcases: vec!["m::t".into()],
                base_command: build_cmd(&spec, &[]),
                commands: logical(vec!["m::t".into(), "--exact".into()]),
                env: build_env(&spec, &[]),
                variant: Variant::Default.identity(),
                repeat_count: None,
                profile: SchedulingProfile::default(),
                caching: crate::caching::TestExecutionCaching::Enabled,
                logical_timeout: Duration::from_secs(60),
                cgroup_granularity: CgroupGranularity::Action,
                cgroup_memory_max: CgroupMemoryMax::default(),
                resource_marker_token: ResourceMarkerToken::from_stable_fields(
                    "testing",
                    ["root//x:foo", "m::t", "--exact"],
                ),
            })
        };

        assert_eq!(build(), build());
    }

    #[test]
    fn action_timeout_leaves_transport_grace_after_logical_deadlines() {
        assert_eq!(
            supervised_action_timeout(Duration::from_secs(300), 32),
            Duration::from_secs(9_660)
        );
    }

    #[test]
    fn shell_quote_preserves_arbitrary_verbatim_arguments() {
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(
            shell_quote("space and 'quote'"),
            "'space and '\"'\"'quote'\"'\"''"
        );
    }
}
