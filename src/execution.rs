//! Building `Execute2` requests for listing and test actions.
//!
//! The target's base command and env are *handles* (and verbatim values) that
//! buck2 expands at execute time — re-materializing artifacts and hidden inputs.
//! We therefore echo every spec handle back UNMODIFIED inside
//! `ArgValueContent::SpecValue`, and append only our own verbatim flags (with
//! `format: None`). Touching a handle would strip the test of its artifact
//! dependencies and it would fail to find its own binary.

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
/// The listing is always issued `cacheable: false`. A listing's product is the
/// set of test names on the action's *stdout*, and buck2 returns empty
/// stdout/stderr on a cache hit (only declared outputs survive a replay), so a
/// cacheable listing would replay as zero tests — a silent green "ran nothing".
/// Re-listing per invocation is cheap (it enumerates names, runs no test bodies).
pub fn build_listing_request(
    spec: &TargetSpec,
    listing_args: &[String],
    profile: &SchedulingProfile,
    timeout: Duration,
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
        cmd: build_cmd(spec, listing_args),
        pre_create_dirs: Vec::new(),
        env: build_env(spec, extra_env),
    };
    ExecuteRequest2 {
        timeout: Some(duration_to_proto(timeout)),
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
    pub cmd: Vec<ArgValue>,
    pub env: Vec<EnvironmentVariable>,
    pub variant: Option<String>,
    pub repeat_count: Option<u64>,
    pub profile: SchedulingProfile,
    pub caching: crate::caching::TestExecutionCaching,
    pub timeout: Duration,
}

/// Build a `Testing` stage `Execute2` request.
pub fn build_testing_request(req: TestingRequest) -> ExecuteRequest2 {
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
        cmd: req.cmd,
        pre_create_dirs: Vec::new(),
        env: req.env,
    };
    ExecuteRequest2 {
        timeout: Some(duration_to_proto(req.timeout)),
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

    #[test]
    fn echoes_handles_unmodified_and_appends_verbatim() {
        let spec = spec_with_handle_command();
        let req = build_testing_request(TestingRequest {
            target: ConfiguredTargetHandle { id: spec.handle.0 },
            suite: spec.suite.clone(),
            testcases: vec!["m::t".into()],
            cmd: build_cmd(&spec, &vec!["m::t".into(), "--exact".into()]),
            env: build_env(&spec, &[("RUST_LOG".to_owned(), "debug".to_owned())]),
            variant: Variant::Default.identity(),
            repeat_count: None,
            profile: SchedulingProfile::default(),
            caching: crate::caching::TestExecutionCaching::Enabled,
            timeout: std::time::Duration::from_secs(60),
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
        // First arg echoes the ArgHandle(3) unmodified.
        match &cmd[0].content.as_ref().unwrap().value {
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
            cmd: build_cmd(&spec, &[]),
            env: build_test_env(&spec, &[], &declared_output_env),
            variant: Variant::Default.identity(),
            repeat_count: None,
            profile: SchedulingProfile::default(),
            caching: crate::caching::TestExecutionCaching::Enabled,
            timeout: std::time::Duration::from_secs(60),
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
            cmd: build_cmd(&spec, &[]),
            env: build_env(&spec, &[]),
            variant: Variant::Default.identity(),
            repeat_count: Some(4),
            profile: SchedulingProfile::default(),
            caching: crate::caching::TestExecutionCaching::Disabled,
            timeout: std::time::Duration::from_secs(60),
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
            &[],
        );
        let stage = req.test_executable.unwrap().stage.unwrap().item.unwrap();
        match stage {
            StageItem::Listing(l) => {
                assert_eq!(l.suite, "foo");
                // Always uncacheable: buck2 drops stdout on a cache hit, so a
                // cached listing would replay as zero tests.
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
}
