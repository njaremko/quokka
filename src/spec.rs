//! The cold, immutable per-target data parsed once from an `ExternalRunnerSpec`.
//!
//! Held behind an `Arc` and shared by every action of the target (across
//! variants, stress repetitions and retries). The hot scheduler tables never
//! touch these strings; they are read only when building an `Execute2` request
//! or formatting a final `TestResult`.

use std::sync::Arc;

use crate::ids::TargetHandle;
use crate::proto::test::{ConfiguredTargetHandle, ExternalRunnerSpec, ExternalRunnerSpecValue};
use crate::proto::test::external_runner_spec_value::Value;

/// An argument in a command or environment, abstracted away from buck2 protobufs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandArg {
    Verbatim(String),
    ArgHandle(i64),
    EnvHandle(String),
}

impl CommandArg {
    pub fn from_proto(v: ExternalRunnerSpecValue) -> Self {
        match v.value.unwrap() {
            Value::Verbatim(s) => CommandArg::Verbatim(s),
            Value::ArgHandle(i) => CommandArg::ArgHandle(i),
            Value::EnvHandle(s) => CommandArg::EnvHandle(s),
        }
    }
}

/// Cold per-target data.
#[derive(Debug)]
pub struct TargetSpec {
    /// Wire handle echoed back on every report/execute for this target.
    pub handle: TargetHandle,
    /// The `Testing`/`Listing` suite identity — the target's `:name`, identical
    /// across the listing stage and every test stage.
    pub suite: String,
    /// Human-readable `cell//package:target` for result names and logs.
    pub display: String,
    /// The provider's `type` field (selects the translator).
    pub test_type: String,
    /// Base command: verbatim args and opaque handles, echoed back unmodified.
    pub command: Box<[CommandArg]>,
    /// Environment, sorted by key for deterministic requests.
    pub env: Box<[(String, CommandArg)]>,
    pub labels: Vec<String>,
    pub contacts: Vec<String>,
    pub oncall: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SpecError {
    #[error("ExternalRunnerSpec missing target")]
    MissingTarget,
    #[error("ExternalRunnerSpec target missing handle")]
    MissingHandle,
}

impl TargetSpec {
    /// Parse an `ExternalRunnerSpec` into cold target data.
    pub fn from_proto(spec: ExternalRunnerSpec) -> Result<Arc<Self>, SpecError> {
        let target = spec.target.ok_or(SpecError::MissingTarget)?;
        let handle = target.handle.ok_or(SpecError::MissingHandle)?;
        let display = format!("{}//{}:{}", target.cell, target.package, target.target);

        let labels = spec.labels;

        // Sort env by key so identical specs produce byte-identical requests.
        let mut env: Vec<(String, CommandArg)> = spec
            .env
            .into_iter()
            .map(|(k, v)| (k, CommandArg::from_proto(v)))
            .collect();
        env.sort_by(|a, b| a.0.cmp(&b.0));

        let command = spec
            .command
            .into_iter()
            .map(CommandArg::from_proto)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Arc::new(TargetSpec {
            handle: TargetHandle(handle.id),
            suite: display.clone(),
            display,
            test_type: spec.test_type,
            command,
            env: env.into_boxed_slice(),
            labels,
            contacts: spec.contacts,
            oncall: spec.oncall,
        }))
    }

    /// The wire handle for reports/executes.
    pub fn handle_proto(&self) -> ConfiguredTargetHandle {
        ConfiguredTargetHandle { id: self.handle.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::test::ConfiguredTarget;
    use std::collections::HashMap;

    fn verbatim(s: &str) -> ExternalRunnerSpecValue {
        ExternalRunnerSpecValue {
            value: Some(Value::Verbatim(s.to_owned())),
        }
    }

    #[test]
    fn parses_target_and_sorts_env() {
        let mut env = HashMap::new();
        env.insert("ZED".to_owned(), verbatim("1"));
        env.insert("ABLE".to_owned(), verbatim("2"));
        let spec = ExternalRunnerSpec {
            target: Some(ConfiguredTarget {
                handle: Some(ConfiguredTargetHandle { id: 42 }),
                cell: "root".into(),
                package: "rust/foo".into(),
                target: "foo".into(),
                configuration: "cfg".into(),
                package_project_relative_path: "rust/foo".into(),
                test_config_unification_rollout: false,
                package_oncall: None,
            }),
            test_type: "rust".into(),
            command: vec![
                verbatim("./test-bin"),
                ExternalRunnerSpecValue {
                    value: Some(Value::ArgHandle(7)),
                },
            ],
            env,
            labels: vec!["rust:flaky".into(), "foo".into()],
            contacts: vec!["team@x".into()],
            oncall: Some("oncall-x".into()),
            working_dir_cell: "root".into(),
        };
        // `working_dir_cell` is intentionally not retained on TargetSpec (the
        // runner sets no working dir; buck2 handles cwd via the provider).
        let parsed = TargetSpec::from_proto(spec).unwrap();
        assert_eq!(parsed.handle, TargetHandle(42));
        assert_eq!(parsed.suite, "root//rust/foo:foo");
        assert_eq!(parsed.display, "root//rust/foo:foo");
        assert_eq!(parsed.test_type, "rust");
        assert_eq!(parsed.command.len(), 2);
        // env sorted: ABLE before ZED.
        assert_eq!(parsed.env[0].0, "ABLE");
        assert_eq!(parsed.env[1].0, "ZED");
        assert_eq!(parsed.labels.len(), 2);
        assert_eq!(parsed.labels[0], "rust:flaky");
    }

    #[test]
    fn missing_target_is_error() {
        let spec = ExternalRunnerSpec {
            target: None,
            ..Default::default()
        };
        assert!(matches!(
            TargetSpec::from_proto(spec),
            Err(SpecError::MissingTarget)
        ));
    }
}
