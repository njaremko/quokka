//! Semantic environment constraints for test execution.
//!
//! Models the hardware capabilities, host isolation requirements, and external
//! services a test needs to run, completely decoupled from infrastructure
//! queue names or transport details.

use std::collections::BTreeSet;
use std::fmt;

/// The isolation level required by the test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostExclusivity {
    Shared,
    Exclusive,
}

impl Default for HostExclusivity {
    fn default() -> Self {
        HostExclusivity::Shared
    }
}

/// The hardware constraints for the environment.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HardwareNeeds {
    pub requires_gpu: bool,
    pub requires_large_mem: bool,
    pub network_isolated: bool,
    pub listing_only: bool,
    pub local_debug: bool,
}

impl HardwareNeeds {
    /// Combines two hardware needs. If they conflict (e.g. they both demand something
    /// that cannot be combined on current infrastructure), this returns an error.
    pub fn combine(self, other: HardwareNeeds) -> Result<HardwareNeeds, ConstraintConflictError> {
        let merged = HardwareNeeds {
            requires_gpu: self.requires_gpu || other.requires_gpu,
            requires_large_mem: self.requires_large_mem || other.requires_large_mem,
            network_isolated: self.network_isolated || other.network_isolated,
            listing_only: self.listing_only || other.listing_only,
            local_debug: self.local_debug || other.local_debug,
        };

        // For now, GPU and Large Mem are mutually exclusive queues in buck2 for this project.
        if merged.requires_gpu && merged.requires_large_mem {
            return Err(ConstraintConflictError(
                "Cannot satisfy both gpu and large-mem hardware needs.",
            ));
        }

        Ok(merged)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintConflictError(pub &'static str);

impl fmt::Display for ConstraintConflictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Constraint conflict: {}", self.0)
    }
}

impl std::error::Error for ConstraintConflictError {}

/// The total execution profile constraint.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchedulingProfile {
    pub hardware: HardwareNeeds,
    pub exclusivity: HostExclusivity,
    pub local_resources: BTreeSet<String>,
}

impl SchedulingProfile {
    pub fn combine(
        mut self,
        other: SchedulingProfile,
    ) -> Result<SchedulingProfile, ConstraintConflictError> {
        self.hardware = self.hardware.combine(other.hardware)?;
        if other.exclusivity == HostExclusivity::Exclusive {
            self.exclusivity = HostExclusivity::Exclusive;
        }
        self.local_resources.extend(other.local_resources);
        Ok(self)
    }
}

// Temporary parser logic for labels (until label parsing is centralized).
pub fn profile_from_labels(
    labels: &[String],
) -> Result<SchedulingProfile, ConstraintConflictError> {
    let mut profile = SchedulingProfile::default();

    for l in labels {
        let suffix = l.split_once(':').unwrap_or(("", l)).1;
        let mut single = SchedulingProfile::default();

        match suffix {
            "gpu" => single.hardware.requires_gpu = true,
            "large-mem" => single.hardware.requires_large_mem = true,
            "network-private" => single.hardware.network_isolated = true,
            "exclusive" | "serial_global_state" => single.exclusivity = HostExclusivity::Exclusive,
            _ => {
                if let Some(r) = suffix.strip_prefix("local-resource=") {
                    single.local_resources.insert(r.to_owned());
                }
            }
        }

        profile = profile.combine(single)?;
    }

    Ok(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpu_selects_gpu() {
        let req = profile_from_labels(&["rust:gpu".to_owned()]).unwrap();
        assert!(req.hardware.requires_gpu);
        assert!(!req.hardware.requires_large_mem);
        assert!(req.local_resources.is_empty());
    }

    #[test]
    fn conflict_gpu_and_large_mem() {
        let req = profile_from_labels(&["python:large-mem".to_owned(), "rust:gpu".to_owned()]);
        assert!(req.is_err());
    }

    #[test]
    fn local_resources_accumulate() {
        let req = profile_from_labels(&[
            "rust:local-resource=postgres".to_owned(),
            "python:local-resource=redis".to_owned(),
        ])
        .unwrap();
        let mut expected = BTreeSet::new();
        expected.insert("postgres".to_owned());
        expected.insert("redis".to_owned());
        assert_eq!(req.local_resources, expected);
    }
}
