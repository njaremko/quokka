//! The two distinct caching concepts, kept apart so one bit is not written by
//! several places with no precedence.
//!
//! - [`CacheClass`] is the target's base test-execution cacheability, derived
//!   from labels (hermetic vs non-hermetic).
//! - [`TestExecutionCaching`] is the per-action decision actually sent in
//!   `ExecuteRequest2.disable_test_execution_caching`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheClass {
    Cacheable,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestExecutionCaching {
    Enabled,
    Disabled,
}

impl TestExecutionCaching {
    pub fn disable_flag(self) -> bool {
        matches!(self, TestExecutionCaching::Disabled)
    }

    /// Derives the final cache enablement per execution action.
    pub fn resolve(
        base: CacheClass,
        is_default_variant: bool,
        is_stress: bool,
        attempt_index: u32,
    ) -> Self {
        if base == CacheClass::Disabled
            || !is_default_variant
            || is_stress
            || attempt_index > 0
        {
            TestExecutionCaching::Disabled
        } else {
            TestExecutionCaching::Enabled
        }
    }
}

pub fn cache_class(labels: &[String]) -> CacheClass {
    let disabled = labels.iter().any(|l| {
        let suffix = l.split_once(':').unwrap_or(("", l)).1;
        matches!(
            suffix,
            "cache_disabled"
                | "flaky"
                | "uses_network"
                | "uses_wall_clock"
                | "uses_randomness_without_seed"
                | "requires_external_service"
                | "serial_global_state"
                | "stress"
                | "network-private"
        )
    });
    if disabled {
        CacheClass::Disabled
    } else {
        CacheClass::Cacheable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_class_disabled_for_nonhermetic() {
        assert_eq!(cache_class(&["rust:flaky".to_owned()]), CacheClass::Disabled);
        assert_eq!(
            cache_class(&["python:uses_network".to_owned()]),
            CacheClass::Disabled
        );
        assert_eq!(
            cache_class(&["pkg".to_owned()]),
            CacheClass::Cacheable
        );
    }
}
