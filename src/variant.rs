//! Test execution *variant* and *repetition*, the two orthogonal multiplicity
//! axes buck2's `Testing` stage exposes (`variant`, `repeat_count`).
//!
//! These are deliberately NOT a build-configuration selector and NOT a
//! translator selector:
//! - `Variant` is only the identity string buck2 records on the `Testing`
//!   stage. Sanitizer/coverage *builds* are selected by buck2 configuration /
//!   executor overrides, not by this string; the string just disambiguates the
//!   action and its cache key.
//! - the test framework (libtest vs nextest vs doctest) is a separate concern
//!   owned by [`crate::translator`], so there is no `Nextest` variant here.

use std::num::NonZeroU32;

/// The `Testing.variant` identity. `Default` emits no variant string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Variant {
    Default,
    Asan,
    Tsan,
    Miri,
    Loom,
    Coverage,
    /// An arbitrary caller-supplied variant identity.
    Named(String),
}

impl Variant {
    pub fn is_default(&self) -> bool {
        matches!(self, Variant::Default)
    }

    /// The string to put in `Testing.variant`, or `None` for `Default`.
    pub fn identity(&self) -> Option<String> {
        match self {
            Variant::Default => None,
            Variant::Asan => Some("asan".to_owned()),
            Variant::Tsan => Some("tsan".to_owned()),
            Variant::Miri => Some("miri".to_owned()),
            Variant::Loom => Some("loom".to_owned()),
            Variant::Coverage => Some("coverage".to_owned()),
            Variant::Named(name) => Some(name.clone()),
        }
    }

    /// Parse a `--variant` CLI value.
    pub fn parse(value: &str) -> Variant {
        match value {
            "default" => Variant::Default,
            "asan" => Variant::Asan,
            "tsan" => Variant::Tsan,
            "miri" => Variant::Miri,
            "loom" => Variant::Loom,
            "coverage" => Variant::Coverage,
            other => Variant::Named(other.to_owned()),
        }
    }
}

/// Execution multiplicity. `Once` is an ordinary single run; `Stress{n}` runs
/// the test `n` times, each as a distinct action with its own `repeat_count`
/// (so each gets a distinct cache key and result identity). `NonZeroU32`
/// makes the meaningless "repeat 0 times" unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RepeatKind {
    Once,
    Stress(NonZeroU32),
}

impl RepeatKind {
    pub fn is_stress(&self) -> bool {
        matches!(self, RepeatKind::Stress(_))
    }

    /// Number of action copies this repetition implies (>= 1).
    pub fn count(&self) -> u32 {
        match self {
            RepeatKind::Once => 1,
            RepeatKind::Stress(n) => n.get(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_identity_and_default() {
        assert!(Variant::Default.is_default());
        assert_eq!(Variant::Default.identity(), None);
        assert_eq!(Variant::Asan.identity(), Some("asan".to_owned()));
        assert_eq!(Variant::parse("miri"), Variant::Miri);
        assert_eq!(
            Variant::parse("custom"),
            Variant::Named("custom".to_owned())
        );
    }

    #[test]
    fn repeat_counts() {
        assert_eq!(RepeatKind::Once.count(), 1);
        assert_eq!(RepeatKind::Stress(NonZeroU32::new(99).unwrap()).count(), 99);
        assert!(RepeatKind::Stress(NonZeroU32::new(2).unwrap()).is_stress());
    }
}
