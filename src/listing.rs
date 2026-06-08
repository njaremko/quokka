//! Framework-agnostic test discovery: test case data model and ignored-test policy.
//!
//! The ignored policy determines which discovered tests are retained for execution.
//! It is critical that this policy aligns perfectly with the execution filter to
//! prevent listed-but-not-run (or vice versa) scenarios — the classic "0 tests run, exit 0"
//! false pass.

/// How ignored tests are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoredPolicy {
    /// Default: do not run ignored tests.
    ExcludeIgnored,
    /// Run both normal and ignored tests.
    IncludeIgnored,
    /// Run only ignored tests.
    IgnoredOnly,
}

impl IgnoredPolicy {
    /// Keep only the discovered tests this policy will actually run.
    /// This requires the framework's parser to accurately report the `ignored` status
    /// of each test during discovery. If the framework cannot determine ignored status
    /// during discovery, filtering must be deferred to run time.
    pub fn selects(self, ignored: bool) -> bool {
        match self {
            IgnoredPolicy::ExcludeIgnored => !ignored,
            IgnoredPolicy::IncludeIgnored => true,
            IgnoredPolicy::IgnoredOnly => ignored,
        }
    }
}



/// A single discovered test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestCase {
    pub name: String,
    pub ignored: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ListingParseError {
    #[error("listing output was not valid UTF-8")]
    NotUtf8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_policy_selects() {
        assert!(!IgnoredPolicy::ExcludeIgnored.selects(true));
        assert!(IgnoredPolicy::ExcludeIgnored.selects(false));
        assert!(IgnoredPolicy::IncludeIgnored.selects(true));
        assert!(IgnoredPolicy::IncludeIgnored.selects(false));
        assert!(IgnoredPolicy::IgnoredOnly.selects(true));
        assert!(!IgnoredPolicy::IgnoredOnly.selects(false));
    }
}
