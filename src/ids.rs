//! Typed identifiers and the test-name interner for the scheduler's hot tables.
//!
//! The hot selection loop must key on cheap integer columns, never on the cold
//! protocol strings (cell/package/target, test names). These newtypes make
//! "a target index" and "an action index" distinct types so retries and
//! batching — which reshuffle rows — cannot silently swap one for the other.

use rustc_hash::FxHashMap;

/// Dense index into the cold per-target store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TargetId(pub u32);

/// Interned test-name index. Test names repeat across variants/stress/retries,
/// so they are interned once and the hot rows carry this id instead of `String`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TestNameId(pub u32);

/// Dense index into the flattened action table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActionId(pub u32);

/// The wire `ConfiguredTargetHandle.id`. Kept in the cold target row and echoed
/// back on every report/execute; never used as a hot map key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TargetHandle(pub i64);

/// Interns test names so the action table carries [`TestNameId`] (a `u32`)
/// rather than cloning the name string per action.
#[derive(Debug, Default)]
pub struct TestNameInterner {
    names: Vec<String>,
    index: FxHashMap<String, TestNameId>,
}

impl TestNameInterner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Intern `name`, returning its stable id (creating one on first sight).
    pub fn intern(&mut self, name: &str) -> TestNameId {
        if let Some(id) = self.index.get(name) {
            return *id;
        }
        let id = TestNameId(self.names.len() as u32);
        self.names.push(name.to_owned());
        self.index.insert(name.to_owned(), id);
        id
    }

    /// Resolve an id back to its name. Panics only on an id this interner never
    /// issued, which is a programming error, not a runtime condition.
    pub fn resolve(&self, id: TestNameId) -> &str {
        &self.names[id.0 as usize]
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interner_roundtrips_and_dedups() {
        let mut interner = TestNameInterner::new();
        let a = interner.intern("mod::test_a");
        let b = interner.intern("mod::test_b");
        let a2 = interner.intern("mod::test_a");
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_eq!(interner.resolve(a), "mod::test_a");
        assert_eq!(interner.resolve(b), "mod::test_b");
        assert_eq!(interner.len(), 2);
    }
}
