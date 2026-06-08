//! Adaptive batching: collapsing many tiny per-test actions into fewer
//! `Execute2` calls to amortize remote-action overhead.
//!
//! A single test per action pays the full per-action synchronization cost
//! (`Execute2` round trip, action-digest + cache query, executor dispatch,
//! process/harness startup, and — on RE — input upload and result download)
//! once per test. [`BatchMode::FixedChunk`] amortizes that across a configurable
//! number of tests per action: the tests in a chunk run sequentially in one
//! process (`--test-threads=1`), and parallelism comes from running many chunks
//! concurrently. Fewer, larger chunks cut overhead but lower cross-chunk
//! parallelism; smaller chunks do the reverse — the chunk size is the tuning
//! knob between those.
//!
//! For monolithic Rust test binaries chunking costs nothing in cache
//! granularity: every per-test action shares the one test-binary artifact, so
//! any source change rebuilds the binary and invalidates every action's digest
//! regardless of chunking — per-test and per-chunk actions hit and miss together.
//!
//! Batching is a transform applied to a target's test list AFTER listing and
//! duration lookup and BEFORE the actions enter the scheduler's ready set, so it
//! never mutates a table the scheduler is draining.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;



/// Neutral per-test weight (ms) for tests with no recorded history, so a cold
/// cache balances chunks evenly instead of piling unseen tests together.
const UNSEEN_CHUNK_WEIGHT_MS: u64 = 50;

/// How tests within a target are grouped into actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchMode {
    /// One action per test (finest cache granularity, highest per-action
    /// overhead). Equivalent to `FixedChunk { size: 1 }`.
    PerTest,
    /// Group tests whose historical p50 is below `p50_lt_ms` into shared
    /// actions; longer tests stay singleton.
    DurationBucketed { p50_lt_ms: u64 },
    /// Group tests into balanced chunks of at most `size` tests per action,
    /// amortizing the per-action synchronization cost. Default.
    FixedChunk { size: NonZeroUsize },
    /// One action for the whole target (emergency fallback / fastest discovery).
    Target,
}

/// What to do when a non-singleton batch action fails to attribute a result to
/// every member (e.g. the process crashed mid-batch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchFailurePolicy {
    /// Report the missing members as FATAL.
    FailAll,
    /// Re-run the batch's members individually to isolate the failure.
    RerunPerTestToIsolate,
}

/// Largest number of tests collapsed into one batched action.
const MAX_BATCH: usize = 64;

/// The subset of a target's tests to execute in a single action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestSelection<T> {
    /// Execute the target natively without enumerating explicit test filters.
    All,
    /// Execute exactly the specified subset of tests.
    Explicit(Vec<T>),
}

/// A policy for partitioning a set of tests into batched actions.
pub trait Batcher<T> {
    fn partition(&self, tests: &[T]) -> Vec<TestSelection<T>>;
}

/// A generic input item for batching that carries a weight and a typical duration.
pub trait Batchable: Clone {
    fn weight_ms(&self) -> u64;
    fn p50_ms(&self) -> u64;
}

impl<T: Batchable> Batcher<T> for BatchMode {
    fn partition(&self, tests: &[T]) -> Vec<TestSelection<T>> {
        match self {
            BatchMode::PerTest => tests.iter().map(|t| TestSelection::Explicit(vec![t.clone()])).collect(),
            BatchMode::Target => vec![TestSelection::All],
            BatchMode::DurationBucketed { p50_lt_ms } => {
                bucket_generic(tests, MAX_BATCH, |t| t.p50_ms() < *p50_lt_ms)
                    .into_iter()
                    .map(|batch| TestSelection::Explicit(batch))
                    .collect()
            }
            BatchMode::FixedChunk { size } => {
                chunk_fixed_generic(tests, *size, |t| t.weight_ms())
                    .into_iter()
                    .map(|batch| TestSelection::Explicit(batch))
                    .collect()
            }
        }
    }
}

/// Group items into balanced chunks of at most `size` members, minimizing the
/// heaviest chunk's total weight (Longest-Processing-Time bin packing with a
/// cardinality cap).
fn chunk_fixed_generic<T: Clone, W>(
    items: &[T],
    size: NonZeroUsize,
    weight_fn: W,
) -> Vec<Vec<T>>
where
    W: Fn(&T) -> u64,
{
    if items.is_empty() {
        return Vec::new();
    }
    let size = size.get();
    let num_chunks = items.len().div_ceil(size);

    let mut order: Vec<&T> = items.iter().collect();
    order.sort_by_key(|t| Reverse(weight_fn(t)));

    let mut chunks: Vec<Vec<T>> = vec![Vec::new(); num_chunks];
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> =
        (0..num_chunks).map(|i| Reverse((0u64, i))).collect();

    for input in order {
        let Reverse((weight, idx)) = heap.pop().expect("a non-full chunk is always available");
        chunks[idx].push((*input).clone());
        if chunks[idx].len() < size {
            let new_weight = weight + weight_fn(input);
            heap.push(Reverse((new_weight, idx)));
        }
    }
    chunks
}

/// Group items into batches, keeping "large" items isolated and grouping "small"
/// items up to `max_batch` size.
fn bucket_generic<T: Clone, P>(
    items: &[T],
    max_batch: usize,
    is_small: P,
) -> Vec<Vec<T>>
where
    P: Fn(&T) -> bool,
{
    let mut batches: Vec<Vec<T>> = Vec::new();
    let mut current: Vec<T> = Vec::new();
    for input in items {
        if is_small(input) {
            current.push(input.clone());
            if current.len() >= max_batch {
                batches.push(std::mem::take(&mut current));
            }
        } else {
            if !current.is_empty() {
                batches.push(std::mem::take(&mut current));
            }
            batches.push(vec![input.clone()]);
        }
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestItem {
        id: u32,
        p50: u64,
        weight: u64,
    }

    impl Batchable for TestItem {
        fn weight_ms(&self) -> u64 { self.weight }
        fn p50_ms(&self) -> u64 { self.p50 }
    }

    fn input(id: u32, p50: u64) -> TestItem {
        TestItem { id, p50, weight: p50 }
    }

    #[test]
    fn per_test_is_one_each() {
        let tests = [input(0, 5), input(1, 5)];
        let batches = BatchMode::PerTest.partition(&tests);
        assert_eq!(batches, vec![
            TestSelection::Explicit(vec![input(0, 5)]),
            TestSelection::Explicit(vec![input(1, 5)])
        ]);
    }

    #[test]
    fn target_is_one_group() {
        let tests = [input(0, 5), input(1, 5), input(2, 5)];
        let batches = BatchMode::Target.partition(&tests);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], TestSelection::All);
    }



    #[test]
    fn empty_target_yields_no_actions() {
        let empty: &[TestItem] = &[];
        assert_eq!(BatchMode::Target.partition(empty), vec![TestSelection::All]);
        assert!(BatchMode::PerTest.partition(empty).is_empty());
        assert!(BatchMode::FixedChunk { size: NonZeroUsize::new(8).unwrap() }.partition(empty).is_empty());
    }

    fn ids(chunks: &[TestSelection<TestItem>]) -> std::collections::BTreeSet<u32> {
        chunks.iter().filter_map(|c| {
            if let TestSelection::Explicit(v) = c { Some(v) } else { None }
        }).flatten().map(|t| t.id).collect()
    }

    #[test]
    fn fixed_chunk_size_one_is_one_per_action() {
        let tests = [input(0, 5), input(1, 5), input(2, 5)];
        let batches = BatchMode::FixedChunk { size: NonZeroUsize::new(1).unwrap() }.partition(&tests);
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|c| {
            if let TestSelection::Explicit(v) = c { v.len() == 1 } else { false }
        }));
    }

    #[test]
    fn fixed_chunk_caps_size_and_covers_all_tests_exactly_once() {
        let tests: Vec<TestItem> = (0..10).map(|i| input(i, i as u64)).collect();
        let batches = BatchMode::FixedChunk { size: NonZeroUsize::new(4).unwrap() }.partition(&tests);
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|c| {
            if let TestSelection::Explicit(v) = c { v.len() <= 4 } else { false }
        }));
        assert_eq!(batches.iter().map(|c| {
            if let TestSelection::Explicit(v) = c { v.len() } else { 0 }
        }).sum::<usize>(), 10);
        assert_eq!(ids(&batches), (0..10).collect());
    }

    #[test]
    fn fixed_chunk_handles_fewer_tests_than_chunk_size() {
        let tests = [input(0, 5), input(1, 5)];
        let batches = BatchMode::FixedChunk { size: NonZeroUsize::new(4).unwrap() }.partition(&tests);
        assert_eq!(batches.len(), 1);
        let mut ids = match &batches[0] {
            TestSelection::Explicit(v) => v.iter().map(|i| i.id).collect::<Vec<_>>(),
            _ => vec![]
        };
        ids.sort();
        assert_eq!(ids, vec![0, 1]);
    }

    #[test]
    fn fixed_chunk_balances_heaviest_tests_across_chunks() {
        let tests = [
            input(0, 100),
            input(1, 100),
            input(2, 100),
            input(3, 100),
            input(4, 1),
            input(5, 1),
            input(6, 1),
            input(7, 1),
        ];
        let batches = BatchMode::FixedChunk { size: NonZeroUsize::new(4).unwrap() }.partition(&tests);
        assert_eq!(batches.len(), 2);
        let weight = |c: &TestSelection<TestItem>| {
            if let TestSelection::Explicit(v) = c {
                v.iter().map(|t| t.weight_ms()).sum::<u64>()
            } else {
                0
            }
        };
        assert_eq!(weight(&batches[0]), 202);
        assert_eq!(weight(&batches[1]), 202);
    }
}
