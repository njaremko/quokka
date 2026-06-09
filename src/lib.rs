//! A TPX-style external test runner for Buck2, specialized for Rust tests.
//!
//! Buck2 owns execution and caching. This runner implements buck2's external
//! test protocol: it serves the `TestExecutor` service (receiving one
//! `ExternalRunnerSpec` per target), lists each target's tests via a cacheable
//! `Execute2(Listing)`, then issues one `Execute2(Testing)` per discovered test
//! and reports each result. See `DESIGN.md` for the full design.

pub mod batching;
pub mod caching;
pub mod cli;
pub mod config;
pub mod duration_db;
pub mod environment;
pub mod execution;
pub mod executor_server;
pub mod ids;
pub mod listing;
pub mod orchestrator;
pub mod policy;
pub mod proto;
pub mod result;
pub mod run;
pub mod scheduler;
pub mod spec;
pub mod translator;
pub mod transport;
pub mod variant;
pub mod db_cli;
