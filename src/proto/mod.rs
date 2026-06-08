//! Generated gRPC/protobuf bindings for the Buck2 test-runner protocol.
//!
//! These are pre-generated (not built with a `build.rs`) so the crate has no
//! build-time `protoc` dependency in the Buck2 graph. The `.proto` sources live
//! in `proto/` and the generated Rust lives in `src/proto/gen/`. To regenerate
//! after a `.proto` change, run `proto/regenerate.sh` (which uses `protoc` +
//! `tonic-build` and copies the output back here).
//!
//! The four packages are mounted as sibling modules under `proto` because prost
//! emits cross-package references as `super::data::…` / `super::host_sharing::…`
//! (the common `buck` package prefix is the shared parent — here, this module).
//!
//! Wire-pinned to buck2 commit `3f054b09fb3ddf6e96c8c38f1f21e9420b4215f0`
//! (release `2026-05-18`), which is the installed `buck2` binary that launches
//! this runner.

// `#[allow(warnings)]` + the clippy groups apply to the included generated code
// only. This is tonic/prost machine output, not hand-written code, so the
// workspace's strict first-party lints (unused_imports, enum_glob_use,
// unwrap_used, etc.) do not apply to it.
#[allow(warnings, clippy::all, clippy::pedantic, clippy::nursery)]
pub mod test {
    include!("gen/test.rs");
}

#[allow(warnings, clippy::all, clippy::pedantic, clippy::nursery)]
pub mod data {
    include!("gen/data.rs");
}

#[allow(warnings, clippy::all, clippy::pedantic, clippy::nursery)]
pub mod host_sharing {
    include!("gen/host_sharing.rs");
}

#[allow(warnings, clippy::all, clippy::pedantic, clippy::nursery)]
pub mod downward_api {
    include!("gen/downward_api.rs");
}
